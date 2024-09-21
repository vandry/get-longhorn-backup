use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use lz4::Decoder;
use s3::creds::Credentials;
use s3::Bucket;
use s3::Region;
use serde::Deserialize;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BackupBlock {
    Offset: u64,
    BlockChecksum: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BackupCfg {
    CompressionMethod: String,
    Blocks: Vec<BackupBlock>,
}

#[derive(Debug)]
struct SkippedData {
    expected_offset: usize,
    found_offset: usize,
}

impl std::fmt::Display for SkippedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Gap in data: expected to find a block for offset {}, found {}",
            self.expected_offset, self.found_offset
        )
    }
}

impl std::error::Error for SkippedData {}

fn get_backup<'a>(
    bucket: &'a Bucket,
    basename: &'a str,
    blocks: &'a [BackupBlock],
) -> impl Stream<Item = Result<(u64, Vec<u8>), Box<dyn std::error::Error>>> + 'a {
    stream! {
        for block in blocks {
            let blockname = format!("{}/blocks/{}/{}/{}.blk", basename, &block.BlockChecksum[0..2], &block.BlockChecksum[2..4], &block.BlockChecksum);
            let contents = bucket.get_object(blockname).await?;
            let mut dec = Decoder::new(contents.as_slice())?;
            let mut out = Vec::new();
            dec.read_to_end(&mut out)?;
            yield Ok((block.Offset, out));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 6 {
        eprintln!(
            "Usage: {} endpoint region bucket backup-cfg-name dst",
            args[0].to_string_lossy()
        );
        std::process::exit(3);
    }
    let s3_endpoint = args[1].to_string_lossy().into_owned();
    let s3_region_name = args[2].to_string_lossy().into_owned();
    let bucket_name = args[3].to_string_lossy().into_owned();
    let backup_name = args[4].to_string_lossy().into_owned();
    let dst_path = PathBuf::from(args[5].clone());

    let basename = if let Some((base_i, _)) = backup_name.rmatch_indices('/').nth(1) {
        &backup_name[..base_i]
    } else {
        eprintln!("backup name must have at least 2 slashes so we can find the backup root.");
        std::process::exit(1);
    };

    let s3_cred = Credentials::default().unwrap();
    let s3_region = Region::Custom {
        region: s3_region_name,
        endpoint: s3_endpoint,
    };

    let bucket = Bucket::new(&bucket_name, s3_region, s3_cred).unwrap();

    let index = bucket.get_object(&backup_name).await?;
    let index = serde_json::from_slice::<BackupCfg>(index.as_slice())?;

    if index.CompressionMethod != "lz4" {
        eprintln!("Only support lz4 as a CompressionMethod");
        std::process::exit(1);
    }
    let b = get_backup(&bucket, basename, &index.Blocks);
    pin_mut!(b);

    let mut f = File::create(dst_path)?;
    while let Some((offset, chunk)) = b.next().await.transpose()? {
        f.seek(std::io::SeekFrom::Start(offset))?;
        f.write_all(&chunk)?;
    }
    Ok(())
}
