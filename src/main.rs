use std::io::{Error, Read, Write};
use std::path::PathBuf;

use awsregion::Region;
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::{glob, PatternError};
use log::debug;
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration};
use std::fs::{remove_file, File};
use uuid::Uuid;

type GenericErr = Box<dyn std::error::Error + Send + Sync>;

use serde::Deserialize;

#[derive(Deserialize)]
struct S3Conf {
    endpoint: String,
    access_key: String,
    pass_key: String,
}

#[derive(Parser)]
enum Cli {
    Upload {
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        object: Option<String>,
        /// File to upload.
        #[arg(long)]
        #[clap(required = true)]
        files: Vec<PathBuf>,
    },
    Download {
        #[arg(long)]
        config_file: PathBuf,
        /// Bucket to upload the file to (will be created if it doesn't exist)
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        object: String,
    },
}

/// Ensures the bucket exists
fn ensure_bucket(bucket: Bucket) -> Result<Bucket, GenericErr> {
    if !bucket.exists()? {
        let r = Bucket::create_with_path_style(
            &bucket.name,
            bucket.region.clone(),
            bucket.credentials().unwrap(),
            BucketConfiguration::default(),
        )?
        .bucket;
        return Ok(r);
    }
    Ok(bucket)
}

fn upload_file(bucket: Bucket, filename: &str, object_path: &str) -> Result<(), GenericErr> {
    debug!("uploading: {}", &filename);
    let bucket = ensure_bucket(bucket)?;
    // debug!("bucket is ok");
    let mut upload_file = File::open(filename).expect("Unable to create file");
    // let file_size = file.metadata()?.len();
    let status_code = bucket.put_object_stream(&mut upload_file, object_path)?;
    if status_code > 399 {
        return Err(Box::new(Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to upload: error -> {}", status_code),
        )));
    }
    Ok(())
}
fn recurse_files(path: impl AsRef<str>) -> Result<Vec<PathBuf>, PatternError> {
    let k = glob(path.as_ref()).map(|res| res.into_iter().map(|e| e.unwrap()).collect::<Vec<_>>());
    k
}
fn prepare_tar(paths: &[PathBuf]) -> Result<String, GenericErr> {
    let tar_name = "export.tar.gz";

    let tar_file = File::create(tar_name)?;

    let enc = GzEncoder::new(tar_file, Compression::default());

    let mut tar_builder = tar::Builder::new(enc);
    let files = paths
        .iter()
        .map(|e| recurse_files(e.to_str().unwrap()))
        .flat_map(|e| e.unwrap());
    for name in files {
        tar_builder.append_path(&name)?;
        debug!("Added {:?}", name.to_str());
    }
    tar_builder.finish()?;

    Ok(tar_name.into())
}

fn upload_artifacts(
    bucket: Bucket,
    dirs: &Vec<PathBuf>,
    object: Option<String>,
) -> Result<String, GenericErr> {
    let filename = prepare_tar(dirs)?;

    let object = object.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        id.to_string()
    });
    let name = &bucket.name.clone();
    let upload_result = upload_file(bucket, &filename, &object);
    remove_file(filename)?;
    return match upload_result {
        Ok(_) => {
            debug!("Uploaded files at {:?} to {}/{}", dirs, name, object);
            Ok(object.to_string())
        },
        Err(err) => Err(err),
    }
}

fn download_artifacts(
    bucket: &Bucket,
    object_path: &str,
    local_path: &str,
    decode_location: &str,
) -> Result<(), GenericErr> {
    let mut output_file = File::create(local_path).expect("Unable to create file");
    let response_data_stream = bucket.get_object(object_path)?;
    output_file.write_all(response_data_stream.bytes())?;
    let tar = File::open(local_path).unwrap();
    let dec = GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    a.unpack(decode_location).map(|_| Ok(()))?
}

fn get_bucket(bucket_name: String, path: &PathBuf) -> Result<Bucket, GenericErr> {
    let mut conf_file = File::open(path).expect("Unable to create file");
    let mut buf = String::new();
    conf_file.read_to_string(&mut buf)?;
    let conf: S3Conf = toml::from_str(&buf)?;
    let region = Region::Custom {
        // should be a configuration too
        region: "us-east-1".to_owned(),
        endpoint: conf.endpoint,
    };
    let credentials = Credentials::new(Some(&conf.access_key), Some(&conf.pass_key), None, None, None)?;
    let bucket = Bucket::new(&bucket_name, region, credentials)?.with_path_style();
    Ok(bucket)
}

fn main() -> Result<(), GenericErr> {
    let args = Cli::parse();

    match args {
        Cli::Upload {
            bucket: bucket_name,
            files,
            object,
            config_file,
        } => {
            let bucket = get_bucket(bucket_name, &config_file)?;
            let archive_name = upload_artifacts(bucket, &files, object)?;
            println!("{}", &archive_name);
        }
        Cli::Download {
            bucket: bucket_name,
            object,
            config_file,
        } => {
            let download_path = "local_download.tar.gz";
            let decode_location = ".";
            // let bucket = Bucket::new(&bucket_name, region, credentials)?.with_path_style();
            let bucket = get_bucket(bucket_name, &config_file)?;
            debug!("downloading :{}", &object);
            download_artifacts(&bucket, &object, download_path, decode_location)?;
            remove_file(download_path).unwrap();
        }
    };

    Ok(())
}
