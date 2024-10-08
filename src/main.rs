use std::io::Read;
use std::path::PathBuf;

use clap::{ArgAction, Parser};
use env_logger::Builder;
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::{glob, PatternError};
use log::LevelFilter;

use chrono::Local;
use rusty_s3::{Bucket, Credentials, UrlStyle};
use s3_simple_artifact_handler::S3Client;
use serde::Deserialize;
use std::fs::{remove_file, File};
use std::io::Write;
use uuid::Uuid;

type GenericErr = Box<dyn std::error::Error + Send + Sync>;

const VERSION: &str = "0.1.0";

#[derive(Deserialize, Debug)]
struct S3Conf {
    endpoint: String,
    access_key: String,
    pass_key: String,
    region: Option<String>,
}

#[derive(Parser)]
enum Cli {
    Version,
    Upload {
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        object: Option<String>,
        /// File to upload.
        #[arg(long)]
        #[clap(required = true, value_delimiter = ',')]
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
        #[arg(long, action=ArgAction::SetFalse)]
        remove: bool,
    },
}

fn upload_file(client: &S3Client, filename: &str, object_path: &str) -> Result<(), GenericErr> {
    log::debug!("uploading: {}", &filename);
    // let bucket = ensure_bucket(bucket, credentials)?;
    let upload_file = File::open(filename).expect("Unable to create file");
    // s3_upload(upload_file, &bucket, credentials, object_path)?;
    client.put(object_path, upload_file)?;
    Ok(())
}
fn recurse_files(path: impl AsRef<str>) -> Result<Vec<PathBuf>, PatternError> {
    let p = if path.as_ref().ends_with("/") {
        path.as_ref().to_owned() + "**/*"
    } else {
        path.as_ref().to_owned()
    };

    glob(&p).map(|res| res.into_iter().map(|e| e.unwrap()).collect::<Vec<_>>())
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
        log::debug!("Added {:?}", name.to_str());
    }
    tar_builder.finish()?;

    Ok(tar_name.into())
}

fn upload_artifacts(
    client: &S3Client,
    dirs: &Vec<PathBuf>,
    object: Option<String>,
) -> Result<String, GenericErr> {
    let filename = prepare_tar(dirs)?;

    let object = object.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        id.to_string()
    });
    let upload_result = upload_file(client, &filename, &object);
    remove_file(filename)?;
    match upload_result {
        Ok(_) => {
            log::debug!("Uploaded files at {:?} to {}/{}", dirs, "name", object);
            Ok(object.to_string())
        }
        Err(err) => Err(err),
    }
}

fn download_artifacts(
    client: &S3Client,
    object_path: &str,
    local_path: &str,
    decode_location: &str,
) -> Result<(), GenericErr> {
    client.get(object_path, local_path)?;
    // output_file.write_all(response_data_stream.bytes())?;
    let tar = File::open(local_path)?;
    let dec = GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    a.unpack(decode_location).map(|_| Ok(()))?
}

fn get_client(bucket_name: String, path: &PathBuf) -> Result<S3Client, GenericErr> {
    let mut conf_file = File::open(path)?;
    let mut buf = String::new();
    conf_file.read_to_string(&mut buf)?;
    let conf: S3Conf = toml::from_str(&buf)?;
    let credentials = Credentials::new(&conf.access_key, &conf.pass_key);
    let region = conf.region.unwrap_or("minio".to_owned());
    let bucket = Bucket::new(
        conf.endpoint.parse()?,
        UrlStyle::Path,
        bucket_name,
        region,
    )?;
    Ok(S3Client::new(bucket, credentials))
}

fn main() -> Result<(), GenericErr> {
    let args = Cli::parse();

    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();

    match args {
        Cli::Version {} => {
            println!("simple-artifact-handler: {}", VERSION);
        }
        Cli::Upload {
            bucket: bucket_name,
            files,
            object,
            config_file,
        } => {
            let client = get_client(bucket_name, &config_file)?.ensure()?;
            let archive_name = upload_artifacts(&client, &files, object)?;
            println!("{}", &archive_name);
        }
        Cli::Download {
            bucket: bucket_name,
            object,
            config_file,
            remove,
        } => {
            let download_path = "local_download.tar.gz";
            let decode_location = ".";
            // let bucket = Bucket::new(&bucket_name, region, credentials)?.with_path_style();
            let client = get_client(bucket_name, &config_file)?;
            log::debug!("downloading :{}", &object);
            download_artifacts(&client, &object, download_path, decode_location)?;
            if remove {
                client.delete(&object)?;
            }
            remove_file(download_path)?;
        }
    };

    Ok(())
}
