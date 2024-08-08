use std::path::PathBuf;

use clap::Parser;
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::{glob, PatternError};
use log::info;
use minio::s3::{
    args::{BucketExistsArgs, DownloadObjectArgs, MakeBucketArgs, PutObjectArgs},
    client::{Client, ClientBuilder},
    creds::StaticProvider,
};

use std::fs::{remove_file, File};
use tar;
use uuid::Uuid;

type GenericErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
enum Cli {
    Upload {
        /// Bucket to upload the file to (will be created if it doesn't exist)
        bucket: String,
        object: Option<String>,
        /// File to upload.
        files: Vec<PathBuf>,
    },
    Download {
        /// Bucket to upload the file to (will be created if it doesn't exist)
        bucket: String,
        object: String,
    },
}

async fn ensure_bucket(client: &Client, bucket: &str) {
    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(&bucket).expect("Invalid bucket name"))
        .await
        .expect("Failed to validate bucket");

    if !exists {
        client
            .make_bucket(&MakeBucketArgs::new(&bucket).expect("Invalid bucket name"))
            .await
            .expect("Failed to create bucket");
    }
}

async fn upload_file(
    client: &Client,
    bucket: &str,
    filename: &str,
    object_path: &str,
) -> Result<(), GenericErr> {
    ensure_bucket(client, bucket).await;
    // Lire le fichier à uploader
    let mut file = File::open(&filename)?;
    let file_size = file.metadata()?.len();

    client
        .put_object(&mut PutObjectArgs::new(
            bucket,
            object_path,
            &mut file,
            Some(file_size as usize),
            None,
        )?)
        .await?;

    Ok(())
}
fn recurse_files(path: impl AsRef<str>) -> Result<Vec<PathBuf>, PatternError> {
    let k = glob(path.as_ref()).map(|res| res.into_iter().map(|e| e.unwrap()).collect::<Vec<_>>());
    k
}
fn prepare_tar(paths: &Vec<PathBuf>) -> Result<String, GenericErr> {
    let tar_name = "export.tar.gz";

    let tar_file = File::create(tar_name)?;

    let enc = GzEncoder::new(tar_file, Compression::default());

    let mut tar_builder = tar::Builder::new(enc);
    let files = paths
        .iter()
        .map(|e| recurse_files(&e.to_str().unwrap()))
        .flat_map(|e| e.unwrap());
    for name in files {
        tar_builder.append_path(name)?;
    }
    tar_builder.finish()?;


    
    Ok(tar_name.into())
}

async fn upload_artifacts(
    client: &Client,
    bucket: &str,
    dirs: &Vec<PathBuf>,
    object: Option<String>,
) -> String {
    let filename = prepare_tar(&dirs).unwrap();
    // TODO: generate random name

    let object = object.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        id.to_string()
    });
    upload_file(&client, bucket, &filename, &object)
        .await
        .expect("Failed to upload file");
    remove_file(filename).expect("Failed to remove artifact archive");
    info!("Uploaded files at {:?} to {}/{}", dirs, bucket, object);
    object.to_string()
}

async fn download_artifacts(
    client: &Client,
    bucket: &str,
    object_path: &str,
    local_path: &str,
    decode_location: &str,
) {
    client
        .download_object(&DownloadObjectArgs::new(bucket, object_path, &local_path).unwrap())
        .await
        // TODO: handle err
        .unwrap();
    let tar = File::open(local_path).unwrap();
    let dec = GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    // TODO: handle err
    a.unpack(decode_location).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), GenericErr> {
    let args = Cli::parse();

    // TODO: take provider from  stdin

    let access_key = "Q3AM3UQ867SPQQA43P2F";
    let password_key = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";
    let endpoint = "https://play.min.io";

    let static_provider = StaticProvider::new(access_key, password_key, None);

    let client = ClientBuilder::new(endpoint.parse()?)
        .provider(Some(Box::new(static_provider)))
        .build()?;

    let result = match args {
        Cli::Upload {
            bucket,
            files,
            object,
        } => {
            let archive_name = upload_artifacts(&client, &bucket, &files, object).await;
            // write archive name to file
            println!("{}", &archive_name);
        }
        Cli::Download { bucket, object } => {
            let download_path = "local_download.tar.gz";
            let decode_location = ".";
            download_artifacts(&client, &bucket, &object, download_path, decode_location).await;
            remove_file(download_path).unwrap();
        }
    };

    Ok(result)
}
