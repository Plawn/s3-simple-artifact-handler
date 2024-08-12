use std::io::Write;
use std::path::PathBuf;

use awsregion::Region;
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::{glob, PatternError};
use log::info;
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration};
use std::fs::{remove_file, File};
use tar;
use uuid::Uuid;

type GenericErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
enum Cli {
    Upload {
        /// Bucket to upload the file to (will be created if it doesn't exist)
        bucket: String,
        // object: Option<String>,
        /// File to upload.
        files: Vec<PathBuf>,
    },
    Download {
        /// Bucket to upload the file to (will be created if it doesn't exist)
        bucket: String,
        object: String,
    },
}

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
    println!("uploading: {}", &filename);
    let bucket = ensure_bucket(bucket)?;
    // println!("bucket is ok");
    let mut async_output_file = File::open(filename).expect("Unable to create file");
    // let file_size = file.metadata()?.len();
    let status_code = bucket.put_object_stream(&mut async_output_file, object_path)?;

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
        tar_builder.append_path(&name)?;
        info!("Added {:?}", name.to_str());
    }
    tar_builder.finish()?;

    Ok(tar_name.into())
}

fn upload_artifacts(bucket: Bucket, dirs: &Vec<PathBuf>, object: Option<String>) -> String {
    let filename = prepare_tar(&dirs).unwrap();
    // TODO: generate random name

    let object = object.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        id.to_string()
    });
    let name = &bucket.name.clone();
    upload_file(bucket, &filename, &object).expect("Failed to upload file");
    remove_file(filename).expect("Failed to remove artifact archive");
    info!("Uploaded files at {:?} to {}/{}", dirs, name, object);
    object.to_string()
}


fn download_artifacts(
    bucket: &Bucket,
    object_path: &str,
    local_path: &str,
    decode_location: &str,
) -> Result<(), GenericErr> {
    let mut async_output_file = File::create(local_path).expect("Unable to create file");
    let response_data_stream = bucket.get_object(object_path)?;
    async_output_file.write_all(response_data_stream.bytes())?;
    // println!("file downloaded: {}", response_data_stream);
    let tar = File::open(local_path).unwrap();
    let dec = GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    // TODO: handle err
    a.unpack(decode_location).unwrap();
    Ok(())
}

fn main() -> Result<(), GenericErr> {
    let args = Cli::parse();

    // TODO: take provider from  stdin

    let access_key = "Q3AM3UQ867SPQQA43P2F";
    let password_key = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";
    let endpoint = "https://play.min.io";

    // let static_provider = StaticProvider::new(access_key, password_key, None);

    // let client = ClientBuilder::new(endpoint.parse()?)
    //     .provider(Some(Box::new(static_provider)))
    //     .build()?;
    // let bucket_name = "rust-s3-test";
    // let region = "us-east-1".parse()?;
    let region = Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: "https://play.min.io".to_owned(),
    };
    let credentials = Credentials::new(Some(access_key), Some(password_key), None, None, None)?;
    // let static_provider = StaticProvider::new(access_key, password_key, None);

    // let client = ClientBuilder::new(endpoint.parse()?)
    //     .provider(Some(Box::new(static_provider)))
    //     .build()?;

    // println!("bucket exists: {}", client.bucket_exists(&BucketArgs::new("bucket")?).await?);
    // client.make_bucket(&MakeBucketArgs::new("bucket")?).await.ok();

    let result = match args {
        Cli::Upload {
            bucket: bucket_name,
            files,
            // object,
        } => {
            let bucket = Bucket::new(&bucket_name, region, credentials)?.with_path_style();
            println!("{:?}", &bucket);
            let archive_name = upload_artifacts(bucket, &files, None);
            // write archive name to file
            println!("{}", &archive_name);
        }
        Cli::Download {
            bucket: bucket_name,
            object,
        } => {
            let download_path = "local_download.tar.gz";
            let decode_location = ".";
            let bucket = Bucket::new(&bucket_name, region, credentials)?.with_path_style();
            println!("downloading :{}", &object);
            download_artifacts(&bucket, &object, download_path, decode_location)?;
            remove_file(download_path).unwrap();
        }
    };

    Ok(result)
}
