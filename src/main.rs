use std::io::{Error, Read, Write};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::{glob, PatternError};
use log::debug;
use reqwest::blocking::Client;
use reqwest::header::ETAG;
use rusty_s3::actions::{
    CompleteMultipartUpload, CreateBucket, CreateMultipartUpload, GetObject, HeadBucket, S3Action,
    UploadPart,
};
use rusty_s3::{Bucket, Credentials, UrlStyle};
use std::fs::{remove_file, File};
use std::iter;
use uuid::Uuid;

type GenericErr = Box<dyn std::error::Error + Send + Sync>;

const SIGNATURE_TIMEOUT: Duration = Duration::from_secs(1);

use serde::Deserialize;

#[derive(Deserialize, Debug)]
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
    },
}

/// Ensures the bucket exists
fn ensure_bucket(bucket: Bucket, credentials: &Credentials) -> Result<Bucket, GenericErr> {
    let action = HeadBucket::new(&bucket, Some(credentials));
    let url = action.sign(SIGNATURE_TIMEOUT);
    let client = Client::new();
    let response = client.get(url).send()?;
    // TODO: finish this
    // if response.status().as_u16() > 399 {
    //     let q = CreateBucket::new(bucket, credentials);
    // }
    // TODO
    // if !bucket.exists()? {
    //     let r = Bucket::create_with_path_style(
    //         &bucket.name,
    //         bucket.region.clone(),
    //         bucket.credentials().unwrap(),
    //         BucketConfiguration::default(),
    //     )?
    //     .bucket;
    //     return Ok(r);
    // }
    Ok(bucket)
}

fn s3_upload(
    file: File,
    bucket: &Bucket,
    credentials: &Credentials,
    object: &str,
) -> Result<(), GenericErr> {
    let client = Client::new();
    let action = CreateMultipartUpload::new(&bucket, Some(&credentials), object);
    let url = action.sign(SIGNATURE_TIMEOUT);
    let resp = client.post(url).send()?.error_for_status()?;
    let body = resp.text()?;

    let multipart = CreateMultipartUpload::parse_response(&body)?;

    println!(
        "multipart upload created - upload id: {}",
        multipart.upload_id()
    );

    let part_upload = UploadPart::new(
        &bucket,
        Some(&credentials),
        object,
        1,
        multipart.upload_id(),
    );
    let url = part_upload.sign(SIGNATURE_TIMEOUT);
    let resp = client.put(url).body(file).send()?.error_for_status()?;
    let etag = resp
        .headers()
        .get(ETAG)
        .expect("every UploadPart request returns an Etag");

    println!("etag: {}", etag.to_str().unwrap());

    let action = CompleteMultipartUpload::new(
        &bucket,
        Some(&credentials),
        object,
        multipart.upload_id(),
        iter::once(etag.to_str().unwrap()),
    );
    let url = action.sign(SIGNATURE_TIMEOUT);

    let resp = client
        .post(url)
        .body(action.body())
        .send()?
        .error_for_status()?;
    let body = resp.text()?;
    println!("it worked! {body}");

    Ok(())
}

fn upload_file(
    bucket: Bucket,
    credentials: &Credentials,
    filename: &str,
    object_path: &str,
) -> Result<(), GenericErr> {
    debug!("uploading: {}", &filename);
    let bucket = ensure_bucket(bucket, credentials)?;
    // debug!("bucket is ok");
    let upload_file = File::open(filename).expect("Unable to create file");
    // let file_size = file.metadata()?.len();
    let status_code = s3_upload(upload_file, &bucket, &credentials, object_path)?;
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
    credentials: &Credentials,
    dirs: &Vec<PathBuf>,
    object: Option<String>,
) -> Result<String, GenericErr> {
    let filename = prepare_tar(dirs)?;

    let object = object.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        id.to_string()
    });
    // let name = &bucket.name.clone();
    let upload_result = upload_file(bucket, credentials, &filename, &object);
    remove_file(filename)?;
    return match upload_result {
        Ok(_) => {
            debug!("Uploaded files at {:?} to {}/{}", dirs, "name", object);
            Ok(object.to_string())
        }
        Err(err) => Err(err),
    };
}

fn download_artifacts(
    bucket: &Bucket,
    credentials: &Credentials,
    object_path: &str,
    local_path: &str,
    decode_location: &str,
) -> Result<(), GenericErr> {
    let mut action = GetObject::new(&bucket, Some(credentials), object_path);
    action
        .query_mut()
        .insert("response-cache-control", "no-cache, no-store");
    let signed_url = action.sign(SIGNATURE_TIMEOUT);
    let mut output_file = File::create(local_path).expect("Unable to create file");
    let client = Client::new();
    // let response_data_stream = bucket.get_object(object_path)?;
    let mut response = client.get(signed_url).send()?;
    response.copy_to(&mut output_file);
    // output_file.write_all(response_data_stream.bytes())?;
    let tar = File::open(local_path).unwrap();
    let dec = GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    a.unpack(decode_location).map(|_| Ok(()))?
}

fn get_bucket(bucket_name: String, path: &PathBuf) -> Result<(Bucket, Credentials), GenericErr> {
    let mut conf_file = File::open(path).expect("Unable to create file");
    let mut buf = String::new();
    conf_file.read_to_string(&mut buf)?;
    let conf: S3Conf = toml::from_str(&buf)?;
    let credentials = Credentials::new(&conf.access_key, &conf.pass_key);
    let region = "minio";
    let bucket = Bucket::new(
        conf.endpoint.parse().unwrap(),
        UrlStyle::Path,
        bucket_name,
        region,
    )
    .unwrap();
    Ok((bucket, credentials))
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
            let (bucket, credentials) = get_bucket(bucket_name, &config_file)?;
            let archive_name = upload_artifacts(bucket, &credentials, &files, object)?;
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
            let (bucket, credentials) = get_bucket(bucket_name, &config_file)?;
            debug!("downloading :{}", &object);
            download_artifacts(
                &bucket,
                &credentials,
                &object,
                download_path,
                decode_location,
            )?;
            remove_file(download_path).unwrap();
        }
    };

    Ok(())
}
