// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

use clap::Parser;
use log::info;
use minio::s3::{
    args::{BucketExistsArgs, MakeBucketArgs, PutObjectArgs},
    client::ClientBuilder,
    creds::StaticProvider,
};

/// Upload a file to the given bucket and object path on the MinIO Play server.
#[derive(Parser)]
struct Cli {
    /// Bucket to upload the file to (will be created if it doesn't exist)
    bucket: String,
    /// Object path to upload the file to.
    object: String,
    /// File to upload.
    file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Cli::parse();

    let static_provider = StaticProvider::new(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
        None,
    );

    let client = ClientBuilder::new("https://play.min.io".parse()?)
        .provider(Some(Box::new(static_provider)))
        .build()?;

    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(&args.bucket).unwrap())
        .await
        .unwrap();

    if !exists {
        client
            .make_bucket(&MakeBucketArgs::new(&args.bucket).unwrap())
            .await
            .unwrap();
    }
    use std::fs::File;
    // Lire le fichier à uploader
    let mut file = File::open(&args.file).unwrap();
    let file_size = file.metadata().unwrap().len();

    client
        .put_object(
            &mut PutObjectArgs::new(&args.bucket, &args.object, &mut file, Some(file_size as usize), None).unwrap(),
        )
        .await
        .unwrap();

    info!(
        "Uploaded file at {:?} to {}/{}",
        args.file, args.bucket, args.object
    );

    Ok(())
}
