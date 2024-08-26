use std::fs::File;

use log::debug;
use reqwest::blocking::Client;
use reqwest::header::ETAG;
use rusty_s3::actions::{
    CompleteMultipartUpload, CreateBucket, CreateMultipartUpload, DeleteObject, GetObject,
    HeadBucket, S3Action, UploadPart,
};
use rusty_s3::{Bucket, Credentials};
use std::time::Duration;
const SIGNATURE_TIMEOUT: Duration = Duration::from_secs(1);
type GenericErr = Box<dyn std::error::Error + Send + Sync>;
use std::iter;

pub struct S3Client {
    client: Client,
    bucket: Bucket,
    credentials: Credentials,
}

impl S3Client {
    pub fn new(bucket: Bucket, credentials: Credentials) -> Self {
        let client = Client::new();

        Self {
            client,
            bucket,
            credentials,
        }
    }

    pub fn get(&self, name: &str, local_path: &str) -> Result<(), GenericErr> {
        let mut action = GetObject::new(&self.bucket, Some(&self.credentials), name);
        action
            .query_mut()
            .insert("response-cache-control", "no-cache, no-store");
        let signed_url = action.sign(SIGNATURE_TIMEOUT);
        let mut output_file = File::create(local_path).expect("Unable to create file");
        // let response_data_stream = bucket.get_object(object_path)?;
        let mut response = self.client.get(signed_url).send()?;
        response.copy_to(&mut output_file)?;
        Ok(())
    }

    pub fn delete(&self, object: &str) -> Result<(), GenericErr> {
        let action = DeleteObject::new(&self.bucket, Some(&self.credentials), object)
            .sign(SIGNATURE_TIMEOUT);
        // TODO
        let _ = self.client.delete(action).send()?;
        Ok(())
    }

    pub fn put(&self, object: &str, file: File) -> Result<(), GenericErr> {
        let action = CreateMultipartUpload::new(&self.bucket, Some(&self.credentials), object);
        let url = action.sign(SIGNATURE_TIMEOUT);
        let resp = self.client.post(url).send()?.error_for_status()?;
        let body = resp.text()?;

        let multipart = CreateMultipartUpload::parse_response(&body)?;

        debug!(
            "multipart upload created - upload id: {}",
            multipart.upload_id()
        );

        let part_upload = UploadPart::new(
            &self.bucket,
            Some(&self.credentials),
            object,
            1,
            multipart.upload_id(),
        );
        let url = part_upload.sign(SIGNATURE_TIMEOUT);
        let resp = self.client.put(url).body(file).send()?.error_for_status()?;
        let etag = resp
            .headers()
            .get(ETAG)
            .expect("every UploadPart request returns an Etag");

        debug!("etag: {}", etag.to_str().unwrap());

        let action = CompleteMultipartUpload::new(
            &self.bucket,
            Some(&self.credentials),
            object,
            multipart.upload_id(),
            iter::once(etag.to_str().unwrap()),
        );
        let url = action.sign(SIGNATURE_TIMEOUT);

        let resp = self
            .client
            .post(url)
            .body(action.body())
            .send()?
            .error_for_status()?;
        if !resp.status().is_success() {
            panic!("upload failed");
        }
        Ok(())
    }

    /// Ensures the bucket exists
    pub fn ensure(self) -> Result<Self, GenericErr> {
        let action = HeadBucket::new(&self.bucket, Some(&self.credentials));
        let url = action.sign(SIGNATURE_TIMEOUT);
        let client = Client::new();
        let response = client.head(url).send()?;
        if response.status().as_u16() > 399 {
            debug!("creating bucket");
            let q = CreateBucket::new(&self.bucket, &self.credentials);
            let response = client.put(q.sign(SIGNATURE_TIMEOUT)).send()?;
            if !response.status().is_success() {
                debug!("{}", &response.status());
                panic!("Failed to create bucket");
            }
        }
        Ok(self)
    }
}
