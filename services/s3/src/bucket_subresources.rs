use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredBucketAclInput {
    Canned(CannedAcl),
    Xml(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StoredBucketAcl {
    Canned(CannedAcl),
    Xml(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
}

impl std::str::FromStr for CannedAcl {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "private" => Ok(Self::Private),
            "public-read" => Ok(Self::PublicRead),
            "public-read-write" => Ok(Self::PublicReadWrite),
            _ => Err(()),
        }
    }
}

pub(crate) fn canned_acl_xml(
    owner_account_id: &str,
    acl: CannedAcl,
) -> String {
    let mut xml = String::from(
        "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner>",
    );
    xml.push_str(&format!(
        "<ID>{owner_account_id}</ID><DisplayName>{owner_account_id}</DisplayName></Owner><AccessControlList>"
    ));
    xml.push_str(&format!(
        "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{owner_account_id}</ID><DisplayName>{owner_account_id}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>"
    ));

    match acl {
        CannedAcl::Private => {}
        CannedAcl::PublicRead => {
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>");
        }
        CannedAcl::PublicReadWrite => {
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>");
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>WRITE</Permission></Grant>");
        }
    }

    xml.push_str("</AccessControlList></AccessControlPolicy>");
    xml
}

use crate::{errors::S3Error, scope::S3Scope, state::S3Service};

impl S3Service {
    pub(crate) fn put_bucket_subresource<F>(
        &self,
        scope: &S3Scope,
        bucket: &str,
        mutate: F,
    ) -> Result<(), S3Error>
    where
        F: FnOnce(&mut crate::bucket::BucketRecord),
    {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        mutate(&mut bucket_record);
        self.persist_bucket_record(bucket, bucket_record)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
        policy: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_policy(Some(policy));
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.into_policy().ok_or_else(|| {
            S3Error::NoSuchBucketPolicy { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_policy(None);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_cors_configuration(Some(configuration));
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.into_cors_configuration().ok_or_else(|| {
            S3Error::NoSuchCORSConfiguration { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_cors_configuration(None);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_lifecycle_configuration(Some(configuration));
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.into_lifecycle_configuration().ok_or_else(|| {
            S3Error::NoSuchLifecycleConfiguration { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_lifecycle_configuration(None);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_encryption_configuration(Some(configuration));
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.into_encryption_configuration().ok_or_else(|| {
            S3Error::NoSuchBucketEncryption { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_encryption_configuration(None);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_acl(
        &self,
        scope: &S3Scope,
        bucket: &str,
        acl: StoredBucketAclInput,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.set_acl_configuration(acl);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_acl(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        Ok(bucket_record.acl_xml())
    }
}
