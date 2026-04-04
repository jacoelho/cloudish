pub use crate::object_ranges::eligible_object_range;
pub use crate::object_read_conditions::{
    ObjectReadPreconditionOutcome, evaluate_object_read_preconditions,
};
use crate::{
    errors::S3Error,
    object_ranges::apply_object_range,
    object_read_model::{
        GetObjectOutput, HeadObjectOutput, ObjectReadRequest,
    },
    scope::S3Scope,
    state::S3Service,
};

impl S3Service {
    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn read_object_metadata(
        &self,
        scope: &S3Scope,
        input: &ObjectReadRequest,
    ) -> Result<HeadObjectOutput, S3Error> {
        self.resolve_readable_object(scope, input)?
            .into_head(input.range.as_ref())
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn head_object(
        &self,
        scope: &S3Scope,
        input: &ObjectReadRequest,
    ) -> Result<HeadObjectOutput, S3Error> {
        self.read_object_metadata(scope, input)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object(
        &self,
        scope: &S3Scope,
        input: &ObjectReadRequest,
    ) -> Result<GetObjectOutput, S3Error> {
        let object = self.resolve_readable_object(scope, input)?;
        let ranged = apply_object_range(
            &self.object_body(&object)?,
            input.range.as_ref(),
        )?;
        let is_partial = ranged.content_range.is_some();

        Ok(GetObjectOutput {
            body: ranged.body,
            content_length: ranged.content_length,
            content_range: ranged.content_range,
            is_partial,
            metadata: object.into_metadata(),
        })
    }

    fn resolve_readable_object(
        &self,
        scope: &S3Scope,
        input: &ObjectReadRequest,
    ) -> Result<crate::object::ObjectRecord, S3Error> {
        self.ensure_bucket_readable(
            scope,
            &input.bucket,
            input.expected_bucket_owner.as_deref(),
        )?;
        let object = self.resolve_object_record(
            &input.bucket,
            &input.key,
            input.version_id.as_deref(),
        )?;
        if let Some(error) =
            object.delete_marker_error(input.version_id.as_deref())
        {
            return Err(error);
        }

        Ok(object)
    }

    pub(crate) fn ensure_bucket_readable(
        &self,
        scope: &S3Scope,
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> Result<(), S3Error> {
        let bucket = self.bucket_record(bucket).ok_or_else(|| {
            S3Error::NoSuchBucket { bucket: bucket.to_owned() }
        })?;

        if let Some(expected_bucket_owner) = expected_bucket_owner
            && expected_bucket_owner != bucket.owner_account_id
        {
            return Err(S3Error::AccessDenied {
                message: format!(
                    "Bucket `{}` is not owned by account {}.",
                    bucket.name, expected_bucket_owner
                ),
            });
        }

        if bucket.owner_account_id != scope.account_id().as_str() {
            return Err(S3Error::AccessDenied {
                message: format!(
                    "Bucket `{}` is not owned by account {}.",
                    bucket.name,
                    scope.account_id()
                ),
            });
        }

        if bucket.region != scope.region().as_str() {
            return Err(S3Error::WrongRegion {
                bucket: bucket.name,
                region: bucket.region,
            });
        }

        Ok(())
    }
}
