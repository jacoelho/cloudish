use aws::AwsError;

use crate::sigv4::{
    hash_hex, hex_encode, hmac_bytes, incomplete_signature_error,
    signature_does_not_match_error, validate_hex_signature,
};

pub(crate) const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str =
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub(crate) const STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER: &str =
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub(crate) const STREAMING_UNSIGNED_PAYLOAD_TRAILER: &str =
    "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsChunkedMode {
    Payload,
    PayloadTrailer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsChunkedSigningContext {
    amz_date: String,
    scope: String,
    signing_key: [u8; 32],
    seed_signature: String,
    mode: AwsChunkedMode,
}

impl AwsChunkedSigningContext {
    pub fn new(
        secret_access_key: &str,
        date: &str,
        region: &str,
        service: &str,
        amz_date: &str,
        seed_signature: &str,
        mode: AwsChunkedMode,
    ) -> Self {
        Self {
            amz_date: amz_date.to_owned(),
            scope: format!("{date}/{region}/{service}/aws4_request"),
            signing_key: signing_key(secret_access_key, date, region, service),
            seed_signature: seed_signature.to_owned(),
            mode,
        }
    }

    pub fn mode(&self) -> AwsChunkedMode {
        self.mode
    }

    /// Decodes and verifies an `aws-chunked` S3 payload.
    ///
    /// # Errors
    ///
    /// Returns an AWS-compatible signature or validation error when the chunk
    /// framing is malformed, a chunk signature does not verify, the trailing
    /// headers do not match the advertised trailer set, or the decoded length
    /// does not match `x-amz-decoded-content-length`.
    pub fn decode(
        &self,
        encoded_body: &[u8],
        decoded_content_length: usize,
        advertised_trailer_names: &[String],
    ) -> Result<AwsChunkedDecoded, AwsError> {
        let mut decoded_body = Vec::new();
        let mut offset = 0;
        let mut previous_signature = self.seed_signature.clone();

        loop {
            let header_end = find_crlf(encoded_body, offset).ok_or_else(|| {
                incomplete_signature_error(
                    "aws-chunked payload ended before chunk metadata completed.",
                )
            })?;
            let header_line = std::str::from_utf8(
                encoded_body.get(offset..header_end).ok_or_else(|| {
                    incomplete_signature_error(
                        "aws-chunked chunk metadata exceeded the payload bounds.",
                    )
                })?,
            )
            .map_err(|_| {
                incomplete_signature_error(
                    "aws-chunked chunk metadata must be valid UTF-8.",
                )
            })?;
            let (chunk_size, chunk_signature) =
                parse_chunk_header(header_line)?;
            offset = header_end + 2;

            let chunk_end = offset
                .checked_add(chunk_size)
                .and_then(|end| end.checked_add(2))
                .ok_or_else(|| {
                    incomplete_signature_error(
                        "aws-chunked chunk metadata overflowed the payload bounds.",
                    )
                })?;
            let chunk_data = encoded_body
                .get(offset..offset + chunk_size)
                .ok_or_else(|| {
                    incomplete_signature_error(
                        "aws-chunked payload ended before chunk data completed.",
                    )
                })?;
            if encoded_body.get(offset + chunk_size..chunk_end)
                != Some(b"\r\n")
            {
                return Err(incomplete_signature_error(
                    "aws-chunked chunk data must end with CRLF.",
                ));
            }

            let expected_signature =
                self.chunk_signature(&previous_signature, chunk_data);
            if expected_signature != chunk_signature {
                return Err(signature_does_not_match_error(
                    "The aws-chunked payload signature did not match the provided chunk signature.",
                ));
            }

            decoded_body.extend_from_slice(chunk_data);
            previous_signature = chunk_signature;
            offset = chunk_end;

            if chunk_size == 0 {
                break;
            }
        }

        if decoded_body.len() != decoded_content_length {
            return Err(incomplete_signature_error(
                "X-Amz-Decoded-Content-Length must match the decoded payload length.",
            ));
        }

        match self.mode {
            AwsChunkedMode::Payload => {
                if offset != encoded_body.len() {
                    return Err(incomplete_signature_error(
                        "aws-chunked payload must end after the final zero-sized chunk.",
                    ));
                }

                Ok(AwsChunkedDecoded {
                    decoded_body,
                    trailer_headers: Vec::new(),
                })
            }
            AwsChunkedMode::PayloadTrailer => {
                let trailer_headers = parse_trailing_headers(
                    encoded_body.get(offset..).ok_or_else(|| {
                        incomplete_signature_error(
                            "aws-chunked payload ended before trailing headers completed.",
                        )
                    })?,
                    advertised_trailer_names,
                )?;
                let (trailer_headers, provided_signature) =
                    split_trailer_signature(trailer_headers)?;
                let canonical_trailer = trailer_headers
                    .iter()
                    .map(|(name, value)| format!("{name}:{value}\n"))
                    .collect::<String>();
                let expected_signature = self.trailer_signature(
                    &previous_signature,
                    canonical_trailer.as_bytes(),
                );
                if expected_signature != provided_signature {
                    return Err(signature_does_not_match_error(
                        "The aws-chunked trailer signature did not match the provided trailer signature.",
                    ));
                }

                Ok(AwsChunkedDecoded { decoded_body, trailer_headers })
            }
        }
    }

    fn chunk_signature(
        &self,
        previous_signature: &str,
        chunk_data: &[u8],
    ) -> String {
        sign(
            &self.signing_key,
            &format!(
                "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{previous_signature}\n{}\n{}",
                self.amz_date,
                self.scope,
                hash_hex(b""),
                hash_hex(chunk_data),
            ),
        )
    }

    fn trailer_signature(
        &self,
        previous_signature: &str,
        trailer_headers: &[u8],
    ) -> String {
        sign(
            &self.signing_key,
            &format!(
                "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{previous_signature}\n{}",
                self.amz_date,
                self.scope,
                hash_hex(trailer_headers),
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsChunkedDecoded {
    decoded_body: Vec<u8>,
    trailer_headers: Vec<(String, String)>,
}

impl AwsChunkedDecoded {
    pub fn decoded_body(&self) -> &[u8] {
        &self.decoded_body
    }

    pub fn trailer_headers(&self) -> &[(String, String)] {
        &self.trailer_headers
    }
}

fn parse_chunk_header(header_line: &str) -> Result<(usize, String), AwsError> {
    let (raw_size, extension) = header_line.split_once(';').ok_or_else(|| {
        incomplete_signature_error(
            "aws-chunked chunk metadata must include a chunk-signature extension.",
        )
    })?;
    let chunk_size = usize::from_str_radix(raw_size.trim(), 16).map_err(|_| {
        incomplete_signature_error(
            "aws-chunked chunk metadata must start with a hexadecimal chunk size.",
        )
    })?;
    let (name, value) = extension.split_once('=').ok_or_else(|| {
        incomplete_signature_error(
            "aws-chunked chunk metadata must use the chunk-signature=... extension format.",
        )
    })?;
    if name.trim() != "chunk-signature" || value.contains(';') {
        return Err(incomplete_signature_error(
            "aws-chunked chunk metadata must use the chunk-signature=... extension format.",
        ));
    }
    validate_hex_signature(value.trim())?;

    Ok((chunk_size, value.trim().to_ascii_lowercase()))
}

fn parse_trailing_headers(
    trailing_bytes: &[u8],
    advertised_trailer_names: &[String],
) -> Result<Vec<(String, String)>, AwsError> {
    if advertised_trailer_names.is_empty() {
        return Err(incomplete_signature_error(
            "aws-chunked trailing headers require an X-Amz-Trailer header.",
        ));
    }

    let trailer_end = trailing_bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or_else(|| {
            incomplete_signature_error(
                "aws-chunked trailing headers must end with CRLF CRLF.",
            )
        })?;
    if trailer_end != trailing_bytes.len() {
        return Err(incomplete_signature_error(
            "aws-chunked payload must end after the trailing headers block.",
        ));
    }

    let trailer_block = std::str::from_utf8(
        trailing_bytes
            .get(..trailer_end.saturating_sub(4))
            .unwrap_or_default(),
    )
    .map_err(|_| {
        incomplete_signature_error(
            "aws-chunked trailing headers must be valid UTF-8.",
        )
    })?;

    let mut parsed = Vec::new();
    for line in trailer_block.split("\r\n") {
        let (name, value) = line.split_once(':').ok_or_else(|| {
            incomplete_signature_error(
                "aws-chunked trailing headers must be NAME:VALUE pairs.",
            )
        })?;
        parsed.push((name.trim().to_owned(), value.trim().to_owned()));
    }

    let advertised = advertised_trailer_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    for (name, _) in &parsed {
        if name.eq_ignore_ascii_case("x-amz-trailer-signature") {
            continue;
        }
        if !advertised
            .iter()
            .any(|advertised_name| advertised_name.eq_ignore_ascii_case(name))
        {
            return Err(incomplete_signature_error(format!(
                "aws-chunked trailing header {name} was not listed in X-Amz-Trailer.",
            )));
        }
    }
    for advertised_name in advertised {
        if !parsed
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(advertised_name))
        {
            return Err(incomplete_signature_error(format!(
                "aws-chunked trailing header {advertised_name} was declared but not provided.",
            )));
        }
    }

    Ok(parsed)
}

fn split_trailer_signature(
    mut trailer_headers: Vec<(String, String)>,
) -> Result<(Vec<(String, String)>, String), AwsError> {
    let signature_index = trailer_headers
        .iter()
        .position(|(name, _)| name.eq_ignore_ascii_case("x-amz-trailer-signature"))
        .ok_or_else(|| {
            incomplete_signature_error(
                "aws-chunked trailing headers must include x-amz-trailer-signature.",
            )
        })?;
    let (_, signature) = trailer_headers.remove(signature_index);
    validate_hex_signature(&signature)?;

    Ok((trailer_headers, signature.to_ascii_lowercase()))
}

fn find_crlf(bytes: &[u8], start: usize) -> Option<usize> {
    bytes.get(start..).and_then(|remaining| {
        remaining
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|offset| start + offset)
    })
}

fn sign(signing_key: &[u8; 32], string_to_sign: &str) -> String {
    hex_encode(&hmac_bytes(signing_key, string_to_sign.as_bytes()))
}

fn signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> [u8; 32] {
    let date_key = hmac_bytes(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let region_key = hmac_bytes(&date_key, region.as_bytes());
    let service_key = hmac_bytes(&region_key, service.as_bytes());
    hmac_bytes(&service_key, b"aws4_request")
}

#[cfg(test)]
mod tests {
    use super::{
        AwsChunkedMode, AwsChunkedSigningContext,
        STREAMING_AWS4_HMAC_SHA256_PAYLOAD,
        STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER,
    };

    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const DATE: &str = "20130524";
    const REGION: &str = "us-east-1";
    const SERVICE: &str = "s3";
    const AMZ_DATE: &str = "20130524T000000Z";

    #[test]
    fn decode_docs_example_streaming_payload() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
            AwsChunkedMode::Payload,
        );

        let decoded =
            context.decode(&payload_example_body(), 66_560, &[]).expect(
                "official AWS streaming payload example should verify and decode",
            );

        assert_eq!(decoded.decoded_body(), vec![b'a'; 66_560].as_slice());
        assert!(decoded.trailer_headers().is_empty());
        assert_eq!(
            STREAMING_AWS4_HMAC_SHA256_PAYLOAD,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        );
    }

    #[test]
    fn reject_invalid_chunk_signature() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
            AwsChunkedMode::Payload,
        );

        let error = context
            .decode(
                &payload_body_with_signatures(
                    "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
                    "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                66_560,
                &[],
            )
            .expect_err("invalid final chunk signature must fail");

        assert_eq!(error.code(), "SignatureDoesNotMatch");
    }

    #[test]
    fn reject_malformed_chunk_framing() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
            AwsChunkedMode::Payload,
        );
        let mut malformed = payload_example_body();
        malformed.truncate(malformed.len().saturating_sub(2));

        let error = context
            .decode(&malformed, 66_560, &[])
            .expect_err("truncated chunk framing must fail");

        assert_eq!(error.code(), "IncompleteSignature");
    }

    #[test]
    fn reject_decoded_length_mismatch() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
            AwsChunkedMode::Payload,
        );

        let error = context
            .decode(&payload_example_body(), 66_559, &[])
            .expect_err("decoded length mismatch must fail");

        assert_eq!(error.code(), "IncompleteSignature");
    }

    #[test]
    fn decode_docs_example_streaming_trailers() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
            AwsChunkedMode::PayloadTrailer,
        );

        let decoded = context
            .decode(
                &trailer_example_body(
                    "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435",
                ),
                66_560,
                &[String::from("x-amz-checksum-crc32c")],
            )
            .expect("official AWS streaming trailer example should verify");

        assert_eq!(decoded.decoded_body(), vec![b'a'; 66_560].as_slice());
        assert_eq!(
            decoded.trailer_headers(),
            &[(
                String::from("x-amz-checksum-crc32c"),
                String::from("sOO8/Q=="),
            )]
        );
        assert_eq!(
            STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
        );
    }

    #[test]
    fn reject_invalid_trailer_signature() {
        let context = AwsChunkedSigningContext::new(
            SECRET_ACCESS_KEY,
            DATE,
            REGION,
            SERVICE,
            AMZ_DATE,
            "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
            AwsChunkedMode::PayloadTrailer,
        );

        let error = context
            .decode(
                &trailer_example_body(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                66_560,
                &[String::from("x-amz-checksum-crc32c")],
            )
            .expect_err("invalid trailer signature must fail");

        assert_eq!(error.code(), "SignatureDoesNotMatch");
    }

    fn payload_example_body() -> Vec<u8> {
        payload_body_with_signatures(
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
        )
    }

    fn payload_body_with_signatures(
        first_signature: &str,
        second_signature: &str,
        final_signature: &str,
    ) -> Vec<u8> {
        [
            format!("10000;chunk-signature={first_signature}\r\n")
                .into_bytes(),
            vec![b'a'; 65_536],
            b"\r\n".to_vec(),
            format!("400;chunk-signature={second_signature}\r\n").into_bytes(),
            vec![b'a'; 1_024],
            b"\r\n".to_vec(),
            format!("0;chunk-signature={final_signature}\r\n\r\n")
                .into_bytes(),
        ]
        .concat()
    }

    fn trailer_example_body(trailer_signature: &str) -> Vec<u8> {
        [
            format!(
                "10000;chunk-signature={}\r\n",
                "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2"
            )
            .into_bytes(),
            vec![b'a'; 65_536],
            b"\r\n".to_vec(),
            format!(
                "400;chunk-signature={}\r\n",
                "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7"
            )
            .into_bytes(),
            vec![b'a'; 1_024],
            b"\r\n".to_vec(),
            format!(
                "0;chunk-signature={}\r\n\r\n",
                "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"
            )
            .into_bytes(),
            format!(
                "x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:{trailer_signature}\r\n\r\n"
            )
            .into_bytes(),
        ]
        .concat()
    }
}
