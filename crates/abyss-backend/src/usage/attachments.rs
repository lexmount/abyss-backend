//! Image attachment ingest contracts, validation, and download response models.
//!
//! Attachments may carry either metadata alone or inline base64 content. All
//! declared sizes, hashes, media signatures, positions, and aggregate limits are
//! checked before a database transaction begins so an invalid image cannot
//! partially persist the surrounding event batch.

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::AppError;

/// Maximum number of image attachments accepted for one usage event.
pub const MAX_IMAGE_ATTACHMENTS_PER_EVENT: usize = 8;
/// Maximum aggregate decoded image bytes accepted for one usage event.
pub const MAX_IMAGE_ATTACHMENT_BYTES_PER_EVENT: usize = 8 * 1024 * 1024;
const MAX_BASE64_IMAGE_CHARACTERS: usize = 11_184_812;

/// Browser-safe raster media types accepted by the audit service.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum ImageMediaType {
    /// Portable Network Graphics image.
    #[serde(rename = "image/png")]
    Png,
    /// JPEG image.
    #[serde(rename = "image/jpeg")]
    Jpeg,
    /// WebP image.
    #[serde(rename = "image/webp")]
    Webp,
    /// Graphics Interchange Format image.
    #[serde(rename = "image/gif")]
    Gif,
}

impl ImageMediaType {
    /// Returns the canonical MIME type persisted and emitted by the API.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Gif => "image/gif",
        }
    }

    /// Parses a media type read from a database row.
    ///
    /// An unknown stored value is an internal data-integrity error rather than
    /// a client validation error because the migration constrains this column.
    pub fn from_stored(value: &str) -> Result<Self, AppError> {
        match value {
            "image/png" => Ok(Self::Png),
            "image/jpeg" => Ok(Self::Jpeg),
            "image/webp" => Ok(Self::Webp),
            "image/gif" => Ok(Self::Gif),
            _ => Err(AppError::internal(format!(
                "stored image attachment has invalid media type: {value}"
            ))),
        }
    }

    /// Returns the safe file extension used in attachment responses.
    #[must_use]
    pub const fn file_extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Jpeg => "jpg",
            Self::Webp => "webp",
            Self::Gif => "gif",
        }
    }

    fn matches_signature(self, bytes: &[u8]) -> bool {
        match self {
            Self::Png => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            Self::Jpeg => bytes.starts_with(b"\xff\xd8\xff"),
            Self::Webp => {
                bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP"
            }
            Self::Gif => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        }
    }
}

/// Image attachment supplied with an Agent usage ingest event.
#[derive(Debug, Deserialize)]
pub struct IngestImageAttachment {
    /// Zero-based display position within the event.
    pub position: i32,
    /// Declared browser-safe raster media type.
    pub media_type: ImageMediaType,
    /// Declared decoded byte length.
    pub byte_size: u64,
    /// Lowercase hexadecimal SHA-256 of the decoded content.
    pub sha256: String,
    /// Optional standard-base64 encoded image bytes.
    pub content_base64: Option<String>,
}

/// Validated attachment ready for database persistence.
#[derive(Debug)]
pub struct ValidatedImageAttachment {
    /// Unique non-negative display position.
    pub position: i32,
    /// Media type verified against the file signature when content is present.
    pub media_type: ImageMediaType,
    /// Decoded size converted to PostgreSQL's signed integer representation.
    pub byte_size: i64,
    /// Decoded 32-byte SHA-256 digest.
    pub sha256: Vec<u8>,
    /// Optional decoded image bytes.
    pub content: Option<Vec<u8>>,
}

/// Attachment metadata returned with usage event APIs.
#[derive(Debug, Serialize)]
pub struct UsageEventAttachmentResponse {
    /// Backend-generated attachment primary key.
    pub id: Uuid,
    /// Display position within the event.
    pub position: i32,
    /// Validated image media type.
    pub media_type: ImageMediaType,
    /// Decoded image byte length.
    pub byte_size: i64,
    /// Lowercase hexadecimal SHA-256 digest.
    pub sha256: String,
    /// Whether bytes can be fetched from the attachment endpoint.
    pub content_available: bool,
}

/// Authorized image bytes returned by the attachment download repository.
pub struct StoredImageAttachment {
    /// Validated media type used for the HTTP `Content-Type` header.
    pub media_type: ImageMediaType,
    /// Lowercase hexadecimal digest used for the HTTP entity tag.
    pub sha256: String,
    /// Authorized image bytes.
    pub content: Vec<u8>,
}

/// Validates and decodes all images for one event before opening a DB transaction.
pub fn validate_image_attachments(
    attachments: &[IngestImageAttachment],
) -> Result<Vec<ValidatedImageAttachment>, AppError> {
    if attachments.len() > MAX_IMAGE_ATTACHMENTS_PER_EVENT {
        return Err(AppError::validation(format!(
            "event attachments must contain at most {MAX_IMAGE_ATTACHMENTS_PER_EVENT} images"
        )));
    }

    let mut positions = HashSet::new();
    let mut aggregate_bytes = 0usize;
    let mut validated = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        if attachment.position < 0_i32 || !positions.insert(attachment.position) {
            return Err(AppError::validation(
                "image attachment positions must be unique non-negative integers".to_owned(),
            ));
        }
        let byte_size = usize::try_from(attachment.byte_size).map_err(|_error| {
            AppError::validation("image attachment byte_size is too large".to_owned())
        })?;
        if byte_size == 0 || byte_size > MAX_IMAGE_ATTACHMENT_BYTES_PER_EVENT {
            return Err(AppError::validation(format!(
                "image attachment byte_size must be between 1 and {MAX_IMAGE_ATTACHMENT_BYTES_PER_EVENT}"
            )));
        }
        aggregate_bytes = aggregate_bytes.checked_add(byte_size).ok_or_else(|| {
            AppError::validation("image attachment aggregate byte_size is too large".to_owned())
        })?;
        if aggregate_bytes > MAX_IMAGE_ATTACHMENT_BYTES_PER_EVENT {
            return Err(AppError::validation(format!(
                "event image attachments must total at most {MAX_IMAGE_ATTACHMENT_BYTES_PER_EVENT} bytes"
            )));
        }

        let sha256 = decode_sha256(&attachment.sha256)?;
        // Metadata-only attachments intentionally retain a digest and size but
        // have no downloadable body. When bytes are present, every declaration
        // is verified before they enter the persistence layer.
        let content = attachment
            .content_base64
            .as_deref()
            .map(|encoded| decode_content(attachment, encoded, byte_size, &sha256))
            .transpose()?;
        let byte_size = i64::try_from(byte_size).map_err(|_error| {
            AppError::validation("image attachment byte_size is too large".to_owned())
        })?;
        validated.push(ValidatedImageAttachment {
            position: attachment.position,
            media_type: attachment.media_type,
            byte_size,
            sha256,
            content,
        });
    }
    validated.sort_by_key(|attachment| attachment.position);
    Ok(validated)
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, AppError> {
    let decoded = hex::decode(value.trim()).map_err(|_error| {
        AppError::validation("image attachment sha256 must be lowercase hexadecimal".to_owned())
    })?;
    if decoded.len() != 32 || value.trim() != hex::encode(&decoded) {
        return Err(AppError::validation(
            "image attachment sha256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(decoded)
}

fn decode_content(
    attachment: &IngestImageAttachment,
    encoded: &str,
    expected_size: usize,
    expected_sha256: &[u8],
) -> Result<Vec<u8>, AppError> {
    // Reject overlong encoded data before allocating the decoded buffer. The
    // limit includes base64 expansion and a small allowance for padding.
    if encoded.len() > MAX_BASE64_IMAGE_CHARACTERS {
        return Err(AppError::validation(
            "image attachment content exceeds the decoded size limit".to_owned(),
        ));
    }
    let content = STANDARD.decode(encoded).map_err(|_error| {
        AppError::validation("image attachment content_base64 is invalid".to_owned())
    })?;
    if content.len() != expected_size {
        return Err(AppError::validation(
            "image attachment byte_size does not match decoded content".to_owned(),
        ));
    }
    if Sha256::digest(&content).as_slice() != expected_sha256 {
        return Err(AppError::validation(
            "image attachment sha256 does not match decoded content".to_owned(),
        ));
    }
    if !attachment.media_type.matches_signature(&content) {
        return Err(AppError::validation(
            "image attachment media_type does not match its file signature".to_owned(),
        ));
    }
    Ok(content)
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};

    use super::{ImageMediaType, IngestImageAttachment, validate_image_attachments};

    const PNG_BYTES: &[u8] = b"\x89PNG\r\n\x1a\nbackend-image";
    const JPEG_BYTES: &[u8] = b"\xff\xd8\xffbackend-image";
    const WEBP_BYTES: &[u8] = b"RIFF\x04\x00\x00\x00WEBPbackend-image";
    const GIF_BYTES: &[u8] = b"GIF89abackend-image";

    fn attachment(content: Option<&[u8]>) -> IngestImageAttachment {
        attachment_for(ImageMediaType::Png, PNG_BYTES, 0, content)
    }

    fn attachment_for(
        media_type: ImageMediaType,
        bytes: &[u8],
        position: i32,
        content: Option<&[u8]>,
    ) -> IngestImageAttachment {
        IngestImageAttachment {
            position,
            media_type,
            byte_size: u64::try_from(bytes.len()).expect("fixture size should fit"),
            sha256: hex::encode(Sha256::digest(bytes)),
            content_base64: content.map(|content| STANDARD.encode(content)),
        }
    }

    #[test]
    fn validates_plaintext_and_usage_only_attachments() {
        let plaintext = validate_image_attachments(&[attachment(Some(PNG_BYTES))])
            .expect("valid plaintext attachment should pass");
        assert_eq!(plaintext[0].content.as_deref(), Some(PNG_BYTES));

        let usage_only = validate_image_attachments(&[attachment(None)])
            .expect("valid metadata-only attachment should pass");
        assert!(usage_only[0].content.is_none());
    }

    #[test]
    fn validates_every_supported_image_media_type() {
        let attachments = [
            attachment_for(ImageMediaType::Png, PNG_BYTES, 0, Some(PNG_BYTES)),
            attachment_for(ImageMediaType::Jpeg, JPEG_BYTES, 1, Some(JPEG_BYTES)),
            attachment_for(ImageMediaType::Webp, WEBP_BYTES, 2, Some(WEBP_BYTES)),
            attachment_for(ImageMediaType::Gif, GIF_BYTES, 3, Some(GIF_BYTES)),
        ];

        let validated = validate_image_attachments(&attachments)
            .expect("all supported image media types should pass");

        assert_eq!(validated.len(), attachments.len());
        assert_eq!(validated[0].media_type, ImageMediaType::Png);
        assert_eq!(validated[1].media_type, ImageMediaType::Jpeg);
        assert_eq!(validated[2].media_type, ImageMediaType::Webp);
        assert_eq!(validated[3].media_type, ImageMediaType::Gif);
    }

    #[test]
    fn rejects_hash_size_signature_and_position_mismatches() {
        let mut wrong_hash = attachment(Some(PNG_BYTES));
        wrong_hash.sha256 = "0".repeat(64);
        assert!(validate_image_attachments(&[wrong_hash]).is_err());

        let mut wrong_size = attachment(Some(PNG_BYTES));
        wrong_size.byte_size += 1;
        assert!(validate_image_attachments(&[wrong_size]).is_err());

        let mut wrong_signature = attachment(Some(PNG_BYTES));
        wrong_signature.media_type = ImageMediaType::Jpeg;
        assert!(validate_image_attachments(&[wrong_signature]).is_err());

        let duplicate_positions = [attachment(None), attachment(None)];
        assert!(validate_image_attachments(&duplicate_positions).is_err());
    }
}
