//! Standalone bearer authentication mapped to one deployment owner.
//!
//! This module is intentionally not a login or SSO implementation. Operators
//! provision one opaque API token, store only its SHA-256 hash in configuration,
//! and every valid request maps to [`OWNER_ID`]. This keeps authorization data
//! ownership explicit until a multi-user identity system is introduced.

use axum::http::{HeaderMap, header};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq as _;
use uuid::Uuid;

use crate::error::AppError;

/// Stable database owner seeded by the initial migration.
pub const OWNER_ID: Uuid = Uuid::from_u128(1);

#[derive(Clone)]
/// Canonical SHA-256 digest of the deployment bearer token.
pub struct IdentityConfig {
    token_hash: [u8; 32],
}

impl IdentityConfig {
    /// Parses exactly 32 bytes encoded as lowercase hexadecimal.
    ///
    /// Requiring canonical encoding avoids accepting multiple textual forms of
    /// the same secret and catches malformed deployment secrets at startup.
    pub fn parse(encoded_hash: &str) -> Result<Self, AppError> {
        let bytes = hex::decode(encoded_hash).map_err(|_error| invalid_hash())?;
        let token_hash = <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_error| invalid_hash())?;
        if encoded_hash != hex::encode(token_hash) {
            return Err(invalid_hash());
        }
        Ok(Self { token_hash })
    }
}

#[derive(Clone)]
/// Validates HTTP bearer credentials for the standalone owner.
pub struct IdentityAuthenticator {
    config: IdentityConfig,
}

impl IdentityAuthenticator {
    /// Creates an authenticator from startup-validated configuration.
    #[must_use]
    pub const fn new(config: IdentityConfig) -> Self {
        Self { config }
    }

    /// Authenticates one request and returns the database owner identifier.
    pub fn authenticate(&self, headers: &HeaderMap) -> Result<Uuid, AppError> {
        let token = bearer_token(headers)?;
        let presented_hash = Sha256::digest(token.as_bytes());
        // Constant-time comparison avoids leaking how many digest bytes match.
        if !bool::from(
            presented_hash
                .as_slice()
                .ct_eq(self.config.token_hash.as_slice()),
        ) {
            return Err(AppError::unauthorized("invalid bearer token".to_owned()));
        }
        Ok(OWNER_ID)
    }
}

fn invalid_hash() -> AppError {
    AppError::config(
        "ABYSS_BACKEND_API_TOKEN_SHA256 must be 64 lowercase hexadecimal characters".to_owned(),
    )
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, AppError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| AppError::unauthorized("bearer token required".to_owned()))?
        .to_str()
        .map_err(|_error| AppError::unauthorized("invalid authorization header".to_owned()))?;
    let mut parts = value.split_whitespace();
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || parts.next().is_some() {
        return Err(AppError::unauthorized(
            "authorization header must contain one bearer token".to_owned(),
        ));
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};
    use sha2::{Digest, Sha256};

    use super::{IdentityAuthenticator, IdentityConfig, OWNER_ID};

    #[test]
    fn valid_bearer_token_authenticates_the_standalone_owner() {
        let authenticator = authenticator("correct-token");
        let headers = authorization("Bearer correct-token");

        assert_eq!(
            authenticator
                .authenticate(&headers)
                .expect("configured token should authenticate"),
            OWNER_ID
        );
    }

    #[test]
    fn missing_malformed_and_incorrect_tokens_are_rejected() {
        let authenticator = authenticator("correct-token");
        for headers in [
            HeaderMap::new(),
            authorization("Basic correct-token"),
            authorization("Bearer"),
            authorization("Bearer incorrect-token"),
            authorization("Bearer correct-token extra"),
        ] {
            assert!(
                authenticator.authenticate(&headers).is_err(),
                "invalid authorization must not authenticate"
            );
        }
    }

    #[test]
    fn configured_hash_requires_canonical_sha256_hex() {
        for value in ["", "abc", &"A".repeat(64), &"0".repeat(62)] {
            assert!(
                IdentityConfig::parse(value).is_err(),
                "non-canonical token hash must fail"
            );
        }
    }

    fn authenticator(token: &str) -> IdentityAuthenticator {
        let encoded = hex::encode(Sha256::digest(token.as_bytes()));
        IdentityAuthenticator::new(
            IdentityConfig::parse(&encoded).expect("test token hash should parse"),
        )
    }

    fn authorization(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(value).expect("test authorization header should parse"),
        );
        headers
    }
}
