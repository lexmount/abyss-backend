//! Environment-backed standalone backend configuration.

use std::{env, net::SocketAddr, num::ParseIntError};

use crate::{error::AppError, identity::IdentityConfig};

const DEFAULT_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_ENVIRONMENT: &str = "local";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_POOL_SIZE: u32 = 10;
const DEFAULT_MAX_INGEST_BATCH_SIZE: usize = 1_000;
const DEFAULT_SUMMARY_SCAN_LIMIT: i64 = 100_000;
const DEFAULT_PAGE_SIZE: i64 = 100;
const DEFAULT_SEARCH_REQUEST_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_SEARCH_POLL_INTERVAL_MILLISECONDS: u64 = 500;
const DEFAULT_SEARCH_BATCH_SIZE: i64 = 100;

pub struct Config {
    pub addr: SocketAddr,
    pub environment: String,
    pub blackbox_allow_non_loopback: bool,
    pub log_level: String,
    pub database_url: String,
    pub database_pool_size: u32,
    pub run_migrations: bool,
    pub max_ingest_batch_size: usize,
    pub summary_scan_limit: i64,
    pub default_page_size: i64,
    pub identity: IdentityConfig,
    pub search: Option<SearchConfig>,
}

impl Config {
    pub fn from_env() -> Result<Self, AppError> {
        let addr = read_env("ABYSS_BACKEND_ADDR", DEFAULT_ADDR)
            .parse::<SocketAddr>()
            .map_err(|error| {
                AppError::config(format!(
                    "ABYSS_BACKEND_ADDR must be a socket address: {error}"
                ))
            })?;

        Ok(Self {
            addr,
            environment: read_env("ABYSS_BACKEND_ENV", DEFAULT_ENVIRONMENT),
            blackbox_allow_non_loopback: read_bool_env(
                "ABYSS_BACKEND_BLACKBOX_ALLOW_NON_LOOPBACK",
                false,
            )?,
            log_level: read_env("ABYSS_BACKEND_LOG_LEVEL", DEFAULT_LOG_LEVEL),
            database_url: read_required_env("ABYSS_BACKEND_DATABASE_URL")?,
            database_pool_size: read_positive_u32_env(
                "ABYSS_BACKEND_DATABASE_POOL_SIZE",
                DEFAULT_POOL_SIZE,
            )?,
            run_migrations: read_bool_env("ABYSS_BACKEND_RUN_MIGRATIONS", true)?,
            max_ingest_batch_size: read_positive_usize_env(
                "ABYSS_BACKEND_MAX_INGEST_BATCH_SIZE",
                DEFAULT_MAX_INGEST_BATCH_SIZE,
            )?,
            summary_scan_limit: read_positive_i64_env(
                "ABYSS_BACKEND_SUMMARY_SCAN_LIMIT",
                DEFAULT_SUMMARY_SCAN_LIMIT,
            )?,
            default_page_size: read_positive_i64_env(
                "ABYSS_BACKEND_DEFAULT_PAGE_SIZE",
                DEFAULT_PAGE_SIZE,
            )?,
            identity: IdentityConfig::parse(&read_required_env("ABYSS_BACKEND_API_TOKEN_SHA256")?)?,
            search: SearchConfig::from_env()?,
        })
    }
}

#[derive(Clone)]
pub struct SearchConfig {
    pub endpoint: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub request_timeout_seconds: u64,
    pub poll_interval_milliseconds: u64,
    pub batch_size: i64,
}

impl SearchConfig {
    fn from_env() -> Result<Option<Self>, AppError> {
        let endpoint = env_value("ABYSS_BACKEND_ELASTICSEARCH_URL");
        let username = env_value("ABYSS_BACKEND_ELASTICSEARCH_USERNAME");
        let password = env_value("ABYSS_BACKEND_ELASTICSEARCH_PASSWORD");

        let Some(endpoint) = endpoint else {
            if username.is_some() || password.is_some() {
                return Err(AppError::config(
                    "ABYSS_BACKEND_ELASTICSEARCH_URL is required when Elasticsearch credentials are configured"
                        .to_owned(),
                ));
            }
            return Ok(None);
        };

        if username.is_some() != password.is_some() {
            return Err(AppError::config(
                "ABYSS_BACKEND_ELASTICSEARCH_USERNAME and ABYSS_BACKEND_ELASTICSEARCH_PASSWORD must be configured together"
                    .to_owned(),
            ));
        }

        Ok(Some(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            username,
            password,
            request_timeout_seconds: read_positive_u64_env(
                "ABYSS_BACKEND_SEARCH_REQUEST_TIMEOUT_SECONDS",
                DEFAULT_SEARCH_REQUEST_TIMEOUT_SECONDS,
            )?,
            poll_interval_milliseconds: read_positive_u64_env(
                "ABYSS_BACKEND_SEARCH_POLL_INTERVAL_MILLISECONDS",
                DEFAULT_SEARCH_POLL_INTERVAL_MILLISECONDS,
            )?,
            batch_size: read_positive_i64_env(
                "ABYSS_BACKEND_SEARCH_BATCH_SIZE",
                DEFAULT_SEARCH_BATCH_SIZE,
            )?,
        }))
    }
}

fn read_env(key: &str, fallback: &str) -> String {
    env_value(key).unwrap_or_else(|| fallback.to_owned())
}

fn read_required_env(key: &str) -> Result<String, AppError> {
    env_value(key).ok_or_else(|| AppError::config(format!("{key} is required")))
}

fn env_value(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_bool_env(key: &str, fallback: bool) -> Result<bool, AppError> {
    let Some(raw) = env_value(key) else {
        return Ok(fallback);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(AppError::config(format!(
            "{key} must be true/false, yes/no, on/off, or 1/0"
        ))),
    }
}

fn read_positive_u32_env(key: &str, fallback: u32) -> Result<u32, AppError> {
    let value = parse_env(key, fallback, str::parse::<u32>)?;
    require_positive(key, value)
}

fn read_positive_u64_env(key: &str, fallback: u64) -> Result<u64, AppError> {
    let value = parse_env(key, fallback, str::parse::<u64>)?;
    require_positive(key, value)
}

fn read_positive_usize_env(key: &str, fallback: usize) -> Result<usize, AppError> {
    let value = parse_env(key, fallback, str::parse::<usize>)?;
    require_positive(key, value)
}

fn read_positive_i64_env(key: &str, fallback: i64) -> Result<i64, AppError> {
    let value = parse_env(key, fallback, str::parse::<i64>)?;
    require_positive(key, value)
}

fn require_positive<T>(key: &str, value: T) -> Result<T, AppError>
where
    T: Default + PartialOrd,
{
    if value <= T::default() {
        return Err(AppError::config(format!("{key} must be greater than 0")));
    }
    Ok(value)
}

fn parse_env<T, F>(key: &str, fallback: T, parser: F) -> Result<T, AppError>
where
    F: FnOnce(&str) -> Result<T, ParseIntError>,
{
    let Some(raw) = env_value(key) else {
        return Ok(fallback);
    };
    parser(&raw).map_err(|error| AppError::config(format!("{key} is invalid: {error}")))
}
