//! One entropy draw per second.
//!
//! `os` is 8 bytes from `getrandom`, hex-encoded to 16 characters.
//! `random_org` calls RANDOM.ORG JSON-RPC `generateStrings` once, then falls
//! back to the OS source if the key is missing or the call fails.

use std::time::Duration;

use serde::Deserialize;

const RANDOM_ORG_URL: &str = "https://api.random.org/json-rpc/4/invoke";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Os,
    RandomOrg,
}

impl Source {
    pub fn from_env(value: &str) -> Self {
        if value.eq_ignore_ascii_case("random_org") {
            Self::RandomOrg
        } else {
            Self::Os
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Draw {
    pub seed: String,
    /// Set when the seed came from RANDOM.ORG. Fallback draws leave this false.
    pub from_trng: bool,
}

pub async fn draw(source: Source, http: &reqwest::Client, api_key: Option<&str>) -> Draw {
    if source == Source::RandomOrg {
        if let Some(key) = api_key.filter(|key| !key.is_empty()) {
            match random_org(http, key).await {
                Ok(seed) => {
                    return Draw {
                        seed,
                        from_trng: true,
                    }
                }
                Err(err) => tracing::warn!(%err, "RANDOM.ORG failed; using the OS CSPRNG"),
            }
        }
    }
    Draw {
        seed: os_seed(),
        from_trng: false,
    }
}

fn os_seed() -> String {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).expect("OS entropy");
    hex::encode(bytes)
}

async fn random_org(http: &reqwest::Client, api_key: &str) -> Result<String, String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "generateStrings",
        "params": {
            "apiKey": api_key,
            "n": 1,
            "length": 16,
            "characters": "0123456789abcdef",
            "replacement": true
        },
        "id": 1
    });
    let text = http
        .post(RANDOM_ORG_URL)
        .timeout(Duration::from_millis(800))
        .json(&body)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .error_for_status()
        .map_err(|err| err.to_string())?
        .text()
        .await
        .map_err(|err| err.to_string())?;
    parse_random_org(&text).ok_or_else(|| "RANDOM.ORG returned no 16-hex seed".to_string())
}

/// Pull the first string out of a JSON-RPC `generateStrings` body.
pub fn parse_random_org(body: &str) -> Option<String> {
    let parsed: RpcResponse = serde_json::from_str(body).ok()?;
    if parsed.error.is_some() {
        return None;
    }
    let seed = parsed.result?.random.data.into_iter().next()?;
    crate::crypto::valid_seed(&seed).then_some(seed)
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<RpcResult>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RpcResult {
    random: RpcRandom,
}

#[derive(Deserialize)]
struct RpcRandom {
    data: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_generate_strings_body() {
        let body = r#"{"jsonrpc":"2.0","result":{"random":{"data":["0123456789abcdef"],"completionTime":"2026-11-08 14:00:00Z"},"bitsUsed":64,"bitsLeft":1,"requestsLeft":1,"advisoryDelay":0},"id":1}"#;
        assert_eq!(parse_random_org(body).as_deref(), Some("0123456789abcdef"));
    }

    #[test]
    fn rejects_an_rpc_error_and_a_short_string() {
        let err = r#"{"jsonrpc":"2.0","error":{"code":400,"message":"no"},"id":1}"#;
        assert_eq!(parse_random_org(err), None);
        let short = r#"{"jsonrpc":"2.0","result":{"random":{"data":["abc"]}},"id":1}"#;
        assert_eq!(parse_random_org(short), None);
    }

    #[tokio::test]
    async fn missing_api_key_stays_on_the_os() {
        let http = reqwest::Client::new();
        let draw = draw(Source::RandomOrg, &http, None).await;
        assert!(!draw.from_trng);
        assert!(crate::crypto::valid_seed(&draw.seed));
    }
}
