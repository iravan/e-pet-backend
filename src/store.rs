//! Redis layout.
//!
//! `hunger` is a sorted set. The score is the next unix second a living cat
//! should lose satiety. A dead cat is removed from that set, so the worker
//! never reads it again. Updates go through Lua so a feed cannot race a drain.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::crypto::{self, FULLNESS_CAP};

/// Hot cache of the latest payload. The handbook's replay window.
const CURRENT_TTL_SECS: u64 = 10;
/// Per-second history. Loot claims quote a timestamp, so this outlives the hot cache.
const SEED_TTL_SECS: u64 = 180;

#[derive(Clone, Debug)]
pub struct Keys {
    prefix: String,
}

impl Keys {
    pub fn new(prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with(':') {
            prefix.push(':');
        }
        Self { prefix }
    }

    fn hunger(&self) -> String {
        format!("{}hunger", self.prefix)
    }

    fn cat(&self, id: &str) -> String {
        format!("{}cat:{}", self.prefix, id)
    }

    /// Pub/sub name. An empty prefix is the handbook's `seed_channel`.
    pub fn channel(&self) -> String {
        format!("{}seed_channel", self.prefix)
    }

    fn global(&self) -> String {
        format!("{}global_current_seed", self.prefix)
    }

    fn seed_at(&self, timestamp: u64) -> String {
        format!("{}seed:{}", self.prefix, timestamp)
    }

    fn verdict(&self, id: &str, timestamp: u64, nonce: u64) -> String {
        format!("{}verdict:{id}:{timestamp}:{nonce}", self.prefix)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeedKind {
    #[serde(rename = "TRNG")]
    Trng,
    #[serde(rename = "LOCAL_CSPRNG_FALLBACK")]
    LocalCsprngFallback,
}

impl SeedKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trng => "TRNG",
            Self::LocalCsprngFallback => "LOCAL_CSPRNG_FALLBACK",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedTick {
    pub seed: String,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub kind: SeedKind,
}

#[derive(Clone, Debug, Serialize)]
pub struct Cat {
    pub id: String,
    pub name: String,
    pub fullness: i32,
    pub status: String,
}

#[derive(Debug)]
pub enum CatError {
    InvalidId,
    Missing,
    Exists,
    NotAlive,
    Redis(redis::RedisError),
}

impl From<redis::RedisError> for CatError {
    fn from(value: redis::RedisError) -> Self {
        Self::Redis(value)
    }
}

pub async fn save_seed(
    con: &mut ConnectionManager,
    keys: &Keys,
    tick: &SeedTick,
) -> redis::RedisResult<()> {
    let body = serde_json::to_string(tick).unwrap_or_default();
    let _: () = con.set_ex(keys.global(), &body, CURRENT_TTL_SECS).await?;
    let _: () = con
        .set_ex(keys.seed_at(tick.timestamp), &tick.seed, SEED_TTL_SECS)
        .await?;
    let _: () = con.publish(keys.channel(), &body).await?;
    Ok(())
}

pub async fn load_current(
    con: &mut ConnectionManager,
    keys: &Keys,
) -> redis::RedisResult<Option<SeedTick>> {
    let body: Option<String> = con.get(keys.global()).await?;
    Ok(body.and_then(|text| serde_json::from_str(&text).ok()))
}

pub async fn load_seed(
    con: &mut ConnectionManager,
    keys: &Keys,
    timestamp: u64,
) -> redis::RedisResult<Option<String>> {
    let hex_seed: Option<String> = con.get(keys.seed_at(timestamp)).await?;
    Ok(hex_seed.filter(|text| crypto::valid_seed(text)))
}

pub async fn create_cat(
    con: &mut ConnectionManager,
    keys: &Keys,
    id: &str,
    name: &str,
    now: u64,
) -> Result<Cat, CatError> {
    if !crypto::valid_cat_id(id) {
        return Err(CatError::InvalidId);
    }
    let key = keys.cat(id);
    let created: bool = redis::cmd("HSETNX")
        .arg(&key)
        .arg("status")
        .arg("ALIVE")
        .query_async(con)
        .await?;
    if !created {
        return Err(CatError::Exists);
    }
    let _: () = con.hset(&key, "fullness", FULLNESS_CAP).await?;
    let _: () = con.hset(&key, "name", name).await?;
    let _: () = con.zadd(keys.hunger(), id, now + 60).await?;
    Ok(Cat {
        id: id.to_string(),
        name: name.to_string(),
        fullness: FULLNESS_CAP,
        status: "ALIVE".to_string(),
    })
}

pub async fn get_cat(con: &mut ConnectionManager, keys: &Keys, id: &str) -> Result<Cat, CatError> {
    if !crypto::valid_cat_id(id) {
        return Err(CatError::InvalidId);
    }
    let key = keys.cat(id);
    let status: Option<String> = con.hget(&key, "status").await?;
    let Some(status) = status else {
        return Err(CatError::Missing);
    };
    let fullness: i32 = con.hget(&key, "fullness").await?;
    let name: String = con.hget(&key, "name").await.unwrap_or_default();
    Ok(Cat {
        id: id.to_string(),
        name,
        fullness,
        status,
    })
}

const FEED: &str = r#"
local key = KEYS[1]
local queue = KEYS[2]
local id = ARGV[1]
local add = tonumber(ARGV[2])
local cap = tonumber(ARGV[3])
local next_ts = tonumber(ARGV[4])
local status = redis.call('HGET', key, 'status')
if status ~= 'ALIVE' then
  return 'not-alive'
end
local fullness = tonumber(redis.call('HGET', key, 'fullness') or '0') + add
if fullness > cap then fullness = cap end
redis.call('HSET', key, 'fullness', fullness)
redis.call('ZADD', queue, next_ts, id)
return tostring(fullness)
"#;

pub async fn feed(
    con: &mut ConnectionManager,
    keys: &Keys,
    id: &str,
    amount: i32,
    now: u64,
) -> Result<i32, CatError> {
    if !crypto::valid_cat_id(id) {
        return Err(CatError::InvalidId);
    }
    let amount = amount.clamp(1, FULLNESS_CAP);
    let reply: String = redis::Script::new(FEED)
        .key(keys.cat(id))
        .key(keys.hunger())
        .arg(id)
        .arg(amount)
        .arg(FULLNESS_CAP)
        .arg(now + 60)
        .invoke_async(con)
        .await?;
    if reply == "not-alive" {
        let exists: bool = con.exists(keys.cat(id)).await?;
        return Err(if exists {
            CatError::NotAlive
        } else {
            CatError::Missing
        });
    }
    Ok(reply.parse().unwrap_or(0))
}

const REVIVE: &str = r#"
local key = KEYS[1]
local queue = KEYS[2]
local id = ARGV[1]
local fullness = tonumber(ARGV[2])
local next_ts = tonumber(ARGV[3])
if redis.call('EXISTS', key) == 0 then
  return 'missing'
end
redis.call('HSET', key, 'status', 'ALIVE', 'fullness', fullness)
redis.call('ZADD', queue, next_ts, id)
return 'ok'
"#;

pub async fn revive(
    con: &mut ConnectionManager,
    keys: &Keys,
    id: &str,
    fullness: i32,
    now: u64,
) -> Result<(), CatError> {
    if !crypto::valid_cat_id(id) {
        return Err(CatError::InvalidId);
    }
    let fullness = fullness.clamp(1, FULLNESS_CAP);
    let reply: String = redis::Script::new(REVIVE)
        .key(keys.cat(id))
        .key(keys.hunger())
        .arg(id)
        .arg(fullness)
        .arg(now + 60)
        .invoke_async(con)
        .await?;
    if reply == "missing" {
        return Err(CatError::Missing);
    }
    Ok(())
}

const DRAIN: &str = r#"
local key = KEYS[1]
local queue = KEYS[2]
local id = ARGV[1]
local decrease = tonumber(ARGV[2])
local next_ts = tonumber(ARGV[3])
if redis.call('EXISTS', key) == 0 then
  redis.call('ZREM', queue, id)
  return 'missing'
end
local status = redis.call('HGET', key, 'status')
if status ~= 'ALIVE' then
  redis.call('ZREM', queue, id)
  return 'dead'
end
local fullness = tonumber(redis.call('HGET', key, 'fullness') or '0') - decrease
if fullness <= 0 then
  redis.call('HSET', key, 'fullness', 0, 'status', 'DEAD')
  redis.call('ZREM', queue, id)
  return 'died'
end
redis.call('HSET', key, 'fullness', fullness)
redis.call('ZADD', queue, next_ts, id)
return 'alive'
"#;

#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    Alive,
    Died,
    Skipped,
}

/// Drain every cat whose score is due. At most `limit` cats per call.
pub async fn drain_due(
    con: &mut ConnectionManager,
    keys: &Keys,
    now: u64,
    global_seed: &str,
    limit: isize,
) -> Result<Vec<(String, DrainOutcome)>, CatError> {
    let due: Vec<String> = con
        .zrangebyscore_limit(keys.hunger(), "-inf", now.to_string(), 0, limit)
        .await?;
    let minute = now / 60;
    let mut outcomes = Vec::with_capacity(due.len());
    for id in due {
        let amount = crypto::drain_amount(global_seed, &id, minute);
        let reply: String = redis::Script::new(DRAIN)
            .key(keys.cat(&id))
            .key(keys.hunger())
            .arg(&id)
            .arg(amount)
            .arg(now + 60)
            .invoke_async(con)
            .await?;
        let outcome = match reply.as_str() {
            "alive" => DrainOutcome::Alive,
            "died" => DrainOutcome::Died,
            _ => DrainOutcome::Skipped,
        };
        outcomes.push((id, outcome));
    }
    Ok(outcomes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Accepted,
    Rejected,
    Expired,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }
}

/// Replay a loot claim from the seed stored for `timestamp`.
///
/// The result is written under `verdict:{id}:{timestamp}:{nonce}` for 180 seconds.
pub async fn judge(
    con: &mut ConnectionManager,
    keys: &Keys,
    id: &str,
    timestamp: u64,
    nonce: u64,
) -> Result<Verdict, CatError> {
    let verdict = match load_seed(con, keys, timestamp).await? {
        Some(seed) if crypto::lucky_hit(&seed, id, nonce) => Verdict::Accepted,
        Some(_) => Verdict::Rejected,
        None => Verdict::Expired,
    };
    let _: () = con
        .set_ex(
            keys.verdict(id, timestamp, nonce),
            verdict.as_str(),
            SEED_TTL_SECS,
        )
        .await?;
    Ok(verdict)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_conn(label: &str) -> Option<(ConnectionManager, Keys)> {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let client = redis::Client::open(url).ok()?;
        let con = ConnectionManager::new(client).await.ok()?;
        let keys = Keys::new(format!(
            "epet:test:{label}:{}:{}:",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Some((con, keys))
    }

    #[tokio::test]
    async fn hunger_queue_drops_a_dead_cat_and_feed_moves_the_score() {
        let Some((mut con, keys)) = test_conn("hunger").await else {
            eprintln!("redis is not running; skip store test");
            return;
        };
        let now = 1_700_000_000;
        create_cat(&mut con, &keys, "momo", "Momo", now)
            .await
            .unwrap();
        let fed = feed(&mut con, &keys, "momo", 10, now).await.unwrap();
        assert_eq!(fed, FULLNESS_CAP);

        let _: () = con.zadd(keys.hunger(), "momo", now - 1).await.unwrap();
        let _: () = con.hset(keys.cat("momo"), "fullness", 1).await.unwrap();
        let global = "0123456789abcdef";
        let outcomes = drain_due(&mut con, &keys, now, global, 100).await.unwrap();
        assert_eq!(outcomes, vec![("momo".to_string(), DrainOutcome::Died)]);
        let dead = get_cat(&mut con, &keys, "momo").await.unwrap();
        assert_eq!(dead.status, "DEAD");
        assert_eq!(dead.fullness, 0);

        let again = drain_due(&mut con, &keys, now + 120, &global, 100)
            .await
            .unwrap();
        assert!(again.is_empty());

        revive(&mut con, &keys, "momo", 40, now).await.unwrap();
        let living = get_cat(&mut con, &keys, "momo").await.unwrap();
        assert_eq!(living.status, "ALIVE");
        assert_eq!(living.fullness, 40);

        let _: () = con.del(keys.cat("momo")).await.unwrap();
        let _: () = con.del(keys.hunger()).await.unwrap();
    }

    #[tokio::test]
    async fn save_seed_keeps_history_and_judge_replays_it() {
        let Some((mut con, keys)) = test_conn("seed").await else {
            eprintln!("redis is not running; skip store test");
            return;
        };
        let tick = SeedTick {
            seed: "0123456789abcdef".to_string(),
            timestamp: 1_715_002_345,
            kind: SeedKind::Trng,
        };
        save_seed(&mut con, &keys, &tick).await.unwrap();
        let current = load_current(&mut con, &keys).await.unwrap().unwrap();
        assert_eq!(current, tick);
        let ttl: i64 = con.ttl(keys.global()).await.unwrap();
        assert!((1..=10).contains(&ttl));

        let loaded = load_seed(&mut con, &keys, tick.timestamp)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, tick.seed);
        let verdict = judge(&mut con, &keys, "momo", tick.timestamp, 245)
            .await
            .unwrap();
        let expect = if crypto::lucky_hit(&tick.seed, "momo", 245) {
            Verdict::Accepted
        } else {
            Verdict::Rejected
        };
        assert_eq!(verdict, expect);
        let stored: String = con
            .get(keys.verdict("momo", tick.timestamp, 245))
            .await
            .unwrap();
        assert_eq!(stored, verdict.as_str());

        let gone = judge(&mut con, &keys, "momo", 1, 1).await.unwrap();
        assert_eq!(gone, Verdict::Expired);

        let _: () = con.del(keys.global()).await.unwrap();
        let _: () = con.del(keys.seed_at(tick.timestamp)).await.unwrap();
        let _: () = con
            .del(keys.verdict("momo", tick.timestamp, 245))
            .await
            .unwrap();
        let _: () = con.del(keys.verdict("momo", 1, 1)).await.unwrap();
    }
}
