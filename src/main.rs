//! Electronic-cat backend.
//!
//! `ROLE=all` (the default) runs both halves in one process:
//! a seed worker publishes one seed a second on Redis `seed_channel`,
//! and the SSE server subscribes to that channel and fans it out.
//! `ROLE=worker` publishes and drains hunger. `ROLE=sse` only fans out.
//! A second SSE process sees the same seeds because it subscribes itself.

use std::sync::Arc;
use std::time::Duration;

use e_pet_backend::http::{self, App};
use e_pet_backend::store::{self, Keys, SeedKind, SeedTick};
use e_pet_backend::trng::{self, Source};
use futures::StreamExt;
use tokio::sync::{broadcast, RwLock};
use tracing::info;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    All,
    Worker,
    Sse,
}

impl Role {
    fn from_env(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "worker" => Self::Worker,
            "sse" => Self::Sse,
            _ => Self::All,
        }
    }

    fn collects(self) -> bool {
        self != Self::Sse
    }

    fn serves(self) -> bool {
        self != Self::Worker
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "e_pet_backend=info".into()),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let prefix = std::env::var("KEY_PREFIX").unwrap_or_else(|_| "epet:".into());
    let source = Source::from_env(&std::env::var("TRNG_SOURCE").unwrap_or_else(|_| "os".into()));
    let role = Role::from_env(&std::env::var("ROLE").unwrap_or_else(|_| "all".into()));
    let api_key = std::env::var("RANDOM_ORG_API_KEY")
        .ok()
        .filter(|key| !key.is_empty());
    if source == Source::RandomOrg && api_key.is_none() {
        tracing::warn!("RANDOM_ORG_API_KEY is unset; seeds fall back to the OS CSPRNG");
    }

    let client = redis::Client::open(redis_url.as_str()).expect("REDIS_URL");
    let manager = redis::aio::ConnectionManager::new(client)
        .await
        .expect("redis connection");
    let (tx, _rx) = broadcast::channel(16);
    let app = App {
        redis: manager,
        keys: Keys::new(prefix),
        tx,
        current: Arc::new(RwLock::new(None)),
    };

    if role.serves() {
        let fan = app.clone();
        let url = redis_url.clone();
        tokio::spawn(async move { fan_in(fan, url).await });
    }
    if role.collects() {
        let collector = app.clone();
        tokio::spawn(async move { collect_seeds(collector, source, api_key).await });
        let hunger = app.clone();
        tokio::spawn(async move { drain_loop(hunger).await });
    }

    if role.serves() {
        let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind");
        info!(%bind, ?source, ?role, channel = %app.keys.channel(), "e-pet-backend listening");
        axum::serve(listener, http::router(app))
            .await
            .expect("serve");
    } else {
        info!(?source, ?role, channel = %app.keys.channel(), "seed worker running");
        std::future::pending::<()>().await;
    }
}

async fn fan_in(app: App, redis_url: String) {
    loop {
        match subscribe_once(&app, &redis_url).await {
            Ok(()) => tracing::warn!("seed subscription ended"),
            Err(err) => tracing::warn!(%err, "seed subscription failed"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn subscribe_once(app: &App, redis_url: &str) -> redis::RedisResult<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(app.keys.channel()).await?;
    info!(channel = %app.keys.channel(), "subscribed to seed channel");
    if let Ok(Some(tick)) = store::load_current(&mut app.redis.clone(), &app.keys).await {
        *app.current.write().await = Some(tick);
    }
    let mut messages = pubsub.on_message();
    while let Some(msg) = messages.next().await {
        let Ok(payload) = msg.get_payload::<String>() else {
            continue;
        };
        if let Ok(tick) = serde_json::from_str::<SeedTick>(&payload) {
            *app.current.write().await = Some(tick);
        }
        let _ = app.tx.send(payload);
    }
    Ok(())
}

async fn collect_seeds(app: App, source: Source, api_key: Option<String>) {
    let http = reqwest::Client::new();
    let mut last_timestamp = 0u64;
    loop {
        let timestamp = http::unix_now();
        if timestamp != last_timestamp {
            let draw = trng::draw(source, &http, api_key.as_deref()).await;
            let tick = SeedTick {
                seed: draw.seed,
                timestamp,
                kind: if draw.from_trng {
                    SeedKind::Trng
                } else {
                    SeedKind::LocalCsprngFallback
                },
            };
            let mut con = app.redis.clone();
            match store::save_seed(&mut con, &app.keys, &tick).await {
                Ok(()) => {
                    info!(timestamp, kind = tick.kind.as_str(), "published seed");
                    *app.current.write().await = Some(tick);
                    last_timestamp = timestamp;
                }
                Err(err) => tracing::warn!(%err, "could not store seed"),
            }
        }
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_millis())
            .unwrap_or(0);
        let remain = Duration::from_millis(u64::from(1000 - millis).max(1));
        tokio::time::sleep(remain).await;
    }
}

async fn drain_loop(app: App) {
    loop {
        let global = app.current.read().await.clone();
        if let Some(tick) = global {
            let mut con = app.redis.clone();
            match store::drain_due(&mut con, &app.keys, http::unix_now(), &tick.seed, 1000).await {
                Ok(done) if !done.is_empty() => info!(n = done.len(), "drained due cats"),
                Ok(_) => {}
                Err(err) => tracing::warn!(?err, "hunger drain failed"),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
