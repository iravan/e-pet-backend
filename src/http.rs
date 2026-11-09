//! HTTP API. The seed stream is SSE. A loot claim hits `/verify`, which
//! answers immediately and replays the roll on a background task.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

use crate::crypto::{self, LUCKY_ITEM_EVENT, MAX_ACTION_NONCE};
use crate::store::{self, Cat, CatError, Keys, SeedTick};

#[derive(Clone)]
pub struct App {
    pub redis: ConnectionManager,
    pub keys: Keys,
    pub tx: broadcast::Sender<String>,
    pub current: Arc<RwLock<Option<SeedTick>>>,
}

pub fn router(app: App) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/stream-seed", get(stream_seed))
        .route("/api/cats", post(create_cat))
        .route("/api/cats/{id}", get(read_cat))
        .route("/api/cats/{id}/feed", post(feed_cat))
        .route("/api/cats/{id}/revive", post(revive_cat))
        .route("/api/cats/{id}/verify", post(verify_cat))
        .with_state(app)
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn health(State(app): State<App>) -> StatusCode {
    let mut con = app.redis.clone();
    match redis::cmd("PING").query_async::<String>(&mut con).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn stream_seed(
    State(app): State<App>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let rx = app.tx.subscribe();
    let mut current = app.current.read().await.clone();
    if current.is_none() {
        let mut con = app.redis.clone();
        current = store::load_current(&mut con, &app.keys)
            .await
            .ok()
            .flatten();
    }
    let first =
        futures::stream::iter(current.map(|tick| {
            Ok(Event::default().data(serde_json::to_string(&tick).unwrap_or_default()))
        }));
    let live = BroadcastStream::new(rx).filter_map(|msg| async move {
        match msg {
            Ok(body) => Some(Ok(Event::default().data(body))),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_)) => {
                Some(Ok(Event::default().data(r#"{"error":"client_lag"}"#)))
            }
        }
    });
    Sse::new(first.chain(live)).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    )
}

#[derive(Deserialize)]
struct NewCat {
    id: String,
    name: Option<String>,
}

async fn create_cat(
    State(app): State<App>,
    Json(body): Json<NewCat>,
) -> Result<(StatusCode, Json<Cat>), (StatusCode, String)> {
    let mut con = app.redis.clone();
    let name = body.name.unwrap_or_else(|| body.id.clone());
    store::create_cat(&mut con, &app.keys, &body.id, &name, unix_now())
        .await
        .map(|cat| (StatusCode::CREATED, Json(cat)))
        .map_err(http_error)
}

async fn read_cat(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Json<Cat>, (StatusCode, String)> {
    let mut con = app.redis.clone();
    store::get_cat(&mut con, &app.keys, &id)
        .await
        .map(Json)
        .map_err(http_error)
}

#[derive(Deserialize)]
struct FeedBody {
    amount: Option<i32>,
}

#[derive(Serialize)]
struct FeedResponse {
    fullness: i32,
}

async fn feed_cat(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<FeedBody>,
) -> Result<Json<FeedResponse>, (StatusCode, String)> {
    let mut con = app.redis.clone();
    store::feed(
        &mut con,
        &app.keys,
        &id,
        body.amount.unwrap_or(30),
        unix_now(),
    )
    .await
    .map(|fullness| Json(FeedResponse { fullness }))
    .map_err(http_error)
}

#[derive(Deserialize)]
struct ReviveBody {
    fullness: Option<i32>,
}

async fn revive_cat(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<ReviveBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut con = app.redis.clone();
    store::revive(
        &mut con,
        &app.keys,
        &id,
        body.fullness.unwrap_or(40),
        unix_now(),
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
    .map_err(http_error)
}

#[derive(Deserialize)]
struct VerifyBody {
    timestamp: u64,
    triggered_event: String,
    action_nonce: u64,
    cat_id: Option<String>,
}

#[derive(Serialize)]
struct Received {
    status: &'static str,
}

async fn verify_cat(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<VerifyBody>,
) -> Result<(StatusCode, Json<Received>), (StatusCode, String)> {
    if !crypto::valid_cat_id(&id) {
        return Err((StatusCode::BAD_REQUEST, "invalid cat id".into()));
    }
    if body.cat_id.as_ref().is_some_and(|other| other != &id) {
        return Err((StatusCode::BAD_REQUEST, "cat id mismatch".into()));
    }
    if body.triggered_event != LUCKY_ITEM_EVENT {
        return Err((StatusCode::BAD_REQUEST, "unknown event".into()));
    }
    if body.action_nonce == 0 || body.action_nonce > MAX_ACTION_NONCE {
        return Err((StatusCode::BAD_REQUEST, "action_nonce out of range".into()));
    }
    let timestamp = body.timestamp;
    let nonce = body.action_nonce;
    let cat_id = id.clone();
    tokio::spawn(async move {
        let mut con = app.redis.clone();
        match store::judge(&mut con, &app.keys, &cat_id, timestamp, nonce).await {
            Ok(verdict) => {
                tracing::info!(%cat_id, timestamp, nonce, verdict = verdict.as_str(), "loot verify")
            }
            Err(err) => tracing::warn!(%cat_id, ?err, "loot verify failed"),
        }
    });
    Ok((StatusCode::ACCEPTED, Json(Received { status: "received" })))
}

fn http_error(err: CatError) -> (StatusCode, String) {
    match err {
        CatError::InvalidId => (StatusCode::BAD_REQUEST, "invalid cat id".into()),
        CatError::Missing => (StatusCode::NOT_FOUND, "cat not found".into()),
        CatError::Exists => (StatusCode::CONFLICT, "cat already exists".into()),
        CatError::NotAlive => (StatusCode::CONFLICT, "cat is dead".into()),
        CatError::Redis(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
    }
}
