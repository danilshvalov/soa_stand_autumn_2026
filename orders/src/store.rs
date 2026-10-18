use anyhow::Context;
use chrono::{DateTime, Utc};
use garde::Validate;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use uuid::Uuid;

use crate::DB_CONNECT_TIMEOUT;

pub const CURRENCY: &str = "RUB";

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Item {
    #[garde(length(min = 1))]
    pub sku: String,
    #[garde(range(min = 1))]
    pub qty: i64,
    #[garde(range(min = 0))]
    pub price_cents: i64,
}

#[derive(Debug, Clone)]
pub struct ClaimedOrder {
    pub id: Uuid,
    pub stage: String,
    pub amount_cents: i64,
    pub currency: String,
    pub items: Json<Vec<Item>>,
    pub attempts: i32,
    pub post_timeout_ms: i32,
}

pub struct PendingUpdate<'a> {
    pub stage: &'a str,
    pub next_attempt_at: DateTime<Utc>,
    pub attempts: i32,
    pub post_timeout_ms: i32,
    pub last_error: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct ClaimedOutboxEvent {
    pub event_id: Uuid,
    pub order_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub attempts: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryApplyResult {
    Applied,
    Duplicate,
}

pub async fn connect(url: &str) -> anyhow::Result<PgPool> {
    let started = tokio::time::Instant::now();
    loop {
        match try_connect(url).await {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                tracing::warn!(error = %err, "database is unavailable, waiting");
                if started.elapsed() > DB_CONNECT_TIMEOUT {
                    return Err(err.context("timed out waiting for the database"));
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn try_connect(url: &str) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(url)
        .await
        .context("postgres connection")
}

pub async fn ping(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("SELECT 1").execute(pool).await.map(|_| ())
}

pub async fn insert_order(
    pool: &PgPool,
    id: Uuid,
    user_id: &str,
    items: &[Item],
    amount_cents: i64,
) -> sqlx::Result<()> {
    let items = serde_json::to_value(items).map_err(|err| sqlx::Error::Encode(Box::new(err)))?;
    sqlx::query!(
        r#"
        INSERT INTO orders (
            id,
            user_id,
            status,
            stage,
            amount_cents,
            currency,
            items
        )
        VALUES (
            $1,
            $2,
            'pending',
            'reserve',
            $3,
            $4,
            $5
        )
        "#,
        id,
        user_id,
        amount_cents,
        CURRENCY,
        items,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn order_status(pool: &PgPool, id: Uuid) -> sqlx::Result<Option<(String, i64)>> {
    let row = sqlx::query!("SELECT status, amount_cents FROM orders WHERE id = $1", id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| (row.status, row.amount_cents)))
}

pub async fn claim_due(
    pool: &PgPool,
    batch: i64,
    lease_until: DateTime<Utc>,
) -> sqlx::Result<Vec<ClaimedOrder>> {
    sqlx::query_as!(
        ClaimedOrder,
        r#"
        WITH due AS (
            SELECT id
            FROM orders
            WHERE status = 'pending' AND next_attempt_at <= now()
            ORDER BY (stage = 'verify') DESC, next_attempt_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE orders o
        SET
            next_attempt_at = $2,
            updated_at = now()
        FROM due
        WHERE o.id = due.id
        RETURNING o.id, o.stage, o.amount_cents, o.currency,
                  o.items as "items: Json<Vec<Item>>", o.attempts, o.post_timeout_ms
        "#,
        batch,
        lease_until,
    )
    .fetch_all(pool)
    .await
}

pub async fn save_pending_attempt(
    pool: &PgPool,
    id: Uuid,
    update: PendingUpdate<'_>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        UPDATE orders
        SET
            stage = $2,
            next_attempt_at = $3,
            attempts = $4,
            post_timeout_ms = $5,
            last_error = $6,
            updated_at = now()
        WHERE id = $1
        "#,
        id,
        update.stage,
        update.next_attempt_at,
        update.attempts,
        update.post_timeout_ms,
        update.last_error,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn finish_paid(pool: &PgPool, id: Uuid, attempts: i32) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query!(
        r#"
        UPDATE orders
        SET
            status = 'paid',
            stage = 'done',
            attempts = $2,
            next_attempt_at = now(),
            last_error = NULL,
            updated_at = now()
        WHERE id = $1
        "#,
        id,
        attempts,
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query!(
        r#"
        INSERT INTO order_paid_outbox (
            event_id,
            order_id
        )
        SELECT
            $2,
            id
        FROM orders
        WHERE id = $1 AND status = 'paid'
        ON CONFLICT (order_id) DO NOTHING
        "#,
        id,
        Uuid::new_v4(),
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

pub async fn finish_rejected(pool: &PgPool, id: Uuid, attempts: i32) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        UPDATE orders
        SET
            status = 'rejected',
            stage = 'done',
            attempts = $2,
            next_attempt_at = now(),
            last_error = NULL,
            updated_at = now()
        WHERE id = $1
        "#,
        id,
        attempts,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn claim_due_outbox(
    pool: &PgPool,
    batch: i64,
    lease_until: DateTime<Utc>,
) -> sqlx::Result<Vec<ClaimedOutboxEvent>> {
    sqlx::query_as!(
        ClaimedOutboxEvent,
        r#"
        WITH due AS (
            SELECT event_id
            FROM order_paid_outbox
            WHERE published_at IS NULL AND next_attempt_at <= now()
            ORDER BY next_attempt_at, created_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE order_paid_outbox e
        SET
            next_attempt_at = $2,
            updated_at = now()
        FROM due
        WHERE e.event_id = due.event_id
        RETURNING e.event_id, e.order_id, e.occurred_at, e.attempts
        "#,
        batch,
        lease_until,
    )
    .fetch_all(pool)
    .await
}

pub async fn mark_outbox_published(pool: &PgPool, event_id: Uuid) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        UPDATE order_paid_outbox
        SET
            published_at = now(),
            last_error = NULL,
            updated_at = now()
        WHERE event_id = $1 AND published_at IS NULL
        "#,
        event_id,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn reschedule_outbox(
    pool: &PgPool,
    event_id: Uuid,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: &str,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        UPDATE order_paid_outbox
        SET
            attempts = $2,
            next_attempt_at = $3,
            last_error = $4,
            updated_at = now()
        WHERE event_id = $1 AND published_at IS NULL
        "#,
        event_id,
        attempts,
        next_attempt_at,
        last_error,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn apply_delivery_bonus(
    pool: &PgPool,
    event_id: &str,
    user_id: &str,
    bonus_points: i64,
) -> sqlx::Result<DeliveryApplyResult> {
    let mut transaction = pool.begin().await?;
    let inserted = sqlx::query!(
        r#"
        INSERT INTO processed_delivery_events (
            event_id,
            user_id,
            bonus_points
        )
        VALUES (
            $1,
            $2,
            $3
        )
        ON CONFLICT (event_id) DO NOTHING
        "#,
        event_id,
        user_id,
        bonus_points,
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected()
        == 1;

    if inserted {
        sqlx::query!(
            r#"
            INSERT INTO bonus_balances (
                user_id,
                points
            )
            VALUES (
                $1,
                $2
            )
            ON CONFLICT (user_id) DO UPDATE
            SET
                points = bonus_balances.points + EXCLUDED.points,
                updated_at = now()
            "#,
            user_id,
            bonus_points,
        )
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(if inserted {
        DeliveryApplyResult::Applied
    } else {
        DeliveryApplyResult::Duplicate
    })
}

pub async fn bonus_balance(pool: &PgPool, user_id: &str) -> sqlx::Result<i64> {
    let row = sqlx::query!(
        "SELECT points FROM bonus_balances WHERE user_id = $1",
        user_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| row.points).unwrap_or(0))
}
