use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::task::JoinSet;

use crate::AppState;
use crate::store::{self, ClaimedOutboxEvent};

const BACKOFF_BASE_MS: i64 = 200;
const BACKOFF_MAX_MS: i64 = 2_000;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const ERROR_INTERVAL: Duration = Duration::from_millis(500);
const CLAIM_BATCH: i64 = 16;
const LEASE_SECONDS: i64 = 5;

pub async fn run(state: Arc<AppState>) {
    loop {
        let lease_until = Utc::now() + ChronoDuration::seconds(LEASE_SECONDS);
        match store::claim_due_outbox(&state.pool, CLAIM_BATCH, lease_until).await {
            Ok(events) => publish_batch(state.clone(), events).await,
            Err(err) => {
                tracing::warn!(error = %err, "failed to claim outbox events");
                tokio::time::sleep(ERROR_INTERVAL).await;
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn publish_batch(state: Arc<AppState>, events: Vec<ClaimedOutboxEvent>) {
    let mut tasks = JoinSet::new();
    for event in events {
        let state = state.clone();
        tasks.spawn(async move { publish_one(&state, event).await });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(err) = result {
            tracing::warn!(error = %err, "outbox publisher task failed");
        }
    }
}

async fn publish_one(state: &AppState, event: ClaimedOutboxEvent) {
    match state
        .events
        .publish_order_paid(event.event_id, event.order_id, event.occurred_at)
        .await
    {
        Ok(()) => {
            if let Err(err) = store::mark_outbox_published(&state.pool, event.event_id).await {
                tracing::warn!(
                    event_id = %event.event_id,
                    order_id = %event.order_id,
                    error = %err,
                    "event accepted but outbox acknowledgement failed"
                );
            } else {
                tracing::info!(
                    event_id = %event.event_id,
                    order_id = %event.order_id,
                    "order.paid published"
                );
            }
        }
        Err(err) => {
            let reason = err.to_string();
            let attempts = event.attempts.saturating_add(1);
            let next_attempt_at = Utc::now() + retry_backoff(event.attempts);
            if let Err(store_err) = store::reschedule_outbox(
                &state.pool,
                event.event_id,
                attempts,
                next_attempt_at,
                &reason,
            )
            .await
            {
                tracing::warn!(
                    event_id = %event.event_id,
                    order_id = %event.order_id,
                    error = %store_err,
                    "failed to reschedule outbox event"
                );
            }
        }
    }
}

fn retry_backoff(attempts: i32) -> ChronoDuration {
    let exponent = attempts.clamp(0, 6) as u32;
    ChronoDuration::milliseconds((BACKOFF_BASE_MS << exponent).min(BACKOFF_MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_grows_and_caps() {
        assert_eq!(retry_backoff(0).num_milliseconds(), 200);
        assert_eq!(retry_backoff(1).num_milliseconds(), 400);
        assert_eq!(retry_backoff(3).num_milliseconds(), 1600);
        assert_eq!(retry_backoff(5).num_milliseconds(), 2000);
        assert_eq!(retry_backoff(50).num_milliseconds(), 2000);
    }
}
