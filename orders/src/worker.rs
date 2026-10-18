use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::AppState;
use crate::payment::{POST_TIMEOUT_BASE_MS, POST_TIMEOUT_MAX_MS, PayOutcome, VerifyOutcome};
use crate::store::{self, ClaimedOrder, PendingUpdate};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const CLAIM_BATCH: i64 = 16;
const LEASE_SECONDS: i64 = 30;
const BACKOFF_BASE_MS: i64 = 200;
const BACKOFF_MAX_MS: i64 = 2_000;
const MIN_DELAY_MS: i64 = 50;
const VERIFY_DELAY_MS: i64 = 100;

pub async fn run(state: Arc<AppState>) {
    loop {
        let lease_until = Utc::now() + ChronoDuration::seconds(LEASE_SECONDS);
        match store::claim_due(&state.pool, CLAIM_BATCH, lease_until).await {
            Ok(orders) => {
                for order in orders {
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(err) = step(&state, order).await {
                            tracing::warn!(error = %err, "processing step failed");
                        }
                    });
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to claim due orders");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn step(state: &AppState, order: ClaimedOrder) -> anyhow::Result<()> {
    match order.stage.as_str() {
        "reserve" => reserve_step(state, &order).await,
        "charge" => charge_step(state, &order).await,
        "verify" => verify_step(state, &order).await,
        "release" => release_step(state, &order).await,
        stage => {
            tracing::warn!(order_id = %order.id, stage, "unknown order stage");
            Ok(())
        }
    }
}

async fn reserve_step(state: &AppState, order: &ClaimedOrder) -> anyhow::Result<()> {
    match state.inventory.reserve(order.id, &order.items.0).await {
        Ok(()) => {
            save_pending(
                state,
                order,
                "charge",
                until(delay_ms(VERIFY_DELAY_MS)),
                order.attempts + 1,
                order.post_timeout_ms,
                None,
            )
            .await
        }
        Err(err) => {
            let reason = err.to_string();
            save_pending(
                state,
                order,
                "reserve",
                until(backoff(order.attempts)),
                order.attempts + 1,
                order.post_timeout_ms,
                Some(&reason),
            )
            .await
        }
    }
}

async fn charge_step(state: &AppState, order: &ClaimedOrder) -> anyhow::Result<()> {
    let timeout = Duration::from_millis(order.post_timeout_ms.max(1) as u64);
    let outcome = state
        .payment
        .charge(order.id, order.amount_cents, &order.currency, timeout)
        .await;
    match outcome {
        PayOutcome::Captured => {
            store::finish_paid(&state.pool, order.id, order.attempts + 1).await?;
            tracing::info!(order_id = %order.id, "order paid");
            Ok(())
        }
        PayOutcome::Declined => {
            tracing::info!(order_id = %order.id, "bank declined the payment, releasing reservation");
            match state.inventory.release(order.id).await {
                Ok(()) => {
                    store::finish_rejected(&state.pool, order.id, order.attempts + 1).await?;
                    Ok(())
                }
                Err(err) => {
                    let reason = err.to_string();
                    save_pending(
                        state,
                        order,
                        "release",
                        until(backoff(order.attempts)),
                        order.attempts + 1,
                        order.post_timeout_ms,
                        Some(&reason),
                    )
                    .await
                }
            }
        }
        PayOutcome::Retryable(reason) => {
            save_pending(
                state,
                order,
                "charge",
                until(backoff(order.attempts)),
                order.attempts + 1,
                POST_TIMEOUT_BASE_MS,
                Some(&reason),
            )
            .await
        }
        PayOutcome::Unknown { timeout } => {
            let post_timeout_ms = if timeout {
                timeout_after_timeout(order.post_timeout_ms)
            } else {
                order.post_timeout_ms
            };
            save_pending(
                state,
                order,
                "verify",
                until(delay_ms(VERIFY_DELAY_MS)),
                order.attempts + 1,
                post_timeout_ms,
                Some("payment outcome is unknown"),
            )
            .await
        }
        PayOutcome::Deferred(delay) => {
            save_pending(
                state,
                order,
                "charge",
                until(delay_from_std(delay)),
                order.attempts,
                order.post_timeout_ms,
                None,
            )
            .await
        }
    }
}

async fn verify_step(state: &AppState, order: &ClaimedOrder) -> anyhow::Result<()> {
    match state.payment.verify(order.id).await {
        VerifyOutcome::Captured => {
            store::finish_paid(&state.pool, order.id, order.attempts + 1).await?;
            tracing::info!(order_id = %order.id, "charge confirmed by lookup");
            Ok(())
        }
        VerifyOutcome::NoCharge => {
            save_pending(
                state,
                order,
                "charge",
                until(backoff(order.attempts)),
                order.attempts + 1,
                order.post_timeout_ms,
                None,
            )
            .await
        }
        VerifyOutcome::Retryable(reason) => {
            save_pending(
                state,
                order,
                "verify",
                until(backoff(order.attempts)),
                order.attempts + 1,
                POST_TIMEOUT_BASE_MS,
                Some(&reason),
            )
            .await
        }
        VerifyOutcome::Unknown => {
            save_pending(
                state,
                order,
                "verify",
                until(delay_ms(VERIFY_DELAY_MS)),
                order.attempts + 1,
                order.post_timeout_ms,
                Some("charge lookup failed"),
            )
            .await
        }
        VerifyOutcome::Deferred(delay) => {
            save_pending(
                state,
                order,
                "verify",
                until(delay_from_std(delay)),
                order.attempts,
                order.post_timeout_ms,
                None,
            )
            .await
        }
    }
}

async fn release_step(state: &AppState, order: &ClaimedOrder) -> anyhow::Result<()> {
    match state.inventory.release(order.id).await {
        Ok(()) => {
            store::finish_rejected(&state.pool, order.id, order.attempts + 1).await?;
            tracing::info!(order_id = %order.id, "reservation released, order rejected");
            Ok(())
        }
        Err(err) => {
            let reason = err.to_string();
            save_pending(
                state,
                order,
                "release",
                until(backoff(order.attempts)),
                order.attempts + 1,
                order.post_timeout_ms,
                Some(&reason),
            )
            .await
        }
    }
}

async fn save_pending(
    state: &AppState,
    order: &ClaimedOrder,
    stage: &str,
    next_attempt_at: DateTime<Utc>,
    attempts: i32,
    post_timeout_ms: i32,
    last_error: Option<&str>,
) -> anyhow::Result<()> {
    store::save_pending_attempt(
        &state.pool,
        order.id,
        PendingUpdate {
            stage,
            next_attempt_at,
            attempts,
            post_timeout_ms,
            last_error,
        },
    )
    .await?;
    Ok(())
}

fn until(delay: ChronoDuration) -> DateTime<Utc> {
    Utc::now() + delay
}

fn delay_ms(ms: i64) -> ChronoDuration {
    ChronoDuration::milliseconds(ms.max(MIN_DELAY_MS))
}

fn delay_from_std(delay: Duration) -> ChronoDuration {
    ChronoDuration::milliseconds((delay.as_millis() as i64).max(MIN_DELAY_MS))
}

fn backoff(attempts: i32) -> ChronoDuration {
    let exponent = attempts.clamp(0, 6) as u32;
    ChronoDuration::milliseconds((BACKOFF_BASE_MS << exponent).min(BACKOFF_MAX_MS))
}

fn timeout_after_timeout(current: i32) -> i32 {
    current.saturating_mul(2).min(POST_TIMEOUT_MAX_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0).num_milliseconds(), 200);
        assert_eq!(backoff(1).num_milliseconds(), 400);
        assert_eq!(backoff(3).num_milliseconds(), 1600);
        assert_eq!(backoff(5).num_milliseconds(), 2000);
        assert_eq!(backoff(50).num_milliseconds(), 2000);
    }

    #[test]
    fn payment_timeout_doubles_and_caps() {
        assert_eq!(timeout_after_timeout(2500), 5000);
        assert_eq!(timeout_after_timeout(8000), POST_TIMEOUT_MAX_MS);
        assert_eq!(
            timeout_after_timeout(POST_TIMEOUT_MAX_MS),
            POST_TIMEOUT_MAX_MS
        );
    }
}
