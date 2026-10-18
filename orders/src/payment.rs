use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub const POST_TIMEOUT_BASE_MS: i32 = 2_500;
pub const POST_TIMEOUT_MAX_MS: i32 = 10_000;

const GATE_MAX_WAIT: Duration = Duration::from_secs(5);
const GET_TIMEOUT: Duration = Duration::from_millis(1_500);
const DOWN_COOLDOWN: Duration = Duration::from_secs(8);
const NORMAL_SLOTS: usize = 4;

#[derive(Debug)]
pub enum PayOutcome {
    Captured,
    Declined,
    Retryable(String),
    Unknown { timeout: bool },
    Deferred(Duration),
}

#[derive(Debug)]
pub enum VerifyOutcome {
    Captured,
    NoCharge,
    Retryable(String),
    Unknown,
    Deferred(Duration),
}

pub struct PaymentClient {
    http: reqwest::Client,
    base_url: String,
    gate: Arc<PaymentGate>,
}

impl PaymentClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .build()
            .expect("reqwest client");
        Self {
            http,
            base_url,
            gate: Arc::new(PaymentGate::new()),
        }
    }

    pub async fn charge(
        &self,
        order_id: Uuid,
        amount_cents: i64,
        currency: &str,
        timeout: Duration,
    ) -> PayOutcome {
        let _permit = match self.acquire(CallKind::Write).await {
            Acquire::Granted(permit) => permit,
            Acquire::Later(delay) => return PayOutcome::Deferred(delay),
        };
        let request = self
            .http
            .post(format!("{}/payments", self.base_url))
            .header("Idempotency-Key", order_id.to_string())
            .timeout(timeout)
            .json(&serde_json::json!({
                "order_id": order_id.to_string(),
                "amount_cents": amount_cents,
                "currency": currency,
            }));
        match request.send().await {
            Ok(response) => {
                self.gate.report_http_response();
                match response.status().as_u16() {
                    200 | 201 => PayOutcome::Captured,
                    402 => PayOutcome::Declined,
                    code => PayOutcome::Retryable(format!("payment responded with {code}")),
                }
            }
            Err(err) => {
                self.gate.report_transport_error(CallKind::Write);
                PayOutcome::Unknown {
                    timeout: err.is_timeout(),
                }
            }
        }
    }

    pub async fn verify(&self, order_id: Uuid) -> VerifyOutcome {
        let _permit = match self.acquire(CallKind::Read).await {
            Acquire::Granted(permit) => permit,
            Acquire::Later(delay) => return VerifyOutcome::Deferred(delay),
        };
        let request = self
            .http
            .get(format!("{}/payments", self.base_url))
            .query(&[("order_id", order_id.to_string())])
            .timeout(GET_TIMEOUT);
        match request.send().await {
            Ok(response) => {
                let status = response.status();
                self.gate.report_http_response();
                if status != reqwest::StatusCode::OK {
                    return VerifyOutcome::Retryable(format!(
                        "GET /payments responded with {status}"
                    ));
                }
                match response.json::<Vec<Value>>().await {
                    Ok(charges) if !charges.is_empty() => VerifyOutcome::Captured,
                    Ok(_) => VerifyOutcome::NoCharge,
                    Err(err) => VerifyOutcome::Retryable(format!("GET /payments: {err}")),
                }
            }
            Err(_) => {
                self.gate.report_transport_error(CallKind::Read);
                VerifyOutcome::Unknown
            }
        }
    }

    async fn acquire(&self, kind: CallKind) -> Acquire {
        let deadline = Instant::now() + GATE_MAX_WAIT;
        loop {
            match self.gate.try_acquire(kind) {
                Acquire::Granted(permit) => return Acquire::Granted(permit),
                Acquire::Later(delay) => {
                    if Instant::now() + delay >= deadline {
                        return Acquire::Later(delay);
                    }
                    tokio::time::sleep(delay.min(Duration::from_millis(100))).await;
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CallKind {
    Read,
    Write,
}

#[derive(Debug)]
enum Acquire {
    Granted(Permit),
    Later(Duration),
}

#[derive(Debug)]
enum Permit {
    Normal(#[allow(dead_code)] OwnedSemaphorePermit),
    Probe,
}

#[derive(Default)]
struct GateState {
    suspect: bool,
    down_until: Option<Instant>,
    probe_in_flight: bool,
}

pub struct PaymentGate {
    state: Mutex<GateState>,
    slots: Arc<Semaphore>,
    cooldown: Duration,
}

impl PaymentGate {
    pub fn new() -> Self {
        Self::with_cooldown(DOWN_COOLDOWN)
    }

    fn with_cooldown(cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(GateState::default()),
            slots: Arc::new(Semaphore::new(NORMAL_SLOTS)),
            cooldown,
        }
    }

    fn try_acquire(&self, kind: CallKind) -> Acquire {
        let mut state = self.state.lock().expect("gate lock");
        let now = Instant::now();
        if let Some(until) = state.down_until {
            if now < until {
                return Acquire::Later(until.duration_since(now));
            }
            state.down_until = None;
            state.suspect = true;
        }
        if state.suspect {
            if kind == CallKind::Write || state.probe_in_flight {
                return Acquire::Later(Duration::from_millis(100));
            }
            state.probe_in_flight = true;
            return Acquire::Granted(Permit::Probe);
        }
        drop(state);
        match self.slots.clone().try_acquire_owned() {
            Ok(permit) => Acquire::Granted(Permit::Normal(permit)),
            Err(_) => Acquire::Later(Duration::from_millis(30)),
        }
    }

    fn report_http_response(&self) {
        let mut state = self.state.lock().expect("gate lock");
        state.suspect = false;
        state.down_until = None;
        state.probe_in_flight = false;
    }

    fn report_transport_error(&self, kind: CallKind) {
        let mut state = self.state.lock().expect("gate lock");
        state.probe_in_flight = false;
        match kind {
            CallKind::Read => {
                state.suspect = false;
                state.down_until = Some(Instant::now() + self.cooldown);
            }
            CallKind::Write => {
                if state.down_until.is_none() {
                    state.suspect = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_failure_defers_writes_and_lets_read_probe() {
        let gate = PaymentGate::new();
        assert!(matches!(
            gate.try_acquire(CallKind::Write),
            Acquire::Granted(Permit::Normal(_))
        ));
        gate.report_transport_error(CallKind::Write);
        assert!(matches!(
            gate.try_acquire(CallKind::Write),
            Acquire::Later(_)
        ));
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Granted(Permit::Probe)
        ));
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Later(_)
        ));
        gate.report_http_response();
        assert!(matches!(
            gate.try_acquire(CallKind::Write),
            Acquire::Granted(Permit::Normal(_))
        ));
    }

    #[test]
    fn read_failure_closes_gate_until_cooldown() {
        let gate = PaymentGate::with_cooldown(Duration::from_millis(20));
        gate.report_transport_error(CallKind::Read);
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Later(_)
        ));
        std::thread::sleep(Duration::from_millis(30));
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Granted(Permit::Probe)
        ));
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Later(_)
        ));
        gate.report_transport_error(CallKind::Read);
        assert!(matches!(
            gate.try_acquire(CallKind::Read),
            Acquire::Later(_)
        ));
    }

    #[test]
    fn normal_mode_allows_four_parallel_calls() {
        let gate = PaymentGate::new();
        let mut permits = Vec::new();
        for _ in 0..NORMAL_SLOTS {
            match gate.try_acquire(CallKind::Write) {
                Acquire::Granted(permit) => permits.push(permit),
                other => panic!("expected a permit, got {other:?}"),
            }
        }
        assert!(matches!(
            gate.try_acquire(CallKind::Write),
            Acquire::Later(_)
        ));
        drop(permits);
        assert!(matches!(
            gate.try_acquire(CallKind::Write),
            Acquire::Granted(Permit::Normal(_))
        ));
    }
}
