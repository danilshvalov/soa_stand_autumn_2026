use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::Serialize;
use uuid::Uuid;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(2);

pub struct EventsClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("event bus request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("event bus responded with {0}")]
    UnexpectedStatus(StatusCode),
}

#[derive(Serialize)]
struct OrderPaidEvent {
    id: Uuid,
    #[serde(rename = "type")]
    event_type: &'static str,
    order_id: Uuid,
    occurred_at: DateTime<Utc>,
}

fn order_paid_event(event_id: Uuid, order_id: Uuid, occurred_at: DateTime<Utc>) -> OrderPaidEvent {
    OrderPaidEvent {
        id: event_id,
        event_type: "order.paid",
        order_id,
        occurred_at,
    }
}

impl EventsClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(PUBLISH_TIMEOUT)
            .build()
            .expect("reqwest client");
        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    pub async fn publish_order_paid(
        &self,
        event_id: Uuid,
        order_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<(), PublishError> {
        let response = self
            .http
            .post(format!("{}/events", self.base_url))
            .json(&order_paid_event(event_id, order_id, occurred_at))
            .send()
            .await?;
        if response.status() == StatusCode::ACCEPTED {
            Ok(())
        } else {
            Err(PublishError::UnexpectedStatus(response.status()))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    use super::*;

    async fn start_bus(status: StatusCode) -> String {
        let app = Router::new().route("/events", post(move || async move { status }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener");
        let address = listener.local_addr().expect("listener address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test event bus");
        });
        format!("http://{address}")
    }

    #[test]
    fn order_paid_payload_matches_contract() {
        let event_id = Uuid::parse_str("f98523da-150f-42f4-8613-62c132154768").unwrap();
        let order_id = Uuid::parse_str("112b1f18-b1c9-49c1-b1b9-6d728f93bc9e").unwrap();
        let occurred_at = Utc.with_ymd_and_hms(2026, 10, 18, 12, 30, 0).unwrap();

        let payload = order_paid_event(event_id, order_id, occurred_at);
        let json = serde_json::to_value(payload).unwrap();

        assert_eq!(json["id"], event_id.to_string());
        assert_eq!(json["type"], "order.paid");
        assert_eq!(json["order_id"], order_id.to_string());
        assert_eq!(json["occurred_at"], "2026-10-18T12:30:00Z");
    }

    #[tokio::test]
    async fn accepted_response_is_success() {
        let client = EventsClient::new(start_bus(StatusCode::ACCEPTED).await);
        let result = client
            .publish_order_paid(Uuid::new_v4(), Uuid::new_v4(), Utc::now())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn non_accepted_response_is_an_error() {
        let client = EventsClient::new(start_bus(StatusCode::SERVICE_UNAVAILABLE).await);
        let result = client
            .publish_order_paid(Uuid::new_v4(), Uuid::new_v4(), Utc::now())
            .await;
        assert!(matches!(
            result,
            Err(PublishError::UnexpectedStatus(
                StatusCode::SERVICE_UNAVAILABLE
            ))
        ));
    }
}
