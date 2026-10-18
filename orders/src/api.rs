use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, body::Bytes};
use garde::Validate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AppState;
use crate::store::{self, Item};

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order))
        .route("/webhooks/delivery", post(delivery_confirmed))
        .route("/users/{id}/bonus", get(get_bonus))
        .with_state(state)
}

#[derive(Deserialize, Validate)]
struct CreateOrderRequest {
    #[garde(length(min = 1))]
    user_id: String,
    #[garde(length(min = 1), dive)]
    items: Vec<Item>,
}

#[derive(Serialize)]
struct CreateOrderResponse {
    id: String,
    status: &'static str,
}

#[derive(Serialize)]
struct OrderResponse {
    id: String,
    status: String,
    amount_cents: i64,
}

#[derive(Serialize)]
struct BonusResponse {
    points: i64,
}

#[derive(Deserialize, Validate)]
struct DeliveryConfirmedRequest {
    #[garde(length(min = 1))]
    id: String,
    #[serde(rename = "type")]
    #[garde(skip)]
    event_type: String,
    #[garde(length(min = 1))]
    user_id: String,
    #[garde(range(min = 1))]
    bonus_points: i64,
}

fn is_delivery_confirmed(event: &DeliveryConfirmedRequest) -> bool {
    event.event_type == "delivery.confirmed"
}

async fn healthz(State(state): State<Arc<AppState>>) -> Response {
    match store::ping(&state.pool).await {
        Ok(()) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Err(err) => {
            tracing::warn!(error = %err, "healthz: database is not responding");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn create_order(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let request: CreateOrderRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return bad_request(),
    };
    if request.validate().is_err() {
        return bad_request();
    }
    let Some(amount_cents) = order_amount(&request.items) else {
        return bad_request();
    };

    let id = Uuid::new_v4();
    if let Err(err) = store::insert_order(
        &state.pool,
        id,
        &request.user_id,
        &request.items,
        amount_cents,
    )
    .await
    {
        tracing::error!(order_id = %id, error = %err, "failed to insert order");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal"})),
        )
            .into_response();
    }

    tracing::info!(order_id = %id, amount_cents, "order accepted");
    (
        StatusCode::CREATED,
        Json(CreateOrderResponse {
            id: id.to_string(),
            status: "pending",
        }),
    )
        .into_response()
}

async fn get_order(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let Ok(id) = Uuid::parse_str(&id) else {
        return not_found();
    };
    match store::order_status(&state.pool, id).await {
        Ok(Some((status, amount_cents))) => (
            StatusCode::OK,
            Json(OrderResponse {
                id: id.to_string(),
                status,
                amount_cents,
            }),
        )
            .into_response(),
        Ok(None) => not_found(),
        Err(err) => {
            tracing::error!(order_id = %id, error = %err, "failed to load order");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal"})),
            )
                .into_response()
        }
    }
}

async fn delivery_confirmed(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let event: DeliveryConfirmedRequest = match serde_json::from_slice(&body) {
        Ok(event) => event,
        Err(_) => return bad_request(),
    };
    if event.validate().is_err() || !is_delivery_confirmed(&event) {
        return bad_request();
    }

    match store::apply_delivery_bonus(&state.pool, &event.id, &event.user_id, event.bonus_points)
        .await
    {
        Ok(result) => {
            tracing::info!(
                event_id = %event.id,
                user_id = %event.user_id,
                duplicate = matches!(result, store::DeliveryApplyResult::Duplicate),
                "delivery event processed"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            tracing::error!(event_id = %event.id, error = %err, "failed to apply delivery bonus");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal"})),
            )
                .into_response()
        }
    }
}

async fn get_bonus(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match store::bonus_balance(&state.pool, &id).await {
        Ok(points) => (StatusCode::OK, Json(BonusResponse { points })).into_response(),
        Err(err) => {
            tracing::error!(user_id = %id, error = %err, "failed to load bonus balance");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal"})),
            )
                .into_response()
        }
    }
}

fn order_amount(items: &[Item]) -> Option<i64> {
    let total = items.iter().try_fold(0i64, |total, item| {
        total.checked_add(item.qty.checked_mul(item.price_cents)?)
    })?;
    (total >= 1).then_some(total)
}

fn bad_request() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "bad_request"})),
    )
        .into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(user_id: &str, items: Vec<Item>) -> CreateOrderRequest {
        CreateOrderRequest {
            user_id: user_id.to_string(),
            items,
        }
    }

    fn item(sku: &str, qty: i64, price_cents: i64) -> Item {
        Item {
            sku: sku.to_string(),
            qty,
            price_cents,
        }
    }

    #[test]
    fn valid_order_passes_validation() {
        let request = request("u-1", vec![item("sku-1", 2, 49900), item("sku-2", 1, 100)]);
        assert!(request.validate().is_ok());
        assert_eq!(order_amount(&request.items), Some(99900));
    }

    #[test]
    fn invalid_orders_are_rejected() {
        assert!(request("", vec![item("sku-1", 1, 100)]).validate().is_err());
        assert!(request("u-1", vec![]).validate().is_err());
        assert!(request("u-1", vec![item("", 1, 100)]).validate().is_err());
        assert!(
            request("u-1", vec![item("sku-1", 0, 100)])
                .validate()
                .is_err()
        );
        assert!(
            request("u-1", vec![item("sku-1", 1, -1)])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn zero_amount_and_overflow_are_rejected() {
        assert_eq!(order_amount(&[item("sku-1", 1, 0)]), None);
        assert_eq!(order_amount(&[item("sku-1", i64::MAX, 2)]), None);
    }

    fn delivery_event(
        id: &str,
        event_type: &str,
        user_id: &str,
        bonus_points: i64,
    ) -> DeliveryConfirmedRequest {
        DeliveryConfirmedRequest {
            id: id.to_string(),
            event_type: event_type.to_string(),
            user_id: user_id.to_string(),
            bonus_points,
        }
    }

    #[test]
    fn valid_delivery_event_passes_validation() {
        let event = delivery_event("evt-1", "delivery.confirmed", "u-1", 40);
        assert!(event.validate().is_ok());
        assert!(is_delivery_confirmed(&event));
    }

    #[test]
    fn invalid_delivery_events_are_rejected() {
        assert!(
            delivery_event("", "delivery.confirmed", "u-1", 40)
                .validate()
                .is_err()
        );
        assert!(
            delivery_event("evt-1", "delivery.confirmed", "", 40)
                .validate()
                .is_err()
        );
        assert!(
            delivery_event("evt-1", "delivery.confirmed", "u-1", 0)
                .validate()
                .is_err()
        );
        assert!(!is_delivery_confirmed(&delivery_event(
            "evt-1",
            "order.paid",
            "u-1",
            40,
        )));
    }
}
