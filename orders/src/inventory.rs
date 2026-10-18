use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::store::Item;

const INVENTORY_TIMEOUT: Duration = Duration::from_secs(2);

pub struct InventoryClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("inventory request failed: {0}")]
    Request(String),
}

impl InventoryClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .build()
            .expect("reqwest client");
        Self { http, base_url }
    }

    pub async fn reserve(&self, order_id: Uuid, items: &[Item]) -> Result<(), InventoryError> {
        let reservation_items: Vec<_> = items
            .iter()
            .map(|item| json!({"sku": item.sku, "qty": item.qty}))
            .collect();
        let response = self
            .http
            .post(format!("{}/reservations", self.base_url))
            .timeout(INVENTORY_TIMEOUT)
            .json(&json!({"order_id": order_id.to_string(), "items": reservation_items}))
            .send()
            .await
            .map_err(|err| InventoryError::Request(err.to_string()))?;
        match response.status().as_u16() {
            200 | 201 => Ok(()),
            code => Err(InventoryError::Request(format!(
                "reserve responded with {code}"
            ))),
        }
    }

    pub async fn release(&self, order_id: Uuid) -> Result<(), InventoryError> {
        let response = self
            .http
            .delete(format!("{}/reservations/{order_id}", self.base_url))
            .timeout(INVENTORY_TIMEOUT)
            .send()
            .await
            .map_err(|err| InventoryError::Request(err.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(InventoryError::Request(format!(
                "release responded with {}",
                response.status()
            )))
        }
    }
}
