// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Polymarket-specific custom data types.
//!
//! These types carry Polymarket domain data through the Nautilus data engine as
//! [`CustomData`](nautilus_model::data::CustomData).

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{HasTsInit, custom::CustomDataTrait},
    identifiers::InstrumentId,
    types::Price,
};
use nautilus_persistence_macros::custom_data;
use serde::{Deserialize, Serialize};

/// Type name published for [`PolymarketFrameCommit`] custom data.
pub const POLYMARKET_FRAME_COMMIT_TYPE_NAME: &str = "PolymarketFrameCommit";

/// Immutable evidence that one Polymarket market-data WebSocket frame was fully accepted.
///
/// The adapter publishes this after every L2 delta batch derived from the frame has entered the
/// same data-event FIFO. A strategy can mark instruments dirty on delta callbacks and use this
/// value as the frame evaluation boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketFrameCommit {
    frame_id: u64,
    affected_instrument_ids: Vec<InstrumentId>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

impl PolymarketFrameCommit {
    pub(crate) fn new(
        frame_id: u64,
        affected_instrument_ids: Vec<InstrumentId>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        debug_assert!(frame_id > 0);
        debug_assert!(!affected_instrument_ids.is_empty());
        debug_assert!(affected_instrument_ids.is_sorted());
        debug_assert!(
            affected_instrument_ids
                .windows(2)
                .all(|ids| ids[0] != ids[1])
        );
        Self {
            frame_id,
            affected_instrument_ids,
            ts_event,
            ts_init,
        }
    }

    /// Returns the monotonic adapter-local frame identifier.
    #[must_use]
    pub const fn frame_id(&self) -> u64 {
        self.frame_id
    }

    /// Returns the canonical sorted instrument IDs affected by this frame.
    #[must_use]
    pub fn affected_instrument_ids(&self) -> &[InstrumentId] {
        &self.affected_instrument_ids
    }

    /// Returns the venue event timestamp for the committed frame.
    #[must_use]
    pub const fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }
}

impl HasTsInit for PolymarketFrameCommit {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for PolymarketFrameCommit {
    fn type_name(&self) -> &'static str {
        POLYMARKET_FRAME_COMMIT_TYPE_NAME
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn type_name_static() -> &'static str {
        POLYMARKET_FRAME_COMMIT_TYPE_NAME
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let commit = serde_json::from_value::<Self>(value)?;
        anyhow::ensure!(commit.frame_id > 0, "frame_id must be non-zero");
        anyhow::ensure!(
            !commit.affected_instrument_ids.is_empty(),
            "affected_instrument_ids must not be empty",
        );
        anyhow::ensure!(
            commit.affected_instrument_ids.is_sorted()
                && commit
                    .affected_instrument_ids
                    .windows(2)
                    .all(|ids| ids[0] != ids[1]),
            "affected_instrument_ids must be sorted and unique",
        );
        Ok(Arc::new(commit))
    }
}

/// Polymarket RTDS crypto price sample from the `crypto_prices` topic.
///
/// The adapter normalizes both live `update` frames and `subscribe` backfill
/// snapshots into this per-tick custom data type.
#[custom_data(pyo3, no_arrow, stub_module = "nautilus_trader.adapters.polymarket")]
pub struct PolymarketRtdsCryptoPrice {
    /// Lowercase venue symbol, e.g. `btcusdt`.
    pub symbol: String,
    /// Current spot price.
    #[custom_data_field(serde)]
    pub value: Price,
    /// Price measurement timestamp in Unix milliseconds.
    pub price_timestamp_ms: u64,
    /// RTDS envelope timestamp in Unix milliseconds.
    pub message_timestamp_ms: u64,
    /// UNIX timestamp (nanoseconds) when the price event occurred.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was initialized.
    pub ts_init: UnixNanos,
}

/// Polymarket RTDS equity price sample from the `equity_prices` topic.
///
/// The adapter normalizes both live `update` frames and `subscribe` backfill
/// snapshots into this per-tick custom data type.
#[custom_data(pyo3, no_arrow, stub_module = "nautilus_trader.adapters.polymarket")]
pub struct PolymarketRtdsEquityPrice {
    /// Lowercase venue symbol, e.g. `aapl`, `eurusd`, or `xauusd`.
    pub symbol: String,
    /// Spot price rounded to the venue's float payload precision.
    #[custom_data_field(serde)]
    pub value: Price,
    /// Full-precision spot price when supplied, otherwise the venue's `value`.
    #[custom_data_field(serde)]
    pub full_accuracy_value: Price,
    /// Price measurement timestamp in Unix milliseconds.
    pub price_timestamp_ms: u64,
    /// RTDS envelope timestamp in Unix milliseconds.
    pub message_timestamp_ms: u64,
    /// System receipt timestamp in Unix milliseconds when present.
    #[custom_data_field(serde)]
    pub received_at_ms: Option<u64>,
    /// `true` when the venue is carrying forward the last known value.
    pub is_carried_forward: bool,
    /// UNIX timestamp (nanoseconds) when the price event occurred.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was initialized.
    pub ts_init: UnixNanos,
}

/// Registers Polymarket custom data types.
///
/// Safe to call multiple times (idempotent via internal `Once` guards).
pub fn register_polymarket_custom_data() {
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketFrameCommit>();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketRtdsCryptoPrice>();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketRtdsEquityPrice>();
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::register_polymarket_custom_data;

    #[rstest]
    fn test_register_polymarket_custom_data_is_idempotent() {
        register_polymarket_custom_data();
        register_polymarket_custom_data();
    }
}
