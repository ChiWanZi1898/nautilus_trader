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

//! Process-local activation of expected signed order identities before HTTP submit handoff.

use std::sync::Arc;

use nautilus_model::{
    identifiers::VenueOrderId,
    orders::{Order, OrderAny},
    types::Quantity,
};

use super::{
    identity::{OrderIdentity, OrderIdentityRegistry},
    order_fill_tracker::OrderFillTrackerMap,
    pending::PendingSubmitTracker,
};

/// Receipt for the exact process-local state inserted by one expected-submit activation.
///
/// This is deliberately not durable evidence. It is retained only long enough to roll back a
/// proven pre-handoff local failure without removing compatible state owned by an earlier attempt.
#[derive(Debug)]
pub(crate) struct ExpectedSubmitActivation {
    venue_order_id: VenueOrderId,
    identity: OrderIdentity,
    quantity: Quantity,
    inserted_identity: bool,
    inserted_pending_submit: bool,
    inserted_fill_state: bool,
    fill_tracker: Arc<OrderFillTrackerMap>,
    order_identities: Arc<OrderIdentityRegistry>,
    pending_submits: PendingSubmitTracker,
    closed: bool,
}

impl ExpectedSubmitActivation {
    /// Marks the boundary after which rollback is forbidden because submit outcome can be unknown.
    pub(crate) fn mark_http_handoff_started(&mut self) {
        self.closed = true;
    }

    /// Rolls back only state inserted by this activation.
    ///
    /// Returns `false` and retains identity state if authenticated activity has already advanced the
    /// order. Callers must never use this after HTTP handoff, where submit outcome is ambiguous.
    #[cfg(test)]
    pub(crate) fn rollback_never_sent(mut self) -> bool {
        self.rollback_pristine_state()
    }

    fn rollback_pristine_state(&mut self) -> bool {
        if self.closed {
            return true;
        }
        self.closed = true;
        if self.inserted_fill_state
            && !self.fill_tracker.deactivate_pristine_order(
                self.venue_order_id,
                self.quantity,
                self.identity.order_side,
            )
        {
            return false;
        }
        if self.inserted_pending_submit
            && !self
                .pending_submits
                .deactivate(self.venue_order_id, self.identity.client_order_id)
        {
            return false;
        }
        if self.inserted_identity
            && !self
                .order_identities
                .deactivate_unaccepted_identity(self.venue_order_id, self.identity)
        {
            return false;
        }
        true
    }
}

impl Drop for ExpectedSubmitActivation {
    fn drop(&mut self) {
        if !self.closed && !self.rollback_pristine_state() {
            log::error!(
                "Could not roll back pristine expected-submit activation before HTTP handoff"
            );
        }
    }
}

/// Idempotently activates the signed venue identity and fill state for one order.
pub(crate) fn activate_expected_submit(
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &Arc<OrderIdentityRegistry>,
    pending_submits: &PendingSubmitTracker,
) -> Result<ExpectedSubmitActivation, String> {
    let identity = OrderIdentity::from_order(order);
    let quantity = order.quantity();
    let inserted_identity =
        order_identities.activate_expected_identity(expected_venue_order_id, identity)?;

    let inserted_pending_submit = match pending_submits
        .activate(expected_venue_order_id, identity.client_order_id)
    {
        Ok(inserted) => inserted,
        Err(reason) => {
            if inserted_identity {
                order_identities.deactivate_unaccepted_identity(expected_venue_order_id, identity);
            }
            return Err(reason);
        }
    };

    let inserted_fill_state = match fill_tracker.activate_expected_order(
        expected_venue_order_id,
        quantity,
        identity.order_side,
    ) {
        Ok(inserted) => inserted,
        Err(reason) => {
            if inserted_pending_submit {
                pending_submits.deactivate(expected_venue_order_id, identity.client_order_id);
            }
            if inserted_identity {
                order_identities.deactivate_unaccepted_identity(expected_venue_order_id, identity);
            }
            return Err(reason);
        }
    };

    Ok(ExpectedSubmitActivation {
        venue_order_id: expected_venue_order_id,
        identity,
        quantity,
        inserted_identity,
        inserted_pending_submit,
        inserted_fill_state,
        fill_tracker: fill_tracker.clone(),
        order_identities: order_identities.clone(),
        pending_submits: pending_submits.clone(),
        closed: false,
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{OrderSide, TimeInForce},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        orders::LimitOrder,
        types::Price,
    };
    use rstest::rstest;

    use super::*;

    fn test_order(client_order_id: &str) -> OrderAny {
        OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            ClientOrderId::from(client_order_id),
            OrderSide::Buy,
            Quantity::from("5"),
            Price::from("0.50"),
            TimeInForce::Gtc,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        ))
    }

    fn trackers() -> (
        Arc<OrderFillTrackerMap>,
        Arc<OrderIdentityRegistry>,
        PendingSubmitTracker,
    ) {
        (
            Arc::new(OrderFillTrackerMap::new()),
            Arc::new(OrderIdentityRegistry::default()),
            PendingSubmitTracker::default(),
        )
    }

    #[rstest]
    fn test_pristine_activation_can_be_rolled_back_before_handoff() {
        let order = test_order("O-ROLLBACK");
        let venue_order_id = VenueOrderId::from("V-ROLLBACK");
        let (fill_tracker, order_identities, pending_submits) = trackers();
        let activation = activate_expected_submit(
            &order,
            venue_order_id,
            &fill_tracker,
            &order_identities,
            &pending_submits,
        )
        .unwrap();

        assert!(activation.rollback_never_sent());
        assert!(!fill_tracker.contains(&venue_order_id));
        assert!(order_identities.get(&venue_order_id).is_none());
        assert!(pending_submits.client_order_id(&venue_order_id).is_none());
    }

    #[rstest]
    fn test_dropped_pre_handoff_activation_rolls_back_pristine_state() {
        let order = test_order("O-DROPPED");
        let venue_order_id = VenueOrderId::from("V-DROPPED");
        let (fill_tracker, order_identities, pending_submits) = trackers();
        {
            let _activation = activate_expected_submit(
                &order,
                venue_order_id,
                &fill_tracker,
                &order_identities,
                &pending_submits,
            )
            .unwrap();
        }

        assert!(!fill_tracker.contains(&venue_order_id));
        assert!(order_identities.get(&venue_order_id).is_none());
        assert!(pending_submits.client_order_id(&venue_order_id).is_none());
    }

    #[rstest]
    fn test_activation_with_authenticated_activity_cannot_be_rolled_back() {
        let order = test_order("O-ACTIVE");
        let venue_order_id = VenueOrderId::from("V-ACTIVE");
        let (fill_tracker, order_identities, pending_submits) = trackers();
        let activation = activate_expected_submit(
            &order,
            venue_order_id,
            &fill_tracker,
            &order_identities,
            &pending_submits,
        )
        .unwrap();
        fill_tracker.record_fill(&venue_order_id, Quantity::from("1"));
        order_identities.mark_accepted(venue_order_id);

        assert!(!activation.rollback_never_sent());
        assert!(fill_tracker.contains(&venue_order_id));
        assert!(order_identities.get(&venue_order_id).is_some());
        assert_eq!(
            pending_submits.client_order_id(&venue_order_id),
            Some(order.client_order_id())
        );
    }

    #[rstest]
    fn test_prepare_all_collision_rolls_back_earlier_pristine_leg() {
        let first = test_order("O-BATCH-1");
        let second = test_order("O-BATCH-2");
        let venue_order_id = VenueOrderId::from("V-DUPLICATE");
        let (fill_tracker, order_identities, pending_submits) = trackers();
        let first_activation = activate_expected_submit(
            &first,
            venue_order_id,
            &fill_tracker,
            &order_identities,
            &pending_submits,
        )
        .unwrap();

        assert!(
            activate_expected_submit(
                &second,
                venue_order_id,
                &fill_tracker,
                &order_identities,
                &pending_submits,
            )
            .is_err()
        );
        assert!(first_activation.rollback_never_sent());
        assert!(!fill_tracker.contains(&venue_order_id));
        assert!(order_identities.get(&venue_order_id).is_none());
    }
}
