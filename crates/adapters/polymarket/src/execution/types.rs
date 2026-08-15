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

//! Shared types for the Polymarket execution module.

use aws_lc_rs::digest;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    identifiers::{ClientOrderId, VenueOrderId},
    orders::OrderAny,
    types::{Price, Quantity},
};

use crate::{
    common::{
        consts::{BATCH_ORDER_LIMIT, CANCEL_ALREADY_DONE},
        enums::PolymarketOrderType,
    },
    http::models::PolymarketOrder,
};

/// Classifies cancel rejection reasons to eliminate duplicate if/else blocks.
pub(crate) enum CancelOutcome {
    AlreadyDone,
    Rejected(String),
}

impl CancelOutcome {
    pub(crate) fn classify(reason: &str) -> Self {
        if reason.contains(CANCEL_ALREADY_DONE) {
            Self::AlreadyDone
        } else {
            Self::Rejected(reason.to_string())
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LimitOrderSubmitRequest {
    pub(crate) token_id: String,
    pub(crate) side: OrderSide,
    pub(crate) price: Price,
    pub(crate) quantity: Quantity,
    pub(crate) time_in_force: TimeInForce,
    pub(crate) post_only: bool,
    pub(crate) neg_risk: bool,
    pub(crate) expire_time: Option<UnixNanos>,
    pub(crate) tick_decimals: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct SignedLimitOrderSubmission {
    pub(crate) order: PolymarketOrder,
    pub(crate) order_type: PolymarketOrderType,
    pub(crate) post_only: bool,
    pub(crate) expected_venue_order_id: VenueOrderId,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LimitHttpRequestEndpoint {
    Single,
    Batch,
}

/// One fully prepared LIMIT-order HTTP request.
///
/// The exact body contains the CLOB owner API-key identifier. This type deliberately implements
/// neither `Debug`, `Display`, nor `Serialize`; callers must never log, expose, or persist the body.
/// A future durable bridge may retain only credential-free signed fields and `body_sha256`.
pub(crate) struct PreparedLimitHttpRequest {
    endpoint: LimitHttpRequestEndpoint,
    body_bytes: Vec<u8>,
    body_sha256: [u8; 32],
    expected_venue_order_ids: Vec<VenueOrderId>,
    client_order_ids: Vec<ClientOrderId>,
}

impl PreparedLimitHttpRequest {
    pub(crate) fn new(
        endpoint: LimitHttpRequestEndpoint,
        body_bytes: Vec<u8>,
        expected_venue_order_ids: Vec<VenueOrderId>,
        client_order_ids: Vec<ClientOrderId>,
    ) -> Result<Self, String> {
        let leg_count = expected_venue_order_ids.len();
        if leg_count == 0 || leg_count != client_order_ids.len() {
            return Err(format!(
                "prepared LIMIT request identity cardinality mismatch: venue={leg_count}, client={}",
                client_order_ids.len()
            ));
        }
        match endpoint {
            LimitHttpRequestEndpoint::Single if leg_count != 1 => {
                return Err(format!(
                    "prepared single LIMIT request requires exactly one identity, found {leg_count}"
                ));
            }
            LimitHttpRequestEndpoint::Batch if !(2..=BATCH_ORDER_LIMIT).contains(&leg_count) => {
                return Err(format!(
                    "prepared batch LIMIT request requires 2..={BATCH_ORDER_LIMIT} identities, found {leg_count}"
                ));
            }
            _ => {}
        }

        let body_digest = digest::digest(&digest::SHA256, &body_bytes);
        let mut body_sha256 = [0_u8; 32];
        body_sha256.copy_from_slice(body_digest.as_ref());

        Ok(Self {
            endpoint,
            body_bytes,
            body_sha256,
            expected_venue_order_ids,
            client_order_ids,
        })
    }

    pub(crate) const fn endpoint(&self) -> LimitHttpRequestEndpoint {
        self.endpoint
    }

    pub(crate) fn body_bytes(&self) -> &[u8] {
        &self.body_bytes
    }

    pub(crate) const fn body_sha256(&self) -> [u8; 32] {
        self.body_sha256
    }

    pub(crate) fn body_hash_matches(&self) -> bool {
        digest::digest(&digest::SHA256, &self.body_bytes).as_ref() == self.body_sha256()
    }

    pub(crate) fn expected_venue_order_ids(&self) -> &[VenueOrderId] {
        &self.expected_venue_order_ids
    }

    pub(crate) fn client_order_ids(&self) -> &[ClientOrderId] {
        &self.client_order_ids
    }

    pub(crate) fn into_body_bytes(self) -> Vec<u8> {
        self.body_bytes
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BatchLimitOrderContext {
    pub(crate) order: OrderAny,
    pub(crate) request: LimitOrderSubmitRequest,
    pub(crate) size_precision: u8,
    pub(crate) price_precision: u8,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_prepared_limit_http_request_retains_exact_bytes_and_stable_hash() {
        let body = br#"{"owner":"sensitive-owner","order":{"signature":"signed"}}"#.to_vec();
        let first = PreparedLimitHttpRequest::new(
            LimitHttpRequestEndpoint::Single,
            body.clone(),
            vec![VenueOrderId::from("0xexpected")],
            vec![ClientOrderId::from("O-1")],
        )
        .expect("valid prepared request");
        let second = PreparedLimitHttpRequest::new(
            LimitHttpRequestEndpoint::Single,
            body.clone(),
            vec![VenueOrderId::from("0xexpected")],
            vec![ClientOrderId::from("O-1")],
        )
        .expect("valid prepared request");

        assert_eq!(first.body_bytes(), body);
        assert_eq!(first.body_sha256(), second.body_sha256());
        assert_eq!(
            first.body_sha256(),
            [
                0x6d, 0x97, 0x78, 0x4e, 0x9e, 0xe1, 0xcb, 0x74, 0xf9, 0x38, 0xa1, 0xfe, 0xac, 0xdc,
                0xf0, 0xd6, 0x31, 0xe2, 0x7d, 0x1c, 0xb5, 0xdd, 0xac, 0xaf, 0x2c, 0x02, 0xc0, 0xdf,
                0x5a, 0x1d, 0xbd, 0xda,
            ]
        );
        assert!(first.body_hash_matches());
    }

    #[rstest]
    fn test_prepared_batch_preserves_identity_order() {
        let venue_ids = vec![
            VenueOrderId::from("0xfirst"),
            VenueOrderId::from("0xsecond"),
        ];
        let client_ids = vec![ClientOrderId::from("O-1"), ClientOrderId::from("O-2")];
        let prepared = PreparedLimitHttpRequest::new(
            LimitHttpRequestEndpoint::Batch,
            b"[]".to_vec(),
            venue_ids.clone(),
            client_ids.clone(),
        )
        .expect("valid prepared batch");

        assert_eq!(prepared.expected_venue_order_ids(), venue_ids);
        assert_eq!(prepared.client_order_ids(), client_ids);
    }

    #[rstest]
    fn test_prepared_request_validation_error_redacts_body() {
        const SENSITIVE_BODY: &str = "owner-api-key-identifier";
        let result = PreparedLimitHttpRequest::new(
            LimitHttpRequestEndpoint::Single,
            SENSITIVE_BODY.as_bytes().to_vec(),
            vec![],
            vec![],
        );
        let error = match result {
            Ok(_) => panic!("empty identity set must fail"),
            Err(error) => error,
        };

        assert!(!error.contains(SENSITIVE_BODY));
        assert!(!error.contains("owner"));
    }
}
