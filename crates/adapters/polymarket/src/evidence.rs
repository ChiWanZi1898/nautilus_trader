// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Immutable adapter evidence facts and their application-owned durability bridge.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use aws_lc_rs::digest;
use nautilus_model::identifiers::{ClientOrderId, VenueOrderId};

use crate::{common::enums::PolymarketOrderType, http::models::PolymarketOrder};

const PREPARED_ID_DOMAIN: &[u8] = b"nautilus-polymarket/submit-prepared/v1\0";
const USER_FRAME_ID_DOMAIN: &[u8] = b"nautilus-polymarket/authenticated-user-frame/v1\0";

/// Single or batch HTTP endpoint selected for one prepared mutation group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolymarketMutationEndpoint {
    Single,
    Batch,
}

/// Credential-free signed leg retained for crash recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketSignedLimitEvidence {
    order: PolymarketOrder,
    order_type: PolymarketOrderType,
    post_only: bool,
    expected_venue_order_id: VenueOrderId,
    client_order_id: ClientOrderId,
}

impl PolymarketSignedLimitEvidence {
    #[must_use]
    pub const fn order(&self) -> &PolymarketOrder {
        &self.order
    }

    #[must_use]
    pub const fn order_type(&self) -> PolymarketOrderType {
        self.order_type
    }

    #[must_use]
    pub const fn post_only(&self) -> bool {
        self.post_only
    }

    #[must_use]
    pub const fn expected_venue_order_id(&self) -> VenueOrderId {
        self.expected_venue_order_id
    }

    #[must_use]
    pub const fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }

    pub(crate) fn new(
        order: PolymarketOrder,
        order_type: PolymarketOrderType,
        post_only: bool,
        expected_venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
    ) -> Self {
        Self {
            order,
            order_type,
            post_only,
            expected_venue_order_id,
            client_order_id,
        }
    }
}

/// Credential-free prepared mutation fact which must be durable before local
/// expected-order identity activation.
#[derive(Clone, Eq, PartialEq)]
pub struct PolymarketSubmitPrepared {
    fact_id: [u8; 32],
    endpoint: PolymarketMutationEndpoint,
    body_sha256: [u8; 32],
    legs: Arc<[PolymarketSignedLimitEvidence]>,
}

impl Debug for PolymarketSubmitPrepared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolymarketSubmitPrepared")
            .field("fact_id", &self.fact_id)
            .field("endpoint", &self.endpoint)
            .field("body_sha256", &self.body_sha256)
            .field("leg_count", &self.legs.len())
            .finish()
    }
}

impl PolymarketSubmitPrepared {
    #[must_use]
    pub const fn fact_id(&self) -> &[u8; 32] {
        &self.fact_id
    }

    #[must_use]
    pub const fn endpoint(&self) -> PolymarketMutationEndpoint {
        self.endpoint
    }

    #[must_use]
    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }

    #[must_use]
    pub fn legs(&self) -> &[PolymarketSignedLimitEvidence] {
        &self.legs
    }

    pub(crate) fn try_new(
        endpoint: PolymarketMutationEndpoint,
        body_sha256: [u8; 32],
        legs: Vec<PolymarketSignedLimitEvidence>,
    ) -> Result<Self, PolymarketEvidenceError> {
        let valid_count = match endpoint {
            PolymarketMutationEndpoint::Single => legs.len() == 1,
            PolymarketMutationEndpoint::Batch => (2..=15).contains(&legs.len()),
        };
        if !valid_count || body_sha256 == [0_u8; 32] {
            return Err(PolymarketEvidenceError::InvalidFact);
        }
        let mut input = Vec::new();
        input.extend_from_slice(PREPARED_ID_DOMAIN);
        input.push(match endpoint {
            PolymarketMutationEndpoint::Single => 1,
            PolymarketMutationEndpoint::Batch => 2,
        });
        input.extend_from_slice(&body_sha256);
        input.push(u8::try_from(legs.len()).map_err(|_| PolymarketEvidenceError::InvalidFact)?);
        for leg in &legs {
            append_bounded_text(&mut input, leg.expected_venue_order_id.as_ref())?;
            append_bounded_text(&mut input, leg.client_order_id.as_ref())?;
        }
        let fact_id = digest::digest(&digest::SHA256, &input)
            .as_ref()
            .try_into()
            .map_err(|_| PolymarketEvidenceError::InvalidFact)?;
        Ok(Self {
            fact_id,
            endpoint,
            body_sha256,
            legs: legs.into(),
        })
    }
}

/// Immutable transition persisted immediately before the HTTP handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketHandoffStarted {
    prepared_fact_id: [u8; 32],
    body_sha256: [u8; 32],
}

impl PolymarketHandoffStarted {
    #[must_use]
    pub const fn prepared_fact_id(&self) -> &[u8; 32] {
        &self.prepared_fact_id
    }

    #[must_use]
    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }

    pub(crate) const fn new(prepared: &PolymarketSubmitPrepared) -> Self {
        Self {
            prepared_fact_id: prepared.fact_id,
            body_sha256: prepared.body_sha256,
        }
    }
}

/// Versioned mutation facts accepted by the durable bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolymarketMutationEvidence<'a> {
    SubmitPrepared(&'a PolymarketSubmitPrepared),
    HandoffStarted(PolymarketHandoffStarted),
}

/// Exact authenticated user-channel text frame retained before any element is
/// released to normalized order lifecycle dispatch.
#[derive(Clone, Eq, PartialEq)]
pub struct PolymarketAuthenticatedUserFrame {
    fact_id: [u8; 32],
    session_epoch: u64,
    frame_sequence: u64,
    raw_utf8: Arc<[u8]>,
}

impl Debug for PolymarketAuthenticatedUserFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolymarketAuthenticatedUserFrame")
            .field("fact_id", &self.fact_id)
            .field("session_epoch", &self.session_epoch)
            .field("frame_sequence", &self.frame_sequence)
            .field("raw_len", &self.raw_utf8.len())
            .finish()
    }
}

impl PolymarketAuthenticatedUserFrame {
    #[must_use]
    pub const fn fact_id(&self) -> &[u8; 32] {
        &self.fact_id
    }

    #[must_use]
    pub const fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    #[must_use]
    pub const fn frame_sequence(&self) -> u64 {
        self.frame_sequence
    }

    #[must_use]
    pub fn raw_utf8(&self) -> &[u8] {
        &self.raw_utf8
    }

    pub(crate) fn try_new(
        session_epoch: u64,
        frame_sequence: u64,
        raw_utf8: &[u8],
    ) -> Result<Self, PolymarketEvidenceError> {
        if session_epoch == 0 || frame_sequence == 0 || raw_utf8.is_empty() {
            return Err(PolymarketEvidenceError::InvalidFact);
        }
        let mut input = Vec::new();
        input.extend_from_slice(USER_FRAME_ID_DOMAIN);
        input.extend_from_slice(&session_epoch.to_be_bytes());
        input.extend_from_slice(&frame_sequence.to_be_bytes());
        input.extend_from_slice(raw_utf8);
        let fact_id = digest::digest(&digest::SHA256, &input)
            .as_ref()
            .try_into()
            .map_err(|_| PolymarketEvidenceError::InvalidFact)?;
        Ok(Self {
            fact_id,
            session_epoch,
            frame_sequence,
            raw_utf8: Arc::from(raw_utf8),
        })
    }
}

impl PolymarketMutationEvidence<'_> {
    #[must_use]
    pub const fn fact_id(&self) -> &[u8; 32] {
        match self {
            Self::SubmitPrepared(fact) => fact.fact_id(),
            Self::HandoffStarted(fact) => fact.prepared_fact_id(),
        }
    }
}

/// Durable acknowledgement returned by the application-owned bridge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketEvidenceAck {
    fact_id: [u8; 32],
    sequence: u64,
}

impl PolymarketEvidenceAck {
    /// Creates an acknowledgement for one durably committed fact.
    ///
    /// # Errors
    ///
    /// Returns an error for the reserved zero sequence or zero fact identity.
    pub fn try_new(fact_id: [u8; 32], sequence: u64) -> Result<Self, PolymarketEvidenceError> {
        if fact_id == [0_u8; 32] || sequence == 0 {
            return Err(PolymarketEvidenceError::InvalidAcknowledgement);
        }
        Ok(Self { fact_id, sequence })
    }

    #[must_use]
    pub const fn fact_id(&self) -> &[u8; 32] {
        &self.fact_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Redacted failure categories for the application-owned evidence bridge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolymarketEvidenceError {
    #[error("evidence bridge is unavailable")]
    Unavailable,
    #[error("evidence durability failed")]
    Durability,
    #[error("evidence capacity is exhausted")]
    Capacity,
    #[error("evidence recovery is incomplete or corrupt")]
    Recovery,
    #[error("adapter evidence fact is invalid")]
    InvalidFact,
    #[error("evidence acknowledgement is invalid")]
    InvalidAcknowledgement,
}

/// Application-owned durability boundary injected into the adapter.
#[async_trait]
pub trait PolymarketEvidenceBridge: Debug + Send + Sync {
    /// Appends one immutable mutation fact and returns only after durability.
    async fn append_mutation(
        &self,
        fact: &PolymarketMutationEvidence<'_>,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError>;

    /// Appends one exact authenticated user frame before normalized dispatch.
    async fn append_authenticated_user_frame(
        &self,
        fact: &PolymarketAuthenticatedUserFrame,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError>;
}

fn append_bounded_text(target: &mut Vec<u8>, value: &str) -> Result<(), PolymarketEvidenceError> {
    let length = u16::try_from(value.len()).map_err(|_| PolymarketEvidenceError::InvalidFact)?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value.as_bytes());
    Ok(())
}
