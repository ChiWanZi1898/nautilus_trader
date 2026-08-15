// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Immutable adapter evidence facts and their application-owned durability bridge.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use async_trait::async_trait;
use aws_lc_rs::digest;
use nautilus_model::identifiers::{ClientOrderId, VenueOrderId};

use crate::{
    common::enums::PolymarketOrderType, evidence_v2::PolymarketAuthenticatedUserFrameV2,
    http::models::PolymarketOrder, signing::eip712::order_hash,
};

const PREPARED_ID_DOMAIN: &[u8] = b"nautilus-polymarket/submit-prepared/v1\0";

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
    neg_risk: bool,
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
    pub const fn neg_risk(&self) -> bool {
        self.neg_risk
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
        neg_risk: bool,
        expected_venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
    ) -> Self {
        Self {
            order,
            order_type,
            post_only,
            neg_risk,
            expected_venue_order_id,
            client_order_id,
        }
    }

    /// Restores and verifies one credential-free signed leg.
    ///
    /// # Errors
    ///
    /// Returns an error unless the retained expected venue identity equals the
    /// EIP-712 hash of the exact signed fields and neg-risk domain.
    pub fn try_restore(
        order: PolymarketOrder,
        order_type: PolymarketOrderType,
        post_only: bool,
        neg_risk: bool,
        expected_venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
    ) -> Result<Self, PolymarketEvidenceError> {
        let calculated =
            order_hash(&order, neg_risk).map_err(|_| PolymarketEvidenceError::InvalidFact)?;
        let calculated = VenueOrderId::from(format!("{calculated:#x}").as_str());
        if calculated != expected_venue_order_id {
            return Err(PolymarketEvidenceError::InvalidFact);
        }
        Ok(Self::new(
            order,
            order_type,
            post_only,
            neg_risk,
            expected_venue_order_id,
            client_order_id,
        ))
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

    /// Restores a prepared fact from the durable bridge representation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid cardinality, identities, body hash, or any
    /// signed leg whose retained EIP-712 identity does not verify.
    pub fn try_restore(
        endpoint: PolymarketMutationEndpoint,
        body_sha256: [u8; 32],
        legs: Vec<PolymarketSignedLimitEvidence>,
    ) -> Result<Self, PolymarketEvidenceError> {
        for leg in &legs {
            let calculated = order_hash(leg.order(), leg.neg_risk())
                .map_err(|_| PolymarketEvidenceError::InvalidFact)?;
            let calculated = VenueOrderId::from(format!("{calculated:#x}").as_str());
            if calculated != leg.expected_venue_order_id() {
                return Err(PolymarketEvidenceError::InvalidFact);
            }
        }
        Self::try_new(endpoint, body_sha256, legs)
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

    /// Restores a handoff marker from durable fields.
    ///
    /// # Errors
    ///
    /// Returns an error for a reserved zero identity or body hash.
    pub fn try_restore(
        prepared_fact_id: [u8; 32],
        body_sha256: [u8; 32],
    ) -> Result<Self, PolymarketEvidenceError> {
        if prepared_fact_id == [0_u8; 32] || body_sha256 == [0_u8; 32] {
            return Err(PolymarketEvidenceError::InvalidFact);
        }
        Ok(Self {
            prepared_fact_id,
            body_sha256,
        })
    }
}

/// Versioned mutation facts accepted by the durable bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolymarketMutationEvidence<'a> {
    SubmitPrepared(&'a PolymarketSubmitPrepared),
    HandoffStarted(PolymarketHandoffStarted),
}

/// One owned mutation WAL record in its independent durable sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketRecoveredMutation {
    evidence_sequence: u64,
    fact: PolymarketRecoveredMutationFact,
}

impl PolymarketRecoveredMutation {
    /// Restores one mutation record.
    ///
    /// # Errors
    ///
    /// Returns an error for the reserved zero evidence sequence.
    pub fn try_new(
        evidence_sequence: u64,
        fact: PolymarketRecoveredMutationFact,
    ) -> Result<Self, PolymarketEvidenceError> {
        if evidence_sequence == 0 {
            return Err(PolymarketEvidenceError::Recovery);
        }
        Ok(Self {
            evidence_sequence,
            fact,
        })
    }

    #[must_use]
    pub const fn evidence_sequence(&self) -> u64 {
        self.evidence_sequence
    }

    #[must_use]
    pub const fn fact(&self) -> &PolymarketRecoveredMutationFact {
        &self.fact
    }
}

/// Owned mutation fact variants returned during recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolymarketRecoveredMutationFact {
    SubmitPrepared(PolymarketSubmitPrepared),
    HandoffStarted(PolymarketHandoffStarted),
}

/// One owned inbound WAL record in its independent durable sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketRecoveredUserFrame {
    evidence_sequence: u64,
    frame: PolymarketAuthenticatedUserFrameV2,
}

impl PolymarketRecoveredUserFrame {
    /// Restores one inbound record.
    ///
    /// # Errors
    ///
    /// Returns an error for the reserved zero evidence sequence.
    pub fn try_new(
        evidence_sequence: u64,
        frame: PolymarketAuthenticatedUserFrameV2,
    ) -> Result<Self, PolymarketEvidenceError> {
        if evidence_sequence == 0 {
            return Err(PolymarketEvidenceError::Recovery);
        }
        Ok(Self {
            evidence_sequence,
            frame,
        })
    }

    #[must_use]
    pub const fn evidence_sequence(&self) -> u64 {
        self.evidence_sequence
    }

    #[must_use]
    pub const fn frame(&self) -> &PolymarketAuthenticatedUserFrameV2 {
        &self.frame
    }
}

/// Complete verified mutation/inbound prefixes captured before adapter start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketEvidenceRecovery {
    lineage: [u8; 32],
    mutation_high_watermark: u64,
    inbound_high_watermark: u64,
    mutations: Arc<[PolymarketRecoveredMutation]>,
    user_frames: Arc<[PolymarketRecoveredUserFrame]>,
}

impl PolymarketEvidenceRecovery {
    /// Validates two exact independent recovery prefixes.
    ///
    /// # Errors
    ///
    /// Returns an error for zero lineage, a sequence gap/reorder, duplicate or
    /// orphan mutation identity, handoff/body mismatch, or duplicate user
    /// session/frame identity.
    pub fn try_new(
        lineage: [u8; 32],
        mutation_high_watermark: u64,
        inbound_high_watermark: u64,
        mutations: Vec<PolymarketRecoveredMutation>,
        user_frames: Vec<PolymarketRecoveredUserFrame>,
    ) -> Result<Self, PolymarketEvidenceError> {
        if lineage == [0_u8; 32]
            || !is_complete_prefix(
                mutations.iter().map(|record| record.evidence_sequence),
                mutation_high_watermark,
            )
            || !is_complete_prefix(
                user_frames.iter().map(|record| record.evidence_sequence),
                inbound_high_watermark,
            )
        {
            return Err(PolymarketEvidenceError::Recovery);
        }

        let mut prepared = HashMap::new();
        let mut handed_off = HashSet::new();
        for record in &mutations {
            match record.fact() {
                PolymarketRecoveredMutationFact::SubmitPrepared(fact) => {
                    if prepared
                        .insert(*fact.fact_id(), *fact.body_sha256())
                        .is_some()
                    {
                        return Err(PolymarketEvidenceError::Recovery);
                    }
                }
                PolymarketRecoveredMutationFact::HandoffStarted(fact) => {
                    if prepared.get(fact.prepared_fact_id()) != Some(fact.body_sha256())
                        || !handed_off.insert(*fact.prepared_fact_id())
                    {
                        return Err(PolymarketEvidenceError::Recovery);
                    }
                }
            }
        }
        let mut previous_frame: Option<(u64, u64)> = None;
        for record in &user_frames {
            let current = (record.frame.session_epoch(), record.frame.frame_sequence());
            match previous_frame {
                None if current.1 != 1 => return Err(PolymarketEvidenceError::Recovery),
                Some((epoch, sequence)) if current.0 == epoch => {
                    if sequence.checked_add(1) != Some(current.1) {
                        return Err(PolymarketEvidenceError::Recovery);
                    }
                }
                Some((epoch, _)) if current.0 > epoch && current.1 == 1 => {}
                Some(_) => return Err(PolymarketEvidenceError::Recovery),
                None => {}
            }
            previous_frame = Some(current);
        }
        Ok(Self {
            lineage,
            mutation_high_watermark,
            inbound_high_watermark,
            mutations: mutations.into(),
            user_frames: user_frames.into(),
        })
    }

    #[must_use]
    pub const fn lineage(&self) -> &[u8; 32] {
        &self.lineage
    }

    #[must_use]
    pub const fn mutation_high_watermark(&self) -> u64 {
        self.mutation_high_watermark
    }

    #[must_use]
    pub const fn inbound_high_watermark(&self) -> u64 {
        self.inbound_high_watermark
    }

    #[must_use]
    pub fn mutations(&self) -> &[PolymarketRecoveredMutation] {
        &self.mutations
    }

    #[must_use]
    pub fn user_frames(&self) -> &[PolymarketRecoveredUserFrame] {
        &self.user_frames
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
    /// Returns the complete prefixes verified under the bridge process lock.
    fn recover(&self) -> Result<PolymarketEvidenceRecovery, PolymarketEvidenceError>;

    /// Opens H+1 appends only after the adapter has restored both exact prefixes.
    fn acknowledge_recovery(
        &self,
        mutation_high_watermark: u64,
        inbound_high_watermark: u64,
    ) -> Result<(), PolymarketEvidenceError>;

    /// Appends one immutable mutation fact and returns only after durability.
    async fn append_mutation(
        &self,
        fact: &PolymarketMutationEvidence<'_>,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError>;

    /// Appends one exact authenticated user frame before normalized dispatch.
    async fn append_authenticated_user_frame(
        &self,
        fact: &PolymarketAuthenticatedUserFrameV2,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError>;
}

fn is_complete_prefix(sequences: impl Iterator<Item = u64>, high_watermark: u64) -> bool {
    let mut expected = 1_u64;
    for sequence in sequences {
        if sequence != expected {
            return false;
        }
        let Some(next) = expected.checked_add(1) else {
            return false;
        };
        expected = next;
    }
    expected.checked_sub(1) == Some(high_watermark)
}

fn append_bounded_text(target: &mut Vec<u8>, value: &str) -> Result<(), PolymarketEvidenceError> {
    let length = u16::try_from(value.len()).map_err(|_| PolymarketEvidenceError::InvalidFact)?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn recovered_frame(
        evidence_sequence: u64,
        session_epoch: u64,
        frame_sequence: u64,
    ) -> PolymarketRecoveredUserFrame {
        PolymarketRecoveredUserFrame::try_new(
            evidence_sequence,
            PolymarketAuthenticatedUserFrameV2::project(
                include_str!("../test_data/ws_user_order_msg.json"),
                session_epoch,
                frame_sequence,
                "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
                "00000000-0000-0000-0000-000000000001",
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[rstest]
    fn recovery_accepts_independent_complete_prefixes_and_session_rollover() {
        let recovery = PolymarketEvidenceRecovery::try_new(
            [0x51; 32],
            0,
            3,
            Vec::new(),
            vec![
                recovered_frame(1, 7, 1),
                recovered_frame(2, 7, 2),
                recovered_frame(3, 9, 1),
            ],
        )
        .unwrap();
        assert_eq!(recovery.mutation_high_watermark(), 0);
        assert_eq!(recovery.inbound_high_watermark(), 3);
    }

    #[rstest]
    #[case(vec![recovered_frame(1, 1, 2)], 1)]
    #[case(vec![recovered_frame(1, 1, 1), recovered_frame(2, 1, 3)], 2)]
    #[case(vec![recovered_frame(1, 2, 1), recovered_frame(2, 1, 1)], 2)]
    #[case(vec![recovered_frame(1, 1, 1), recovered_frame(2, 2, 2)], 2)]
    fn recovery_rejects_noncontiguous_user_session_frames(
        #[case] frames: Vec<PolymarketRecoveredUserFrame>,
        #[case] high_watermark: u64,
    ) {
        assert_eq!(
            PolymarketEvidenceRecovery::try_new([0x52; 32], 0, high_watermark, Vec::new(), frames,),
            Err(PolymarketEvidenceError::Recovery),
        );
    }

    #[rstest]
    fn recovery_rejects_orphan_handoff() {
        let handoff = PolymarketHandoffStarted::try_restore([0x11; 32], [0x22; 32]).unwrap();
        let mutation = PolymarketRecoveredMutation::try_new(
            1,
            PolymarketRecoveredMutationFact::HandoffStarted(handoff),
        )
        .unwrap();
        assert_eq!(
            PolymarketEvidenceRecovery::try_new([0x53; 32], 1, 0, vec![mutation], Vec::new(),),
            Err(PolymarketEvidenceError::Recovery),
        );
    }
}
