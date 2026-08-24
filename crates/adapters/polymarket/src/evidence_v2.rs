// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Strict credential-free projection of authenticated Polymarket user frames.
//!
//! The authenticated user lane projects each complete wire frame into this canonical form before
//! durable append and releases normalized lifecycle messages only after the matching durability
//! acknowledgement. Credential wire fields are compared transiently and cannot be retained by any
//! public output type.

use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
    str::FromStr,
};

use aws_lc_rs::digest::{self, SHA256};
use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer,
    de::{Error as DeError, SeqAccess, Visitor},
};
use thiserror::Error;
use ustr::Ustr;

use crate::{
    common::{
        enums::{
            PolymarketEventType, PolymarketLiquiditySide, PolymarketOrderSide,
            PolymarketOrderStatus, PolymarketOrderType, PolymarketOutcome, PolymarketTradeStatus,
        },
        models::PolymarketMakerOrder,
    },
    websocket::messages::{PolymarketUserOrder, PolymarketUserTrade, UserWsMessage},
};

const HASH_DOMAIN: &[u8] = b"nautilus-polymarket/authenticated-user-frame/v2\0";
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_ELEMENTS: usize = 1024;
const MAX_NESTED: usize = 1024;
const MAX_TEXT: usize = 512;
const MAX_DECIMAL: usize = 128;
const OWNED_RELATION_MARKER: &str = "__POLYMARKET_EVIDENCE_OWNED__";
const FOREIGN_RELATION_MARKER: &str = "__POLYMARKET_EVIDENCE_FOREIGN__";

struct BoundedVec<T, const MAX: usize>(Vec<T>);

impl<T, const MAX: usize> BoundedVec<T, MAX> {
    fn into_vec(self) -> Vec<T> {
        self.0
    }
}

impl<'de, T, const MAX: usize> Deserialize<'de> for BoundedVec<T, MAX>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoundedVisitor<T, const MAX: usize>(PhantomData<T>);

        impl<'de, T, const MAX: usize> Visitor<'de> for BoundedVisitor<T, MAX>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T, MAX>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "an array containing at most {MAX} elements")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if sequence.size_hint().is_some_and(|length| length > MAX) {
                    return Err(A::Error::custom("array exceeds its hard element bound"));
                }
                let mut values = Vec::new();
                values
                    .try_reserve_exact(sequence.size_hint().unwrap_or(0).min(MAX))
                    .map_err(|_| A::Error::custom("array allocation failed"))?;
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX {
                        return Err(A::Error::custom("array exceeds its hard element bound"));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVisitor(PhantomData))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceEnvelopeV2 {
    Single = 1,
    Batch = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceSideV2 {
    Buy = 1,
    Sell = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceRoleV2 {
    Maker = 1,
    Taker = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceEventV2 {
    Placement = 1,
    Update = 2,
    Cancellation = 3,
    Trade = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceOrderTypeV2 {
    Fok = 1,
    Fak = 2,
    Gtc = 3,
    Gtd = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceOrderStatusV2 {
    Invalid = 1,
    Live = 2,
    Delayed = 3,
    Matched = 4,
    Unmatched = 5,
    Canceled = 6,
    CanceledMarketResolved = 7,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PolymarketEvidenceTradeStatusV2 {
    MatchedNotBroadcasted = 1,
    Matched = 2,
    Mined = 3,
    Confirmed = 4,
    Retrying = 5,
    Failed = 6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketWireDecimalV2 {
    mantissa: u128,
    scale: u8,
}

impl PolymarketWireDecimalV2 {
    fn parse(value: &str) -> Result<Self, PolymarketEvidenceV2Error> {
        if value.is_empty() || value.len() > MAX_DECIMAL || !value.is_ascii() {
            return Err(PolymarketEvidenceV2Error::InvalidDecimal);
        }
        let mut point_seen = false;
        let mut digit_seen = false;
        let mut scale = 0_u8;
        let mut mantissa = 0_u128;
        for byte in value.bytes() {
            if byte.is_ascii_digit() {
                digit_seen = true;
                mantissa = mantissa
                    .checked_mul(10)
                    .and_then(|current| current.checked_add(u128::from(byte - b'0')))
                    .ok_or(PolymarketEvidenceV2Error::InvalidDecimal)?;
                if point_seen {
                    scale = scale
                        .checked_add(1)
                        .ok_or(PolymarketEvidenceV2Error::InvalidDecimal)?;
                }
            } else if byte == b'.' && !point_seen {
                point_seen = true;
            } else {
                return Err(PolymarketEvidenceV2Error::InvalidDecimal);
            }
        }
        if !digit_seen || value.starts_with('.') || value.ends_with('.') {
            return Err(PolymarketEvidenceV2Error::InvalidDecimal);
        }
        while scale > 0 && mantissa.is_multiple_of(10) {
            mantissa /= 10;
            scale -= 1;
        }
        if mantissa == 0 {
            scale = 0;
        }
        if scale > 38 {
            return Err(PolymarketEvidenceV2Error::InvalidDecimal);
        }
        Ok(Self { mantissa, scale })
    }

    fn zero() -> Self {
        Self {
            mantissa: 0,
            scale: 0,
        }
    }

    #[must_use]
    pub const fn mantissa(self) -> u128 {
        self.mantissa
    }

    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketOrderOwnershipV2 {
    maker_address_matches_account: bool,
    owner_matches_api_key: bool,
    order_owner_matches_api_key: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketTradeOwnershipV2 {
    maker_address_matches_account: bool,
    owner_matches_api_key: bool,
    trade_owner_matches_api_key: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketMakerOwnershipV2 {
    maker_address_matches_account: bool,
    owner_matches_api_key: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketMakerEvidenceV2 {
    asset_id: String,
    maker_address: String,
    matched_amount: PolymarketWireDecimalV2,
    order_id: String,
    outcome: Option<String>,
    ownership: PolymarketMakerOwnershipV2,
    price: PolymarketWireDecimalV2,
    side: Option<PolymarketEvidenceSideV2>,
    fee_rate_bps: Option<PolymarketWireDecimalV2>,
}

impl PolymarketMakerEvidenceV2 {
    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    #[must_use]
    pub const fn ownership(&self) -> PolymarketMakerOwnershipV2 {
        self.ownership
    }

    #[must_use]
    pub const fn fee_rate_bps(&self) -> Option<PolymarketWireDecimalV2> {
        self.fee_rate_bps
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketOrderEvidenceV2 {
    asset_id: String,
    associated_trades: Box<[String]>,
    created_at: Option<String>,
    expiration: Option<String>,
    order_id: String,
    maker_address: String,
    market: String,
    ownership: PolymarketOrderOwnershipV2,
    order_type: PolymarketEvidenceOrderTypeV2,
    original_size: PolymarketWireDecimalV2,
    outcome: Option<String>,
    price: PolymarketWireDecimalV2,
    side: PolymarketEvidenceSideV2,
    size_matched: PolymarketWireDecimalV2,
    status: PolymarketEvidenceOrderStatusV2,
    status_detail: Option<String>,
    timestamp: String,
    event: PolymarketEvidenceEventV2,
}

impl PolymarketOrderEvidenceV2 {
    #[must_use]
    pub const fn ownership(&self) -> PolymarketOrderOwnershipV2 {
        self.ownership
    }

    #[must_use]
    pub const fn status(&self) -> PolymarketEvidenceOrderStatusV2 {
        self.status
    }

    #[must_use]
    pub fn status_detail(&self) -> Option<&str> {
        self.status_detail.as_deref()
    }

    #[must_use]
    pub const fn size_matched(&self) -> PolymarketWireDecimalV2 {
        self.size_matched
    }

    fn to_dispatch_message(&self) -> Result<PolymarketUserOrder, PolymarketEvidenceV2Error> {
        Ok(PolymarketUserOrder {
            asset_id: Ustr::from(self.asset_id.as_str()),
            associate_trades: Some(self.associated_trades.to_vec()),
            created_at: self.created_at.clone().unwrap_or_default(),
            expiration: self.expiration.clone(),
            id: self.order_id.clone(),
            maker_address: Ustr::from(self.maker_address.as_str()),
            market: Ustr::from(self.market.as_str()),
            order_owner: Ustr::from(relation_marker(self.ownership.order_owner_matches_api_key)),
            order_type: dispatch_order_type(self.order_type),
            original_size: dispatch_decimal_text(self.original_size)?,
            outcome: PolymarketOutcome::from(self.outcome.as_deref().unwrap_or("")),
            owner: Ustr::from(relation_marker(self.ownership.owner_matches_api_key)),
            price: dispatch_decimal_text(self.price)?,
            side: dispatch_side(self.side),
            size_matched: dispatch_decimal_text(self.size_matched)?,
            status: dispatch_order_status(self.status),
            timestamp: self.timestamp.clone(),
            event_type: dispatch_event(self.event),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketTradeEvidenceV2 {
    asset_id: String,
    bucket_index: u64,
    fee_rate_bps: Option<PolymarketWireDecimalV2>,
    trade_id: String,
    last_update: String,
    maker_address: String,
    maker_rows: Box<[PolymarketMakerEvidenceV2]>,
    market: String,
    match_time: String,
    outcome: Option<String>,
    ownership: PolymarketTradeOwnershipV2,
    price: PolymarketWireDecimalV2,
    side: PolymarketEvidenceSideV2,
    size: PolymarketWireDecimalV2,
    status: PolymarketEvidenceTradeStatusV2,
    taker_order_id: String,
    timestamp: String,
    transaction_hash: Option<String>,
    role: PolymarketEvidenceRoleV2,
    event: PolymarketEvidenceEventV2,
}

impl PolymarketTradeEvidenceV2 {
    #[must_use]
    pub const fn ownership(&self) -> PolymarketTradeOwnershipV2 {
        self.ownership
    }

    #[must_use]
    pub const fn status(&self) -> PolymarketEvidenceTradeStatusV2 {
        self.status
    }

    #[must_use]
    pub fn maker_rows(&self) -> &[PolymarketMakerEvidenceV2] {
        &self.maker_rows
    }

    fn to_dispatch_message(&self) -> Result<PolymarketUserTrade, PolymarketEvidenceV2Error> {
        let mut maker_orders = Vec::new();
        maker_orders
            .try_reserve_exact(self.maker_rows.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for maker in &self.maker_rows {
            maker_orders.push(maker.to_dispatch_message()?);
        }
        Ok(PolymarketUserTrade {
            asset_id: Ustr::from(self.asset_id.as_str()),
            bucket_index: self.bucket_index,
            fee_rate_bps: self
                .fee_rate_bps
                .map(dispatch_decimal_text)
                .transpose()?
                .unwrap_or_default(),
            id: self.trade_id.clone(),
            last_update: self.last_update.clone(),
            maker_address: Ustr::from(self.maker_address.as_str()),
            maker_orders,
            market: Ustr::from(self.market.as_str()),
            match_time: self.match_time.clone(),
            outcome: PolymarketOutcome::from(self.outcome.as_deref().unwrap_or("")),
            owner: Ustr::from(relation_marker(self.ownership.owner_matches_api_key)),
            price: dispatch_decimal_text(self.price)?,
            side: dispatch_side(self.side),
            size: dispatch_decimal_text(self.size)?,
            status: dispatch_trade_status(self.status),
            taker_order_id: self.taker_order_id.clone(),
            timestamp: self.timestamp.clone(),
            trade_owner: Ustr::from(relation_marker(self.ownership.trade_owner_matches_api_key)),
            transaction_hash: self.transaction_hash.clone(),
            trader_side: dispatch_role(self.role),
            event_type: dispatch_event(self.event),
        })
    }
}

impl PolymarketMakerEvidenceV2 {
    fn to_dispatch_message(&self) -> Result<PolymarketMakerOrder, PolymarketEvidenceV2Error> {
        Ok(PolymarketMakerOrder {
            asset_id: Ustr::from(self.asset_id.as_str()),
            maker_address: self.maker_address.clone(),
            matched_amount: Decimal::from_str(&dispatch_decimal_text(self.matched_amount)?)
                .map_err(|_| PolymarketEvidenceV2Error::InvalidDecimal)?,
            order_id: self.order_id.clone(),
            outcome: PolymarketOutcome::from(self.outcome.as_deref().unwrap_or("")),
            owner: relation_marker(self.ownership.owner_matches_api_key).to_owned(),
            price: Decimal::from_str(&dispatch_decimal_text(self.price)?)
                .map_err(|_| PolymarketEvidenceV2Error::InvalidDecimal)?,
            side: self.side.map(dispatch_side),
        })
    }
}

impl PolymarketOrderOwnershipV2 {
    #[must_use]
    pub const fn maker_address_matches_account(self) -> bool {
        self.maker_address_matches_account
    }

    #[must_use]
    pub const fn owner_matches_api_key(self) -> bool {
        self.owner_matches_api_key
    }

    #[must_use]
    pub const fn order_owner_matches_api_key(self) -> bool {
        self.order_owner_matches_api_key
    }
}

impl PolymarketTradeOwnershipV2 {
    #[must_use]
    pub const fn maker_address_matches_account(self) -> bool {
        self.maker_address_matches_account
    }

    #[must_use]
    pub const fn owner_matches_api_key(self) -> bool {
        self.owner_matches_api_key
    }

    #[must_use]
    pub const fn trade_owner_matches_api_key(self) -> bool {
        self.trade_owner_matches_api_key
    }
}

impl PolymarketMakerOwnershipV2 {
    #[must_use]
    pub const fn maker_address_matches_account(self) -> bool {
        self.maker_address_matches_account
    }

    #[must_use]
    pub const fn owner_matches_api_key(self) -> bool {
        self.owner_matches_api_key
    }
}

const fn relation_marker(matches: bool) -> &'static str {
    if matches {
        OWNED_RELATION_MARKER
    } else {
        FOREIGN_RELATION_MARKER
    }
}

fn decimal_text(value: PolymarketWireDecimalV2) -> String {
    let mut digits = value.mantissa.to_string();
    let scale = usize::from(value.scale);
    if scale == 0 {
        return digits;
    }
    if digits.len() > scale {
        digits.insert(digits.len() - scale, '.');
        digits
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    }
}

fn dispatch_decimal_text(
    value: PolymarketWireDecimalV2,
) -> Result<String, PolymarketEvidenceV2Error> {
    let text = decimal_text(value);
    Decimal::from_str(&text).map_err(|_| PolymarketEvidenceV2Error::InvalidDecimal)?;
    Ok(text)
}

const fn dispatch_side(value: PolymarketEvidenceSideV2) -> PolymarketOrderSide {
    match value {
        PolymarketEvidenceSideV2::Buy => PolymarketOrderSide::Buy,
        PolymarketEvidenceSideV2::Sell => PolymarketOrderSide::Sell,
    }
}

const fn dispatch_role(value: PolymarketEvidenceRoleV2) -> PolymarketLiquiditySide {
    match value {
        PolymarketEvidenceRoleV2::Maker => PolymarketLiquiditySide::Maker,
        PolymarketEvidenceRoleV2::Taker => PolymarketLiquiditySide::Taker,
    }
}

const fn dispatch_event(value: PolymarketEvidenceEventV2) -> PolymarketEventType {
    match value {
        PolymarketEvidenceEventV2::Placement => PolymarketEventType::Placement,
        PolymarketEvidenceEventV2::Update => PolymarketEventType::Update,
        PolymarketEvidenceEventV2::Cancellation => PolymarketEventType::Cancellation,
        PolymarketEvidenceEventV2::Trade => PolymarketEventType::Trade,
    }
}

const fn dispatch_order_type(value: PolymarketEvidenceOrderTypeV2) -> PolymarketOrderType {
    match value {
        PolymarketEvidenceOrderTypeV2::Fok => PolymarketOrderType::FOK,
        PolymarketEvidenceOrderTypeV2::Fak => PolymarketOrderType::FAK,
        PolymarketEvidenceOrderTypeV2::Gtc => PolymarketOrderType::GTC,
        PolymarketEvidenceOrderTypeV2::Gtd => PolymarketOrderType::GTD,
    }
}

const fn dispatch_order_status(value: PolymarketEvidenceOrderStatusV2) -> PolymarketOrderStatus {
    match value {
        PolymarketEvidenceOrderStatusV2::Invalid => PolymarketOrderStatus::Invalid,
        PolymarketEvidenceOrderStatusV2::Live => PolymarketOrderStatus::Live,
        PolymarketEvidenceOrderStatusV2::Delayed => PolymarketOrderStatus::Delayed,
        PolymarketEvidenceOrderStatusV2::Matched => PolymarketOrderStatus::Matched,
        PolymarketEvidenceOrderStatusV2::Unmatched => PolymarketOrderStatus::Unmatched,
        PolymarketEvidenceOrderStatusV2::Canceled => PolymarketOrderStatus::Canceled,
        PolymarketEvidenceOrderStatusV2::CanceledMarketResolved => {
            PolymarketOrderStatus::CanceledMarketResolved
        }
    }
}

const fn dispatch_trade_status(value: PolymarketEvidenceTradeStatusV2) -> PolymarketTradeStatus {
    match value {
        PolymarketEvidenceTradeStatusV2::MatchedNotBroadcasted => {
            PolymarketTradeStatus::MatchedNotBroadcasted
        }
        PolymarketEvidenceTradeStatusV2::Matched => PolymarketTradeStatus::Matched,
        PolymarketEvidenceTradeStatusV2::Mined => PolymarketTradeStatus::Mined,
        PolymarketEvidenceTradeStatusV2::Confirmed => PolymarketTradeStatus::Confirmed,
        PolymarketEvidenceTradeStatusV2::Retrying => PolymarketTradeStatus::Retrying,
        PolymarketEvidenceTradeStatusV2::Failed => PolymarketTradeStatus::Failed,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolymarketUserEvidenceElementV2 {
    Order(PolymarketOrderEvidenceV2),
    Trade(PolymarketTradeEvidenceV2),
}

#[derive(Clone, Eq, PartialEq)]
pub struct PolymarketAuthenticatedUserFrameV2 {
    fact_id: [u8; 32],
    canonical_bytes: Box<[u8]>,
    session_epoch: u64,
    frame_sequence: u64,
    envelope: PolymarketEvidenceEnvelopeV2,
    elements: Box<[PolymarketUserEvidenceElementV2]>,
}

impl Debug for PolymarketAuthenticatedUserFrameV2 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolymarketAuthenticatedUserFrameV2")
            .field("fact_id", &self.fact_id)
            .field("session_epoch", &self.session_epoch)
            .field("frame_sequence", &self.frame_sequence)
            .field("envelope", &self.envelope)
            .field("element_count", &self.elements.len())
            .field("canonical_len", &self.canonical_bytes.len())
            .finish()
    }
}

impl PolymarketAuthenticatedUserFrameV2 {
    /// Strictly projects one complete authenticated wire frame without retaining credentials.
    ///
    /// # Errors
    ///
    /// Rejects unknown fields/enums, malformed decimals, invalid sentinels, partial batches, zero
    /// sequence coordinates, and any canonical payload exceeding the hard bound.
    pub fn project(
        raw: &str,
        session_epoch: u64,
        frame_sequence: u64,
        account_address: &str,
        api_key: &str,
    ) -> Result<Self, PolymarketEvidenceV2Error> {
        if raw.len() > MAX_FRAME_BYTES
            || session_epoch == 0
            || frame_sequence == 0
            || account_address.is_empty()
            || api_key.is_empty()
        {
            return Err(PolymarketEvidenceV2Error::InvalidFrame);
        }
        let (envelope, wire) = if raw.trim_start().starts_with('[') {
            (
                PolymarketEvidenceEnvelopeV2::Batch,
                serde_json::from_str::<BoundedVec<WireUserMessage, MAX_ELEMENTS>>(raw)
                    .map_err(|_| PolymarketEvidenceV2Error::InvalidJson)?
                    .into_vec(),
            )
        } else {
            (
                PolymarketEvidenceEnvelopeV2::Single,
                vec![
                    serde_json::from_str::<WireUserMessage>(raw)
                        .map_err(|_| PolymarketEvidenceV2Error::InvalidJson)?,
                ],
            )
        };
        if wire.is_empty() || wire.len() > MAX_ELEMENTS {
            return Err(PolymarketEvidenceV2Error::InvalidFrame);
        }
        let mut elements = Vec::new();
        elements
            .try_reserve_exact(wire.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for message in wire {
            elements.push(message.project(account_address, api_key)?);
        }
        let body = encode_body(session_epoch, frame_sequence, envelope, &elements)?;
        let mut context = digest::Context::new(&SHA256);
        context.update(HASH_DOMAIN);
        context.update(&body);
        let digest = context.finish();
        let fact_id = <[u8; 32]>::try_from(digest.as_ref())
            .map_err(|_| PolymarketEvidenceV2Error::InvalidFrame)?;
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(32 + body.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        canonical.extend_from_slice(&fact_id);
        canonical.extend_from_slice(&body);
        if canonical.len() > MAX_FRAME_BYTES {
            return Err(PolymarketEvidenceV2Error::Capacity);
        }
        Ok(Self {
            fact_id,
            canonical_bytes: canonical.into_boxed_slice(),
            session_epoch,
            frame_sequence,
            envelope,
            elements: elements.into_boxed_slice(),
        })
    }

    /// Restores, strictly decodes, and rehashes one canonical credential-free V2 frame.
    ///
    /// # Errors
    ///
    /// Rejects malformed fields, bounds, trailing bytes, noncanonical values, or a fact identity
    /// which does not match the canonical body.
    pub fn try_restore(canonical: &[u8]) -> Result<Self, PolymarketEvidenceV2Error> {
        if canonical.len() > MAX_FRAME_BYTES {
            return Err(PolymarketEvidenceV2Error::Capacity);
        }
        let mut reader = CanonicalReader::new(canonical);
        let fact_id = reader.array()?;
        if reader.byte()? != 2 {
            return Err(PolymarketEvidenceV2Error::InvalidEncoding);
        }
        let session_epoch = reader.u64()?;
        let frame_sequence = reader.u64()?;
        let envelope = decode_envelope(reader.byte()?)?;
        let count = usize::from(reader.u16()?);
        if session_epoch == 0
            || frame_sequence == 0
            || count == 0
            || count > MAX_ELEMENTS
            || (envelope == PolymarketEvidenceEnvelopeV2::Single && count != 1)
        {
            return Err(PolymarketEvidenceV2Error::InvalidEncoding);
        }
        let mut elements = Vec::new();
        elements
            .try_reserve_exact(count)
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for _ in 0..count {
            elements.push(decode_element(&mut reader)?);
        }
        reader.finish()?;
        let body = encode_body(session_epoch, frame_sequence, envelope, &elements)?;
        let mut context = digest::Context::new(&SHA256);
        context.update(HASH_DOMAIN);
        context.update(&body);
        let expected_fact_id = <[u8; 32]>::try_from(context.finish().as_ref())
            .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)?;
        if fact_id != expected_fact_id || canonical.get(32..) != Some(body.as_slice()) {
            return Err(PolymarketEvidenceV2Error::FactIdMismatch);
        }
        Ok(Self {
            fact_id,
            canonical_bytes: canonical.into(),
            session_epoch,
            frame_sequence,
            envelope,
            elements: elements.into_boxed_slice(),
        })
    }

    #[must_use]
    pub const fn fact_id(&self) -> &[u8; 32] {
        &self.fact_id
    }

    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
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
    pub const fn envelope(&self) -> PolymarketEvidenceEnvelopeV2 {
        self.envelope
    }

    #[must_use]
    pub fn elements(&self) -> &[PolymarketUserEvidenceElementV2] {
        &self.elements
    }

    /// Reconstructs credential-free normalized messages for the existing lifecycle reducer.
    ///
    /// Credential ownership is represented only by fixed non-secret relation markers. This is an
    /// internal compatibility seam for live/recovery dispatch, not a wire or persistence format.
    pub(crate) fn to_dispatch_messages(
        &self,
    ) -> Result<Vec<UserWsMessage>, PolymarketEvidenceV2Error> {
        let mut messages = Vec::new();
        messages
            .try_reserve_exact(self.elements.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for element in &self.elements {
            messages.push(match element {
                PolymarketUserEvidenceElementV2::Order(order) => {
                    UserWsMessage::Order(order.to_dispatch_message()?)
                }
                PolymarketUserEvidenceElementV2::Trade(trade) => {
                    UserWsMessage::Trade(trade.to_dispatch_message()?)
                }
            });
        }
        Ok(messages)
    }
}

pub(crate) const fn dispatch_api_key_marker() -> &'static str {
    OWNED_RELATION_MARKER
}

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum PolymarketEvidenceV2Error {
    #[error("authenticated user frame is invalid")]
    InvalidFrame,
    #[error("authenticated user frame JSON is not strict V2 wire data")]
    InvalidJson,
    #[error("authenticated user frame contains an invalid retained field")]
    InvalidField,
    #[error("authenticated user frame contains an invalid decimal")]
    InvalidDecimal,
    #[error("authenticated user frame exceeds a hard capacity")]
    Capacity,
    #[error("credential-free authenticated user frame encoding is invalid")]
    InvalidEncoding,
    #[error("credential-free authenticated user frame identity does not match its content")]
    FactIdMismatch,
}

#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum WireUserMessage {
    #[serde(rename = "order")]
    Order(WireOrder),
    #[serde(rename = "trade")]
    Trade(WireTrade),
}

impl WireUserMessage {
    fn project(
        self,
        account_address: &str,
        api_key: &str,
    ) -> Result<PolymarketUserEvidenceElementV2, PolymarketEvidenceV2Error> {
        match self {
            Self::Order(order) => order.project(account_address, api_key),
            Self::Trade(trade) => trade.project(account_address, api_key),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOrder {
    asset_id: String,
    #[serde(default)]
    associate_trades: Option<BoundedVec<String, MAX_NESTED>>,
    created_at: String,
    expiration: Option<String>,
    id: String,
    maker_address: String,
    market: String,
    order_owner: String,
    order_type: String,
    original_size: String,
    outcome: String,
    owner: String,
    price: String,
    side: String,
    size_matched: String,
    status: String,
    timestamp: String,
    #[serde(rename = "type")]
    event: String,
}

impl WireOrder {
    fn project(
        self,
        account_address: &str,
        api_key: &str,
    ) -> Result<PolymarketUserEvidenceElementV2, PolymarketEvidenceV2Error> {
        let associated = self
            .associate_trades
            .map(BoundedVec::into_vec)
            .unwrap_or_default();
        validate_many([
            self.asset_id.as_str(),
            self.id.as_str(),
            self.maker_address.as_str(),
            self.market.as_str(),
            self.timestamp.as_str(),
            self.owner.as_str(),
            self.order_owner.as_str(),
        ])?;
        validate_unsigned(&self.timestamp)?;
        for trade in &associated {
            validate_text(trade)?;
        }
        let created_at = optional_nonempty(self.created_at);
        if let Some(value) = created_at.as_deref() {
            validate_unsigned(value)?;
        }
        let expiration = self.expiration.and_then(optional_nonempty);
        if let Some(value) = expiration.as_deref() {
            validate_unsigned(value)?;
        }
        let outcome = validated_optional(self.outcome)?;
        let (status, status_detail) = parse_order_status(&self.status)?;
        let event = parse_event(&self.event)?;
        if event == PolymarketEvidenceEventV2::Trade {
            return Err(PolymarketEvidenceV2Error::InvalidField);
        }
        Ok(PolymarketUserEvidenceElementV2::Order(
            PolymarketOrderEvidenceV2 {
                asset_id: self.asset_id,
                associated_trades: associated.into_boxed_slice(),
                created_at,
                expiration,
                order_id: self.id,
                maker_address: self.maker_address.clone(),
                market: self.market,
                ownership: PolymarketOrderOwnershipV2 {
                    maker_address_matches_account: self.maker_address == account_address,
                    owner_matches_api_key: self.owner == api_key,
                    order_owner_matches_api_key: self.order_owner == api_key,
                },
                order_type: parse_order_type(&self.order_type)?,
                original_size: PolymarketWireDecimalV2::parse(&self.original_size)?,
                outcome,
                price: PolymarketWireDecimalV2::parse(&self.price)?,
                side: parse_side(&self.side)?,
                size_matched: if self.size_matched.is_empty() {
                    PolymarketWireDecimalV2::zero()
                } else {
                    PolymarketWireDecimalV2::parse(&self.size_matched)?
                },
                status,
                status_detail,
                timestamp: self.timestamp,
                event,
            },
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTrade {
    asset_id: String,
    bucket_index: u64,
    #[serde(default)]
    fee_rate_bps: Option<String>,
    id: String,
    last_update: String,
    maker_address: String,
    maker_orders: BoundedVec<WireMaker, MAX_NESTED>,
    market: String,
    #[serde(alias = "matchtime")]
    match_time: String,
    outcome: String,
    owner: String,
    price: String,
    side: String,
    size: String,
    status: String,
    taker_order_id: String,
    timestamp: String,
    trade_owner: String,
    #[serde(default)]
    transaction_hash: Option<String>,
    trader_side: String,
    #[serde(rename = "type")]
    event: String,
}

impl WireTrade {
    fn project(
        self,
        account_address: &str,
        api_key: &str,
    ) -> Result<PolymarketUserEvidenceElementV2, PolymarketEvidenceV2Error> {
        let maker_orders = self.maker_orders.into_vec();
        validate_many([
            self.asset_id.as_str(),
            self.id.as_str(),
            self.last_update.as_str(),
            self.maker_address.as_str(),
            self.market.as_str(),
            self.match_time.as_str(),
            self.taker_order_id.as_str(),
            self.timestamp.as_str(),
            self.owner.as_str(),
            self.trade_owner.as_str(),
        ])?;
        validate_unsigned(&self.timestamp)?;
        let event = parse_event(&self.event)?;
        if event != PolymarketEvidenceEventV2::Trade {
            return Err(PolymarketEvidenceV2Error::InvalidField);
        }
        let mut makers = Vec::new();
        makers
            .try_reserve_exact(maker_orders.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for maker in maker_orders {
            makers.push(maker.project(account_address, api_key)?);
        }
        let outcome = validated_optional(self.outcome)?;
        let transaction_hash = match self.transaction_hash {
            Some(value) => validated_optional(value)?,
            None => None,
        };
        Ok(PolymarketUserEvidenceElementV2::Trade(
            PolymarketTradeEvidenceV2 {
                asset_id: self.asset_id,
                bucket_index: self.bucket_index,
                fee_rate_bps: parse_optional_decimal(self.fee_rate_bps)?,
                trade_id: self.id,
                last_update: self.last_update,
                maker_address: self.maker_address.clone(),
                maker_rows: makers.into_boxed_slice(),
                market: self.market,
                match_time: self.match_time,
                outcome,
                ownership: PolymarketTradeOwnershipV2 {
                    maker_address_matches_account: self.maker_address == account_address,
                    owner_matches_api_key: self.owner == api_key,
                    trade_owner_matches_api_key: self.trade_owner == api_key,
                },
                price: PolymarketWireDecimalV2::parse(&self.price)?,
                side: parse_side(&self.side)?,
                size: PolymarketWireDecimalV2::parse(&self.size)?,
                status: parse_trade_status(&self.status)?,
                taker_order_id: self.taker_order_id,
                timestamp: self.timestamp,
                transaction_hash,
                role: parse_role(&self.trader_side)?,
                event,
            },
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireMaker {
    asset_id: String,
    #[serde(default)]
    fee_rate_bps: Option<String>,
    maker_address: String,
    matched_amount: String,
    order_id: String,
    outcome: String,
    owner: String,
    price: String,
    #[serde(default)]
    side: Option<String>,
}

impl WireMaker {
    fn project(
        self,
        account_address: &str,
        api_key: &str,
    ) -> Result<PolymarketMakerEvidenceV2, PolymarketEvidenceV2Error> {
        validate_many([
            self.asset_id.as_str(),
            self.maker_address.as_str(),
            self.order_id.as_str(),
            self.owner.as_str(),
        ])?;
        let outcome = validated_optional(self.outcome)?;
        Ok(PolymarketMakerEvidenceV2 {
            asset_id: self.asset_id,
            maker_address: self.maker_address.clone(),
            matched_amount: PolymarketWireDecimalV2::parse(&self.matched_amount)?,
            order_id: self.order_id,
            outcome,
            ownership: PolymarketMakerOwnershipV2 {
                maker_address_matches_account: self.maker_address == account_address,
                owner_matches_api_key: self.owner == api_key,
            },
            price: PolymarketWireDecimalV2::parse(&self.price)?,
            side: self.side.as_deref().map(parse_side).transpose()?,
            fee_rate_bps: parse_optional_decimal(self.fee_rate_bps)?,
        })
    }
}

fn optional_nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn validated_optional(value: String) -> Result<Option<String>, PolymarketEvidenceV2Error> {
    let value = optional_nonempty(value);
    if let Some(value) = value.as_deref() {
        validate_text(value)?;
    }
    Ok(value)
}

fn parse_optional_decimal(
    value: Option<String>,
) -> Result<Option<PolymarketWireDecimalV2>, PolymarketEvidenceV2Error> {
    value
        .and_then(optional_nonempty)
        .as_deref()
        .map(PolymarketWireDecimalV2::parse)
        .transpose()
}

fn validate_text(value: &str) -> Result<(), PolymarketEvidenceV2Error> {
    if value.is_empty() || value.len() > MAX_TEXT {
        Err(PolymarketEvidenceV2Error::InvalidField)
    } else {
        Ok(())
    }
}

fn validate_many<const N: usize>(values: [&str; N]) -> Result<(), PolymarketEvidenceV2Error> {
    values.into_iter().try_for_each(validate_text)
}

fn validate_unsigned(value: &str) -> Result<(), PolymarketEvidenceV2Error> {
    validate_text(value)?;
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        Ok(())
    } else {
        Err(PolymarketEvidenceV2Error::InvalidField)
    }
}

fn parse_side(value: &str) -> Result<PolymarketEvidenceSideV2, PolymarketEvidenceV2Error> {
    match value {
        "BUY" => Ok(PolymarketEvidenceSideV2::Buy),
        "SELL" => Ok(PolymarketEvidenceSideV2::Sell),
        _ => Err(PolymarketEvidenceV2Error::InvalidField),
    }
}

fn parse_role(value: &str) -> Result<PolymarketEvidenceRoleV2, PolymarketEvidenceV2Error> {
    match value {
        "MAKER" => Ok(PolymarketEvidenceRoleV2::Maker),
        "TAKER" => Ok(PolymarketEvidenceRoleV2::Taker),
        _ => Err(PolymarketEvidenceV2Error::InvalidField),
    }
}

fn parse_event(value: &str) -> Result<PolymarketEvidenceEventV2, PolymarketEvidenceV2Error> {
    match value {
        "PLACEMENT" => Ok(PolymarketEvidenceEventV2::Placement),
        "UPDATE" => Ok(PolymarketEvidenceEventV2::Update),
        "CANCELLATION" => Ok(PolymarketEvidenceEventV2::Cancellation),
        "TRADE" => Ok(PolymarketEvidenceEventV2::Trade),
        _ => Err(PolymarketEvidenceV2Error::InvalidField),
    }
}

fn parse_order_type(
    value: &str,
) -> Result<PolymarketEvidenceOrderTypeV2, PolymarketEvidenceV2Error> {
    match value {
        "FOK" => Ok(PolymarketEvidenceOrderTypeV2::Fok),
        "FAK" => Ok(PolymarketEvidenceOrderTypeV2::Fak),
        "GTC" => Ok(PolymarketEvidenceOrderTypeV2::Gtc),
        "GTD" => Ok(PolymarketEvidenceOrderTypeV2::Gtd),
        _ => Err(PolymarketEvidenceV2Error::InvalidField),
    }
}

fn parse_order_status(
    value: &str,
) -> Result<(PolymarketEvidenceOrderStatusV2, Option<String>), PolymarketEvidenceV2Error> {
    const STATUSES: &[(&str, PolymarketEvidenceOrderStatusV2)] = &[
        (
            "CANCELED_MARKET_RESOLVED",
            PolymarketEvidenceOrderStatusV2::CanceledMarketResolved,
        ),
        ("INVALID", PolymarketEvidenceOrderStatusV2::Invalid),
        ("LIVE", PolymarketEvidenceOrderStatusV2::Live),
        ("DELAYED", PolymarketEvidenceOrderStatusV2::Delayed),
        ("MATCHED", PolymarketEvidenceOrderStatusV2::Matched),
        ("UNMATCHED", PolymarketEvidenceOrderStatusV2::Unmatched),
        ("CANCELED", PolymarketEvidenceOrderStatusV2::Canceled),
    ];
    for (prefix, status) in STATUSES {
        if value == *prefix {
            return Ok((*status, None));
        }
        if let Some(detail) = value
            .strip_prefix(prefix)
            .and_then(|suffix| suffix.strip_prefix('_'))
            && !detail.is_empty()
        {
            validate_text(detail)?;
            return Ok((*status, Some(detail.to_owned())));
        }
    }
    Err(PolymarketEvidenceV2Error::InvalidField)
}

fn parse_trade_status(
    value: &str,
) -> Result<PolymarketEvidenceTradeStatusV2, PolymarketEvidenceV2Error> {
    match value {
        "MATCHED_NOT_BROADCASTED" => Ok(PolymarketEvidenceTradeStatusV2::MatchedNotBroadcasted),
        "MATCHED" => Ok(PolymarketEvidenceTradeStatusV2::Matched),
        "MINED" => Ok(PolymarketEvidenceTradeStatusV2::Mined),
        "CONFIRMED" => Ok(PolymarketEvidenceTradeStatusV2::Confirmed),
        "RETRYING" => Ok(PolymarketEvidenceTradeStatusV2::Retrying),
        "FAILED" => Ok(PolymarketEvidenceTradeStatusV2::Failed),
        _ => Err(PolymarketEvidenceV2Error::InvalidField),
    }
}

fn encode_body(
    session_epoch: u64,
    frame_sequence: u64,
    envelope: PolymarketEvidenceEnvelopeV2,
    elements: &[PolymarketUserEvidenceElementV2],
) -> Result<Vec<u8>, PolymarketEvidenceV2Error> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(32)
        .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
    output.push(2);
    output.extend_from_slice(&session_epoch.to_be_bytes());
    output.extend_from_slice(&frame_sequence.to_be_bytes());
    output.push(envelope as u8);
    output.extend_from_slice(
        &u16::try_from(elements.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?
            .to_be_bytes(),
    );
    for element in elements {
        match element {
            PolymarketUserEvidenceElementV2::Order(order) => encode_order(&mut output, order)?,
            PolymarketUserEvidenceElementV2::Trade(trade) => encode_trade(&mut output, trade)?,
        }
        if output.len() > MAX_FRAME_BYTES - 32 {
            return Err(PolymarketEvidenceV2Error::Capacity);
        }
    }
    Ok(output)
}

fn encode_order(
    output: &mut Vec<u8>,
    order: &PolymarketOrderEvidenceV2,
) -> Result<(), PolymarketEvidenceV2Error> {
    output.push(1);
    put_text(output, &order.asset_id)?;
    put_texts(output, &order.associated_trades)?;
    put_optional_text(output, order.created_at.as_deref())?;
    put_optional_text(output, order.expiration.as_deref())?;
    put_text(output, &order.order_id)?;
    put_text(output, &order.maker_address)?;
    put_text(output, &order.market)?;
    output.push(
        u8::from(order.ownership.maker_address_matches_account)
            | (u8::from(order.ownership.owner_matches_api_key) << 1)
            | (u8::from(order.ownership.order_owner_matches_api_key) << 2),
    );
    output.push(order.order_type as u8);
    put_decimal(output, order.original_size);
    put_optional_text(output, order.outcome.as_deref())?;
    put_decimal(output, order.price);
    output.push(order.side as u8);
    put_decimal(output, order.size_matched);
    output.push(order.status as u8);
    put_optional_text(output, order.status_detail.as_deref())?;
    put_text(output, &order.timestamp)?;
    output.push(order.event as u8);
    Ok(())
}

fn encode_trade(
    output: &mut Vec<u8>,
    trade: &PolymarketTradeEvidenceV2,
) -> Result<(), PolymarketEvidenceV2Error> {
    output.push(2);
    put_text(output, &trade.asset_id)?;
    output.extend_from_slice(&trade.bucket_index.to_be_bytes());
    put_optional_decimal(output, trade.fee_rate_bps);
    put_text(output, &trade.trade_id)?;
    put_text(output, &trade.last_update)?;
    put_text(output, &trade.maker_address)?;
    output.extend_from_slice(
        &u16::try_from(trade.maker_rows.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?
            .to_be_bytes(),
    );
    for maker in &trade.maker_rows {
        encode_maker(output, maker)?;
    }
    put_text(output, &trade.market)?;
    put_text(output, &trade.match_time)?;
    put_optional_text(output, trade.outcome.as_deref())?;
    output.push(
        u8::from(trade.ownership.maker_address_matches_account)
            | (u8::from(trade.ownership.owner_matches_api_key) << 1)
            | (u8::from(trade.ownership.trade_owner_matches_api_key) << 2),
    );
    put_decimal(output, trade.price);
    output.push(trade.side as u8);
    put_decimal(output, trade.size);
    output.push(trade.status as u8);
    put_text(output, &trade.taker_order_id)?;
    put_text(output, &trade.timestamp)?;
    put_optional_text(output, trade.transaction_hash.as_deref())?;
    output.push(trade.role as u8);
    output.push(trade.event as u8);
    Ok(())
}

fn encode_maker(
    output: &mut Vec<u8>,
    maker: &PolymarketMakerEvidenceV2,
) -> Result<(), PolymarketEvidenceV2Error> {
    put_text(output, &maker.asset_id)?;
    put_text(output, &maker.maker_address)?;
    put_decimal(output, maker.matched_amount);
    put_text(output, &maker.order_id)?;
    put_optional_text(output, maker.outcome.as_deref())?;
    output.push(
        u8::from(maker.ownership.maker_address_matches_account)
            | (u8::from(maker.ownership.owner_matches_api_key) << 1),
    );
    put_decimal(output, maker.price);
    match maker.side {
        Some(side) => {
            output.push(1);
            output.push(side as u8);
        }
        None => output.push(0),
    }
    put_optional_decimal(output, maker.fee_rate_bps);
    Ok(())
}

fn put_text(output: &mut Vec<u8>, value: &str) -> Result<(), PolymarketEvidenceV2Error> {
    output.extend_from_slice(
        &u16::try_from(value.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?
            .to_be_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_texts(output: &mut Vec<u8>, values: &[String]) -> Result<(), PolymarketEvidenceV2Error> {
    output.extend_from_slice(
        &u16::try_from(values.len())
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?
            .to_be_bytes(),
    );
    values.iter().try_for_each(|value| put_text(output, value))
}

fn put_optional_text(
    output: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), PolymarketEvidenceV2Error> {
    match value {
        Some(value) => {
            output.push(1);
            put_text(output, value)
        }
        None => {
            output.push(0);
            Ok(())
        }
    }
}

fn put_decimal(output: &mut Vec<u8>, value: PolymarketWireDecimalV2) {
    output.extend_from_slice(&value.mantissa.to_be_bytes());
    output.push(value.scale);
}

fn put_optional_decimal(output: &mut Vec<u8>, value: Option<PolymarketWireDecimalV2>) {
    match value {
        Some(value) => {
            output.push(1);
            put_decimal(output, value);
        }
        None => output.push(0),
    }
}

fn decode_envelope(value: u8) -> Result<PolymarketEvidenceEnvelopeV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceEnvelopeV2::Single),
        2 => Ok(PolymarketEvidenceEnvelopeV2::Batch),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_element(
    reader: &mut CanonicalReader<'_>,
) -> Result<PolymarketUserEvidenceElementV2, PolymarketEvidenceV2Error> {
    match reader.byte()? {
        1 => {
            let order = PolymarketOrderEvidenceV2 {
                asset_id: reader.text()?,
                associated_trades: reader.texts()?.into_boxed_slice(),
                created_at: reader.optional_text()?,
                expiration: reader.optional_text()?,
                order_id: reader.text()?,
                maker_address: reader.text()?,
                market: reader.text()?,
                ownership: decode_order_ownership(reader.byte()?)?,
                order_type: decode_order_type(reader.byte()?)?,
                original_size: reader.decimal()?,
                outcome: reader.optional_text()?,
                price: reader.decimal()?,
                side: decode_side(reader.byte()?)?,
                size_matched: reader.decimal()?,
                status: decode_order_status(reader.byte()?)?,
                status_detail: reader.optional_text()?,
                timestamp: reader.text()?,
                event: decode_event(reader.byte()?)?,
            };
            validate_unsigned(&order.timestamp)?;
            if let Some(value) = order.created_at.as_deref() {
                validate_unsigned(value)?;
            }
            if let Some(value) = order.expiration.as_deref() {
                validate_unsigned(value)?;
            }
            if order.event == PolymarketEvidenceEventV2::Trade {
                return Err(PolymarketEvidenceV2Error::InvalidEncoding);
            }
            Ok(PolymarketUserEvidenceElementV2::Order(order))
        }
        2 => {
            let asset_id = reader.text()?;
            let bucket_index = reader.u64()?;
            let fee_rate_bps = reader.optional_decimal()?;
            let trade_id = reader.text()?;
            let last_update = reader.text()?;
            let maker_address = reader.text()?;
            let maker_count = usize::from(reader.u16()?);
            if maker_count > MAX_NESTED {
                return Err(PolymarketEvidenceV2Error::Capacity);
            }
            let mut maker_rows = Vec::new();
            maker_rows
                .try_reserve_exact(maker_count)
                .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
            for _ in 0..maker_count {
                maker_rows.push(decode_maker(reader)?);
            }
            let trade = PolymarketTradeEvidenceV2 {
                asset_id,
                bucket_index,
                fee_rate_bps,
                trade_id,
                last_update,
                maker_address,
                maker_rows: maker_rows.into_boxed_slice(),
                market: reader.text()?,
                match_time: reader.text()?,
                outcome: reader.optional_text()?,
                ownership: decode_trade_ownership(reader.byte()?)?,
                price: reader.decimal()?,
                side: decode_side(reader.byte()?)?,
                size: reader.decimal()?,
                status: decode_trade_status(reader.byte()?)?,
                taker_order_id: reader.text()?,
                timestamp: reader.text()?,
                transaction_hash: reader.optional_text()?,
                role: decode_role(reader.byte()?)?,
                event: decode_event(reader.byte()?)?,
            };
            validate_unsigned(&trade.timestamp)?;
            if trade.event != PolymarketEvidenceEventV2::Trade {
                return Err(PolymarketEvidenceV2Error::InvalidEncoding);
            }
            Ok(PolymarketUserEvidenceElementV2::Trade(trade))
        }
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_maker(
    reader: &mut CanonicalReader<'_>,
) -> Result<PolymarketMakerEvidenceV2, PolymarketEvidenceV2Error> {
    Ok(PolymarketMakerEvidenceV2 {
        asset_id: reader.text()?,
        maker_address: reader.text()?,
        matched_amount: reader.decimal()?,
        order_id: reader.text()?,
        outcome: reader.optional_text()?,
        ownership: decode_maker_ownership(reader.byte()?)?,
        price: reader.decimal()?,
        side: reader.optional_side()?,
        fee_rate_bps: reader.optional_decimal()?,
    })
}

fn decode_side(value: u8) -> Result<PolymarketEvidenceSideV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceSideV2::Buy),
        2 => Ok(PolymarketEvidenceSideV2::Sell),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_role(value: u8) -> Result<PolymarketEvidenceRoleV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceRoleV2::Maker),
        2 => Ok(PolymarketEvidenceRoleV2::Taker),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_event(value: u8) -> Result<PolymarketEvidenceEventV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceEventV2::Placement),
        2 => Ok(PolymarketEvidenceEventV2::Update),
        3 => Ok(PolymarketEvidenceEventV2::Cancellation),
        4 => Ok(PolymarketEvidenceEventV2::Trade),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_order_type(
    value: u8,
) -> Result<PolymarketEvidenceOrderTypeV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceOrderTypeV2::Fok),
        2 => Ok(PolymarketEvidenceOrderTypeV2::Fak),
        3 => Ok(PolymarketEvidenceOrderTypeV2::Gtc),
        4 => Ok(PolymarketEvidenceOrderTypeV2::Gtd),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_order_status(
    value: u8,
) -> Result<PolymarketEvidenceOrderStatusV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceOrderStatusV2::Invalid),
        2 => Ok(PolymarketEvidenceOrderStatusV2::Live),
        3 => Ok(PolymarketEvidenceOrderStatusV2::Delayed),
        4 => Ok(PolymarketEvidenceOrderStatusV2::Matched),
        5 => Ok(PolymarketEvidenceOrderStatusV2::Unmatched),
        6 => Ok(PolymarketEvidenceOrderStatusV2::Canceled),
        7 => Ok(PolymarketEvidenceOrderStatusV2::CanceledMarketResolved),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_trade_status(
    value: u8,
) -> Result<PolymarketEvidenceTradeStatusV2, PolymarketEvidenceV2Error> {
    match value {
        1 => Ok(PolymarketEvidenceTradeStatusV2::MatchedNotBroadcasted),
        2 => Ok(PolymarketEvidenceTradeStatusV2::Matched),
        3 => Ok(PolymarketEvidenceTradeStatusV2::Mined),
        4 => Ok(PolymarketEvidenceTradeStatusV2::Confirmed),
        5 => Ok(PolymarketEvidenceTradeStatusV2::Retrying),
        6 => Ok(PolymarketEvidenceTradeStatusV2::Failed),
        _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
    }
}

fn decode_order_ownership(
    value: u8,
) -> Result<PolymarketOrderOwnershipV2, PolymarketEvidenceV2Error> {
    if value & !0b111 != 0 {
        return Err(PolymarketEvidenceV2Error::InvalidEncoding);
    }
    Ok(PolymarketOrderOwnershipV2 {
        maker_address_matches_account: value & 1 != 0,
        owner_matches_api_key: value & 2 != 0,
        order_owner_matches_api_key: value & 4 != 0,
    })
}

fn decode_trade_ownership(
    value: u8,
) -> Result<PolymarketTradeOwnershipV2, PolymarketEvidenceV2Error> {
    if value & !0b111 != 0 {
        return Err(PolymarketEvidenceV2Error::InvalidEncoding);
    }
    Ok(PolymarketTradeOwnershipV2 {
        maker_address_matches_account: value & 1 != 0,
        owner_matches_api_key: value & 2 != 0,
        trade_owner_matches_api_key: value & 4 != 0,
    })
}

fn decode_maker_ownership(
    value: u8,
) -> Result<PolymarketMakerOwnershipV2, PolymarketEvidenceV2Error> {
    if value & !0b11 != 0 {
        return Err(PolymarketEvidenceV2Error::InvalidEncoding);
    }
    Ok(PolymarketMakerOwnershipV2 {
        maker_address_matches_account: value & 1 != 0,
        owner_matches_api_key: value & 2 != 0,
    })
}

struct CanonicalReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> CanonicalReader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PolymarketEvidenceV2Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PolymarketEvidenceV2Error::Capacity)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(PolymarketEvidenceV2Error::InvalidEncoding)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, PolymarketEvidenceV2Error> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(PolymarketEvidenceV2Error::InvalidEncoding)
    }

    fn u16(&mut self) -> Result<u16, PolymarketEvidenceV2Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, PolymarketEvidenceV2Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)?,
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PolymarketEvidenceV2Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)
    }

    fn text(&mut self) -> Result<String, PolymarketEvidenceV2Error> {
        let length = usize::from(self.u16()?);
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)?;
        validate_text(value)?;
        Ok(value.to_owned())
    }

    fn texts(&mut self) -> Result<Vec<String>, PolymarketEvidenceV2Error> {
        let count = usize::from(self.u16()?);
        if count > MAX_NESTED {
            return Err(PolymarketEvidenceV2Error::Capacity);
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| PolymarketEvidenceV2Error::Capacity)?;
        for _ in 0..count {
            values.push(self.text()?);
        }
        Ok(values)
    }

    fn optional_text(&mut self) -> Result<Option<String>, PolymarketEvidenceV2Error> {
        match self.byte()? {
            0 => Ok(None),
            1 => self.text().map(Some),
            _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
        }
    }

    fn decimal(&mut self) -> Result<PolymarketWireDecimalV2, PolymarketEvidenceV2Error> {
        let mantissa = u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| PolymarketEvidenceV2Error::InvalidEncoding)?,
        );
        let scale = self.byte()?;
        if scale > 38 || (mantissa == 0 && scale != 0) || (scale > 0 && mantissa.is_multiple_of(10))
        {
            return Err(PolymarketEvidenceV2Error::InvalidEncoding);
        }
        Ok(PolymarketWireDecimalV2 { mantissa, scale })
    }

    fn optional_decimal(
        &mut self,
    ) -> Result<Option<PolymarketWireDecimalV2>, PolymarketEvidenceV2Error> {
        match self.byte()? {
            0 => Ok(None),
            1 => self.decimal().map(Some),
            _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
        }
    }

    fn optional_side(
        &mut self,
    ) -> Result<Option<PolymarketEvidenceSideV2>, PolymarketEvidenceV2Error> {
        match self.byte()? {
            0 => Ok(None),
            1 => decode_side(self.byte()?).map(Some),
            _ => Err(PolymarketEvidenceV2Error::InvalidEncoding),
        }
    }

    fn finish(self) -> Result<(), PolymarketEvidenceV2Error> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(PolymarketEvidenceV2Error::InvalidEncoding)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
    const API_KEY: &str = "00000000-0000-0000-0000-000000000001";

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("test_data/{name}"))
            .unwrap_or_else(|error| panic!("failed to load {name}: {error}"))
    }

    #[test]
    fn projects_single_and_batch_without_retaining_credential_bytes() {
        let single = PolymarketAuthenticatedUserFrameV2::project(
            &fixture("ws_user_order_msg.json"),
            9,
            1,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        assert_eq!(single.envelope(), PolymarketEvidenceEnvelopeV2::Single);
        assert_eq!(single.elements().len(), 1);
        assert_eq!(
            PolymarketAuthenticatedUserFrameV2::try_restore(single.canonical_bytes()).unwrap(),
            single
        );
        assert_eq!(
            single.fact_id(),
            &[
                208, 16, 62, 208, 126, 214, 240, 51, 235, 129, 230, 162, 65, 237, 50, 45, 100, 84,
                3, 19, 198, 100, 49, 40, 115, 43, 240, 221, 49, 49, 106, 112,
            ]
        );
        let batch = PolymarketAuthenticatedUserFrameV2::project(
            &fixture("ws_user_batch_msg.json"),
            9,
            2,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        assert_eq!(batch.envelope(), PolymarketEvidenceEnvelopeV2::Batch);
        assert_eq!(batch.elements().len(), 2);
        assert!(
            !batch
                .canonical_bytes()
                .windows(API_KEY.len())
                .any(|window| window == API_KEY.as_bytes())
        );
        assert!(!format!("{batch:?}").contains(API_KEY));
        let restored =
            PolymarketAuthenticatedUserFrameV2::try_restore(batch.canonical_bytes()).unwrap();
        assert_eq!(restored, batch);
        assert_eq!(
            restored.to_dispatch_messages(),
            batch.to_dispatch_messages()
        );
        let mut tampered = batch.canonical_bytes().to_vec();
        tampered[0] ^= 1;
        assert!(PolymarketAuthenticatedUserFrameV2::try_restore(&tampered).is_err());
    }

    #[test]
    fn fok_empty_sentinels_are_explicit_and_status_detail_is_retained() {
        let frame = PolymarketAuthenticatedUserFrameV2::project(
            &fixture("ws_user_order_fok_killed.json"),
            7,
            4,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        let PolymarketUserEvidenceElementV2::Order(order) = &frame.elements()[0] else {
            panic!("expected order");
        };
        assert_eq!(order.size_matched, PolymarketWireDecimalV2::zero());
        assert!(order.created_at.is_none());
        assert!(order.outcome.is_none());
        assert_eq!(order.status, PolymarketEvidenceOrderStatusV2::Canceled);
        assert_eq!(
            order.status_detail.as_deref(),
            Some("order couldn't be fully filled. FOK orders are fully filled or killed.")
        );
    }

    #[test]
    fn decimal_normalization_and_strict_batch_validation_are_atomic() {
        assert_eq!(
            PolymarketWireDecimalV2::parse("0.100000000000000000000000000000000000000").unwrap(),
            PolymarketWireDecimalV2 {
                mantissa: 1,
                scale: 1,
            }
        );
        assert!(
            PolymarketWireDecimalV2::parse("0.000000000000000000000000000000000000001").is_err()
        );
        assert!(serde_json::from_str::<BoundedVec<u8, 2>>("[1,2,3]").is_err());

        let mut value: serde_json::Value =
            serde_json::from_str(&fixture("ws_user_trade.json")).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("event_type".into(), serde_json::json!("trade"));
        let raw = serde_json::to_string(&value).unwrap();
        let normalized = PolymarketAuthenticatedUserFrameV2::project(
            &raw.replace("25.0000", "25.0").replace("0.5000", "0.5"),
            3,
            1,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        let original =
            PolymarketAuthenticatedUserFrameV2::project(&raw, 3, 1, ACCOUNT, API_KEY).unwrap();
        assert_eq!(original.fact_id(), normalized.fact_id());

        let malformed = format!("[{raw},{{\"event_type\":\"trade\",\"unknown\":1}}]");
        assert!(
            PolymarketAuthenticatedUserFrameV2::project(&malformed, 3, 2, ACCOUNT, API_KEY)
                .is_err()
        );
    }

    #[test]
    fn lifecycle_compatibility_rejects_v2_decimal_outside_rust_decimal_range() {
        let mut value: serde_json::Value =
            serde_json::from_str(&fixture("ws_user_trade_msg.json")).unwrap();
        value["size"] = serde_json::json!("123456789012345678901234567890");
        let raw = serde_json::to_string(&value).unwrap();
        let frame =
            PolymarketAuthenticatedUserFrameV2::project(&raw, 4, 1, ACCOUNT, API_KEY).unwrap();

        assert!(frame.to_dispatch_messages().is_err());
    }

    #[test]
    fn every_trade_status_and_single_element_batch_are_identity_bound() {
        let original: serde_json::Value =
            serde_json::from_str(&fixture("ws_user_trade_msg.json")).unwrap();
        for (wire, expected) in [
            (
                "MATCHED_NOT_BROADCASTED",
                PolymarketEvidenceTradeStatusV2::MatchedNotBroadcasted,
            ),
            ("MATCHED", PolymarketEvidenceTradeStatusV2::Matched),
            ("MINED", PolymarketEvidenceTradeStatusV2::Mined),
            ("CONFIRMED", PolymarketEvidenceTradeStatusV2::Confirmed),
            ("RETRYING", PolymarketEvidenceTradeStatusV2::Retrying),
            ("FAILED", PolymarketEvidenceTradeStatusV2::Failed),
        ] {
            let mut value = original.clone();
            value["status"] = serde_json::json!(wire);
            let raw = serde_json::to_string(&value).unwrap();
            let frame =
                PolymarketAuthenticatedUserFrameV2::project(&raw, 2, 1, ACCOUNT, API_KEY).unwrap();
            let PolymarketUserEvidenceElementV2::Trade(trade) = &frame.elements()[0] else {
                panic!("expected trade");
            };
            assert_eq!(trade.status(), expected);
        }

        let raw = fixture("ws_user_trade_msg.json");
        let single =
            PolymarketAuthenticatedUserFrameV2::project(&raw, 2, 1, ACCOUNT, API_KEY).unwrap();
        let batch = PolymarketAuthenticatedUserFrameV2::project(
            &format!("[{raw}]"),
            2,
            1,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        assert_ne!(single.fact_id(), batch.fact_id());
    }

    #[test]
    fn current_user_channel_matchtime_alias_has_the_same_canonical_identity() {
        let raw = fixture("ws_user_trade_msg.json");
        let legacy =
            PolymarketAuthenticatedUserFrameV2::project(&raw, 2, 1, ACCOUNT, API_KEY).unwrap();

        let mut current: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let match_time = current
            .as_object_mut()
            .unwrap()
            .remove("match_time")
            .unwrap();
        current["matchtime"] = match_time;
        let current = PolymarketAuthenticatedUserFrameV2::project(
            &serde_json::to_string(&current).unwrap(),
            2,
            1,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();

        assert_eq!(current.fact_id(), legacy.fact_id());
        assert_eq!(current.canonical_bytes(), legacy.canonical_bytes());
        assert_eq!(
            current.to_dispatch_messages(),
            legacy.to_dispatch_messages()
        );
    }

    #[test]
    fn maker_fee_and_each_ownership_relation_are_retained_without_credentials() {
        let mut value: serde_json::Value =
            serde_json::from_str(&fixture("ws_user_trade.json")).unwrap();
        value["event_type"] = serde_json::json!("trade");
        value["maker_orders"][0]["maker_address"] = serde_json::json!(ACCOUNT);
        value["maker_orders"][0]["owner"] = serde_json::json!(API_KEY);
        value["maker_orders"][0]["fee_rate_bps"] = serde_json::json!("12.500");
        let raw = serde_json::to_string(&value).unwrap();
        let frame =
            PolymarketAuthenticatedUserFrameV2::project(&raw, 5, 8, ACCOUNT, API_KEY).unwrap();
        assert_eq!(
            frame.fact_id(),
            &[
                157, 246, 141, 18, 82, 78, 37, 47, 236, 77, 148, 95, 129, 235, 156, 114, 210, 255,
                6, 39, 130, 255, 230, 128, 34, 81, 120, 116, 89, 56, 10, 223,
            ]
        );
        let PolymarketUserEvidenceElementV2::Trade(trade) = &frame.elements()[0] else {
            panic!("expected trade");
        };
        assert!(trade.ownership().maker_address_matches_account());
        assert!(trade.ownership().owner_matches_api_key());
        assert!(trade.ownership().trade_owner_matches_api_key());
        let maker = &trade.maker_rows()[0];
        assert!(maker.ownership().maker_address_matches_account());
        assert!(maker.ownership().owner_matches_api_key());
        assert_eq!(
            maker.fee_rate_bps(),
            Some(PolymarketWireDecimalV2 {
                mantissa: 125,
                scale: 1,
            })
        );
        assert!(
            !frame
                .canonical_bytes()
                .windows(API_KEY.len())
                .any(|window| window == API_KEY.as_bytes())
        );

        value["maker_orders"][0]["fee_rate_bps"] = serde_json::json!("");
        let empty_fee = PolymarketAuthenticatedUserFrameV2::project(
            &serde_json::to_string(&value).unwrap(),
            5,
            9,
            ACCOUNT,
            API_KEY,
        )
        .unwrap();
        let PolymarketUserEvidenceElementV2::Trade(trade) = &empty_fee.elements()[0] else {
            panic!("expected trade");
        };
        assert_eq!(trade.maker_rows()[0].fee_rate_bps(), None);
    }

    #[test]
    fn unknown_fields_statuses_bad_decimals_and_oversize_frames_fail_closed() {
        let mut order: serde_json::Value =
            serde_json::from_str(&fixture("ws_user_order_msg.json")).unwrap();
        order["unexpected"] = serde_json::json!(true);
        assert!(
            PolymarketAuthenticatedUserFrameV2::project(
                &serde_json::to_string(&order).unwrap(),
                1,
                1,
                ACCOUNT,
                API_KEY,
            )
            .is_err()
        );

        order.as_object_mut().unwrap().remove("unexpected");
        order["status"] = serde_json::json!("FUTURE_STATUS");
        assert!(
            PolymarketAuthenticatedUserFrameV2::project(
                &serde_json::to_string(&order).unwrap(),
                1,
                2,
                ACCOUNT,
                API_KEY,
            )
            .is_err()
        );

        order["status"] = serde_json::json!("LIVE");
        order["price"] = serde_json::json!("5e-1");
        assert!(
            PolymarketAuthenticatedUserFrameV2::project(
                &serde_json::to_string(&order).unwrap(),
                1,
                3,
                ACCOUNT,
                API_KEY,
            )
            .is_err()
        );
        assert!(
            PolymarketAuthenticatedUserFrameV2::project(
                &"x".repeat(MAX_FRAME_BYTES + 1),
                1,
                4,
                ACCOUNT,
                API_KEY,
            )
            .is_err()
        );
    }
}
