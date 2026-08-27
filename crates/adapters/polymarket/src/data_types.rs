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

use std::{collections::HashSet, sync::Arc};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{HasTsInit, custom::CustomDataTrait},
    identifiers::{InstrumentId, Symbol},
    types::Price,
};
use nautilus_persistence_macros::custom_data;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::{
    common::consts::POLYMARKET_VENUE,
    http::models::{ClobMarketInfoResponse, GammaEvent, GammaMarket, GammaTag},
};

/// Type name published for [`PolymarketFrameCommit`] custom data.
pub const POLYMARKET_FRAME_COMMIT_TYPE_NAME: &str = "PolymarketFrameCommit";
/// Type name published for [`PolymarketBookReadiness`] custom data.
pub const POLYMARKET_BOOK_READINESS_TYPE_NAME: &str = "PolymarketBookReadiness";

/// Type name returned for complete Gamma event-container snapshots.
pub const POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME: &str =
    "PolymarketEventDefinitionSnapshot";
/// Type name returned for correlated exact CLOB V2 market-parameter snapshots.
pub const POLYMARKET_CLOB_MARKET_INFO_SNAPSHOT_TYPE_NAME: &str = "PolymarketClobMarketInfoSnapshot";

const MAX_EVENT_DEFINITIONS: usize = 10_000;
const MAX_EVENT_MARKETS: usize = 1_000;
const MAX_EVENT_TAGS: usize = 256;
const MAX_MARKET_OUTCOMES: usize = 64;
const MAX_DEFINITION_TEXT_BYTES: usize = 4_096;
const MAX_CLOB_MARKET_INFOS: usize = 2_000;

/// Immutable evidence that one Polymarket market-data WebSocket frame was fully accepted.
///
/// The adapter publishes this after every L2 delta batch derived from the frame has entered the
/// same data-event FIFO. A strategy can mark instruments dirty on delta callbacks and use this
/// value as the frame evaluation boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketFrameCommit {
    frame_id: u64,
    shard_id: u64,
    connection_generation: u64,
    affected_instrument_ids: Vec<InstrumentId>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    #[serde(default)]
    book_apply_elapsed_ns: u64,
}

impl PolymarketFrameCommit {
    pub(crate) fn new(
        frame_id: u64,
        shard_id: u64,
        connection_generation: u64,
        affected_instrument_ids: Vec<InstrumentId>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        book_apply_elapsed_ns: u64,
    ) -> Self {
        debug_assert!(frame_id > 0);
        debug_assert!(connection_generation > 0);
        debug_assert!(!affected_instrument_ids.is_empty());
        debug_assert!(affected_instrument_ids.is_sorted());
        debug_assert!(
            affected_instrument_ids
                .windows(2)
                .all(|ids| ids[0] != ids[1])
        );
        Self {
            frame_id,
            shard_id,
            connection_generation,
            affected_instrument_ids,
            ts_event,
            ts_init,
            book_apply_elapsed_ns,
        }
    }

    /// Returns the monotonic adapter-local frame identifier.
    #[must_use]
    pub const fn frame_id(&self) -> u64 {
        self.frame_id
    }

    /// Returns the adapter-local market WebSocket shard identifier.
    #[must_use]
    pub const fn shard_id(&self) -> u64 {
        self.shard_id
    }

    /// Returns the non-zero generation derived from the exact transport connection epoch.
    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
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

    /// Returns adapter time from decoded frame dispatch through book application and publication.
    ///
    /// This excludes network transit and WebSocket decoding, and ends immediately before the
    /// frame commit enters the shared data-event FIFO.
    #[must_use]
    pub const fn book_apply_elapsed_ns(&self) -> u64 {
        self.book_apply_elapsed_ns
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
            commit.connection_generation > 0,
            "connection_generation must be non-zero",
        );
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

/// Readiness phase for one Polymarket L2 book source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolymarketBookReadinessState {
    /// Incremental updates are suppressed until a full snapshot is accepted.
    AwaitingSnapshot,
    /// A full snapshot from the exact current connection generation was accepted.
    Ready,
}

/// Cause of one immutable book-readiness transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolymarketBookReadinessReason {
    /// A local L2 subscription began or restarted and awaits its first full snapshot.
    Subscribed,
    /// The managed market transport became unavailable.
    ConnectionUnavailable,
    /// The owning market WebSocket shard advanced to a replacement connection.
    ConnectionEpochAdvanced,
    /// A market frame was malformed or could not be applied atomically.
    MalformedFrame,
    /// The local L2 subscription was retired.
    Unsubscribed,
    /// The data client is disconnecting.
    Disconnected,
    /// Venue tick-size evidence retired the prior book grid.
    TickSizeChanged,
    /// A full venue snapshot established the current book source.
    SnapshotAccepted,
}

/// Immutable per-instrument Polymarket L2 readiness transition.
///
/// This is descriptive adapter evidence. Event-wide readiness remains owned by the application
/// projector, which joins every required instrument in a complete topology at a frame commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketBookReadiness {
    instrument_id: InstrumentId,
    shard_id: u64,
    connection_generation: u64,
    book_epoch: u64,
    state: PolymarketBookReadinessState,
    reason: PolymarketBookReadinessReason,
    snapshot_frame_id: Option<u64>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

impl PolymarketBookReadiness {
    pub(crate) fn awaiting_snapshot(
        instrument_id: InstrumentId,
        shard_id: u64,
        connection_generation: u64,
        book_epoch: u64,
        reason: PolymarketBookReadinessReason,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        debug_assert!(connection_generation > 0);
        debug_assert!(book_epoch > 0);
        debug_assert!(reason != PolymarketBookReadinessReason::SnapshotAccepted);
        Self {
            instrument_id,
            shard_id,
            connection_generation,
            book_epoch,
            state: PolymarketBookReadinessState::AwaitingSnapshot,
            reason,
            snapshot_frame_id: None,
            ts_event,
            ts_init,
        }
    }

    pub(crate) fn ready(
        instrument_id: InstrumentId,
        shard_id: u64,
        connection_generation: u64,
        book_epoch: u64,
        snapshot_frame_id: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        debug_assert!(connection_generation > 0);
        debug_assert!(book_epoch > 0);
        debug_assert!(snapshot_frame_id > 0);
        Self {
            instrument_id,
            shard_id,
            connection_generation,
            book_epoch,
            state: PolymarketBookReadinessState::Ready,
            reason: PolymarketBookReadinessReason::SnapshotAccepted,
            snapshot_frame_id: Some(snapshot_frame_id),
            ts_event,
            ts_init,
        }
    }

    /// Returns the affected instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the owning market WebSocket shard.
    #[must_use]
    pub const fn shard_id(&self) -> u64 {
        self.shard_id
    }

    /// Returns the exact non-zero connection generation.
    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    /// Returns the instrument-local book epoch.
    #[must_use]
    pub const fn book_epoch(&self) -> u64 {
        self.book_epoch
    }

    /// Returns the readiness phase.
    #[must_use]
    pub const fn state(&self) -> PolymarketBookReadinessState {
        self.state
    }

    /// Returns the transition reason.
    #[must_use]
    pub const fn reason(&self) -> PolymarketBookReadinessReason {
        self.reason
    }

    /// Returns the snapshot frame that established readiness, when ready.
    #[must_use]
    pub const fn snapshot_frame_id(&self) -> Option<u64> {
        self.snapshot_frame_id
    }

    /// Returns the event timestamp assigned to the transition.
    #[must_use]
    pub const fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.connection_generation > 0,
            "connection_generation must be non-zero",
        );
        anyhow::ensure!(self.book_epoch > 0, "book_epoch must be non-zero");
        match (self.state, self.reason, self.snapshot_frame_id) {
            (
                PolymarketBookReadinessState::AwaitingSnapshot,
                PolymarketBookReadinessReason::Subscribed
                | PolymarketBookReadinessReason::ConnectionUnavailable
                | PolymarketBookReadinessReason::ConnectionEpochAdvanced
                | PolymarketBookReadinessReason::MalformedFrame
                | PolymarketBookReadinessReason::Unsubscribed
                | PolymarketBookReadinessReason::Disconnected
                | PolymarketBookReadinessReason::TickSizeChanged,
                None,
            )
            | (
                PolymarketBookReadinessState::Ready,
                PolymarketBookReadinessReason::SnapshotAccepted,
                Some(1..),
            ) => Ok(()),
            _ => anyhow::bail!("invalid book readiness state/reason/frame combination"),
        }
    }
}

impl HasTsInit for PolymarketBookReadiness {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for PolymarketBookReadiness {
    fn type_name(&self) -> &'static str {
        POLYMARKET_BOOK_READINESS_TYPE_NAME
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
        POLYMARKET_BOOK_READINESS_TYPE_NAME
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let readiness = serde_json::from_value::<Self>(value)?;
        readiness.validate()?;
        Ok(Arc::new(readiness))
    }
}

/// One canonical event tag retained from the Gamma event container.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketEventTagDefinition {
    id: String,
    label: Option<String>,
    slug: Option<String>,
}

impl PolymarketEventTagDefinition {
    /// Returns the Gamma tag identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the optional human-readable label.
    #[must_use]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Returns the optional canonical tag slug.
    #[must_use]
    pub fn slug(&self) -> Option<&str> {
        self.slug.as_deref()
    }
}

/// One market member exactly retained from a complete Gamma event container.
///
/// The outcome and token vectors preserve their source-relative order. This type deliberately does
/// not decide whether the member is a valid YES/NO condition; the application-owned topology
/// catalog performs that classification over the complete event snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketEventMarketDefinition {
    market_id: String,
    condition_id: String,
    question_id: Option<String>,
    market_slug: Option<String>,
    question: String,
    outcomes: Vec<String>,
    token_ids: Vec<String>,
    instrument_ids: Vec<InstrumentId>,
    active: Option<bool>,
    closed: Option<bool>,
    accepting_orders: Option<bool>,
    enable_order_book: Option<bool>,
    neg_risk: Option<bool>,
    neg_risk_market_id: Option<String>,
    neg_risk_other: Option<bool>,
    group_item_title: Option<String>,
    group_item_threshold: Option<String>,
    price_tick: Option<String>,
    minimum_order_size: Option<String>,
    fees_enabled: Option<bool>,
    fee_schedule: Option<PolymarketFeeScheduleDefinition>,
}

/// Exact public fee schedule retained from one Gamma market definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketFeeScheduleDefinition {
    rate: String,
    exponent: String,
    taker_only: bool,
    rebate_rate: String,
}

impl PolymarketFeeScheduleDefinition {
    /// Returns the exact normalized fee-curve rate.
    #[must_use]
    pub fn rate(&self) -> &str {
        &self.rate
    }

    /// Returns the exact normalized fee-curve exponent.
    #[must_use]
    pub fn exponent(&self) -> &str {
        &self.exponent
    }

    /// Returns whether the venue declares the schedule taker-only.
    #[must_use]
    pub const fn taker_only(&self) -> bool {
        self.taker_only
    }

    /// Returns the exact normalized maker-rebate rate.
    #[must_use]
    pub fn rebate_rate(&self) -> &str {
        &self.rebate_rate
    }
}

impl PolymarketEventMarketDefinition {
    /// Returns the Gamma market identifier.
    #[must_use]
    pub fn market_id(&self) -> &str {
        &self.market_id
    }

    /// Returns the on-chain condition identifier.
    #[must_use]
    pub fn condition_id(&self) -> &str {
        &self.condition_id
    }

    /// Returns the optional question identifier.
    #[must_use]
    pub fn question_id(&self) -> Option<&str> {
        self.question_id.as_deref()
    }

    /// Returns the optional market slug.
    #[must_use]
    pub fn market_slug(&self) -> Option<&str> {
        self.market_slug.as_deref()
    }

    /// Returns the market question.
    #[must_use]
    pub fn question(&self) -> &str {
        &self.question
    }

    /// Returns the source-ordered outcome labels.
    #[must_use]
    pub fn outcomes(&self) -> &[String] {
        &self.outcomes
    }

    /// Returns the source-ordered CLOB token identifiers paired with [`Self::outcomes`].
    #[must_use]
    pub fn token_ids(&self) -> &[String] {
        &self.token_ids
    }

    /// Returns the source-ordered Nautilus instrument identifiers paired with [`Self::token_ids`].
    #[must_use]
    pub fn instrument_ids(&self) -> &[InstrumentId] {
        &self.instrument_ids
    }

    /// Returns the Gamma active flag when present.
    #[must_use]
    pub const fn active(&self) -> Option<bool> {
        self.active
    }

    /// Returns the Gamma closed flag when present.
    #[must_use]
    pub const fn closed(&self) -> Option<bool> {
        self.closed
    }

    /// Returns the Gamma accepting-orders flag when present.
    #[must_use]
    pub const fn accepting_orders(&self) -> Option<bool> {
        self.accepting_orders
    }

    /// Returns the Gamma order-book-enabled flag when present.
    #[must_use]
    pub const fn enable_order_book(&self) -> Option<bool> {
        self.enable_order_book
    }

    /// Returns the market negative-risk flag when present.
    #[must_use]
    pub const fn neg_risk(&self) -> Option<bool> {
        self.neg_risk
    }

    /// Returns the optional market negative-risk domain identifier.
    #[must_use]
    pub fn neg_risk_market_id(&self) -> Option<&str> {
        self.neg_risk_market_id.as_deref()
    }

    /// Returns whether Gamma marks this as the synthetic negative-risk "other" member.
    #[must_use]
    pub const fn neg_risk_other(&self) -> Option<bool> {
        self.neg_risk_other
    }

    /// Returns the optional grouped-event display label.
    #[must_use]
    pub fn group_item_title(&self) -> Option<&str> {
        self.group_item_title.as_deref()
    }

    /// Returns the optional grouped-event threshold text.
    #[must_use]
    pub fn group_item_threshold(&self) -> Option<&str> {
        self.group_item_threshold.as_deref()
    }

    /// Returns the exact normalized minimum price increment.
    #[must_use]
    pub fn price_tick(&self) -> Option<&str> {
        self.price_tick.as_deref()
    }

    /// Returns the exact normalized minimum order size.
    #[must_use]
    pub fn minimum_order_size(&self) -> Option<&str> {
        self.minimum_order_size.as_deref()
    }

    /// Returns whether Gamma marks fees enabled for this market.
    #[must_use]
    pub const fn fees_enabled(&self) -> Option<bool> {
        self.fees_enabled
    }

    /// Returns the exact public fee schedule when supplied.
    #[must_use]
    pub const fn fee_schedule(&self) -> Option<&PolymarketFeeScheduleDefinition> {
        self.fee_schedule.as_ref()
    }
}

/// One complete Gamma event-container observation.
///
/// This is a venue definition, not an application topology generation and not an event-health
/// assertion. It retains every market member, including inactive, non-accepting, non-binary, and
/// placeholder members, so downstream classification cannot confuse filtering with completeness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketEventDefinition {
    event_id: String,
    slug: Option<String>,
    title: Option<String>,
    category: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    restricted: Option<bool>,
    enable_order_book: Option<bool>,
    enable_neg_risk: Option<bool>,
    neg_risk: Option<bool>,
    neg_risk_market_id: Option<String>,
    tags: Vec<PolymarketEventTagDefinition>,
    markets: Vec<PolymarketEventMarketDefinition>,
}

impl PolymarketEventDefinition {
    pub(crate) fn try_from_gamma(mut event: GammaEvent) -> anyhow::Result<Self> {
        validate_required_id("event_id", &event.id)?;
        validate_optional_text("event.slug", event.slug.as_deref())?;
        validate_optional_text("event.title", event.title.as_deref())?;
        validate_optional_text("event.category", event.category.as_deref())?;
        validate_optional_text("event.start_date", event.start_date.as_deref())?;
        validate_optional_text("event.end_date", event.end_date.as_deref())?;
        validate_optional_text(
            "event.neg_risk_market_id",
            event.neg_risk_market_id.as_deref(),
        )?;
        anyhow::ensure!(
            event.markets.len() <= MAX_EVENT_MARKETS,
            "event {} exceeds the market-member bound",
            event.id,
        );
        anyhow::ensure!(
            event.tags.len() <= MAX_EVENT_TAGS,
            "event {} exceeds the tag bound",
            event.id,
        );

        let mut tags = event
            .tags
            .drain(..)
            .map(PolymarketEventTagDefinition::try_from_gamma)
            .collect::<anyhow::Result<Vec<_>>>()?;
        tags.sort_by(|a, b| (&a.id, &a.slug, &a.label).cmp(&(&b.id, &b.slug, &b.label)));
        ensure_unique_by(&tags, |tag| tag.id.as_str(), "event tag id")?;

        let mut markets = event
            .markets
            .drain(..)
            .map(PolymarketEventMarketDefinition::try_from_gamma)
            .collect::<anyhow::Result<Vec<_>>>()?;
        markets
            .sort_by(|a, b| (&a.condition_id, &a.market_id).cmp(&(&b.condition_id, &b.market_id)));
        ensure_unique_by(&markets, |market| market.market_id.as_str(), "market id")?;
        ensure_unique_by(
            &markets,
            |market| market.condition_id.as_str(),
            "condition id",
        )?;
        let mut token_ids = HashSet::new();
        for market in &markets {
            for token_id in &market.token_ids {
                anyhow::ensure!(
                    token_ids.insert(token_id.as_str()),
                    "event {} repeats token id {token_id}",
                    event.id,
                );
            }
        }

        Ok(Self {
            event_id: event.id,
            slug: normalize_optional_text(event.slug),
            title: normalize_optional_text(event.title),
            category: normalize_optional_text(event.category),
            start_date: normalize_optional_text(event.start_date),
            end_date: normalize_optional_text(event.end_date),
            active: event.active,
            closed: event.closed,
            archived: event.archived,
            restricted: event.restricted,
            enable_order_book: event.enable_order_book,
            enable_neg_risk: event.enable_neg_risk,
            neg_risk: event.neg_risk,
            neg_risk_market_id: normalize_optional_text(event.neg_risk_market_id),
            tags,
            markets,
        })
    }

    /// Returns the Gamma event identifier.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the optional event slug.
    #[must_use]
    pub fn slug(&self) -> Option<&str> {
        self.slug.as_deref()
    }

    /// Returns the optional event title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the optional event category.
    #[must_use]
    pub fn category(&self) -> Option<&str> {
        self.category.as_deref()
    }

    /// Returns the optional event start date text.
    #[must_use]
    pub fn start_date(&self) -> Option<&str> {
        self.start_date.as_deref()
    }

    /// Returns the optional event end date text.
    #[must_use]
    pub fn end_date(&self) -> Option<&str> {
        self.end_date.as_deref()
    }

    /// Returns the Gamma active flag when present.
    #[must_use]
    pub const fn active(&self) -> Option<bool> {
        self.active
    }

    /// Returns the Gamma closed flag when present.
    #[must_use]
    pub const fn closed(&self) -> Option<bool> {
        self.closed
    }

    /// Returns the Gamma archived flag when present.
    #[must_use]
    pub const fn archived(&self) -> Option<bool> {
        self.archived
    }

    /// Returns the Gamma restricted flag when present.
    #[must_use]
    pub const fn restricted(&self) -> Option<bool> {
        self.restricted
    }

    /// Returns the event order-book-enabled flag when present.
    #[must_use]
    pub const fn enable_order_book(&self) -> Option<bool> {
        self.enable_order_book
    }

    /// Returns the event negative-risk-enabled flag when present.
    #[must_use]
    pub const fn enable_neg_risk(&self) -> Option<bool> {
        self.enable_neg_risk
    }

    /// Returns the event negative-risk flag when present.
    #[must_use]
    pub const fn neg_risk(&self) -> Option<bool> {
        self.neg_risk
    }

    /// Returns the optional event negative-risk domain identifier.
    #[must_use]
    pub fn neg_risk_market_id(&self) -> Option<&str> {
        self.neg_risk_market_id.as_deref()
    }

    /// Returns the canonical event tags.
    #[must_use]
    pub fn tags(&self) -> &[PolymarketEventTagDefinition] {
        &self.tags
    }

    /// Returns every canonical market member from the Gamma event container.
    #[must_use]
    pub fn markets(&self) -> &[PolymarketEventMarketDefinition] {
        &self.markets
    }
}

impl PolymarketEventTagDefinition {
    fn try_from_gamma(tag: GammaTag) -> anyhow::Result<Self> {
        validate_required_id("tag.id", &tag.id)?;
        validate_optional_text("tag.label", tag.label.as_deref())?;
        validate_optional_text("tag.slug", tag.slug.as_deref())?;
        Ok(Self {
            id: tag.id,
            label: normalize_optional_text(tag.label),
            slug: normalize_optional_text(tag.slug),
        })
    }
}

impl PolymarketEventMarketDefinition {
    fn try_from_gamma(market: GammaMarket) -> anyhow::Result<Self> {
        validate_required_id("market.id", &market.id)?;
        validate_required_id("market.condition_id", &market.condition_id)?;
        validate_required_text("market.question", &market.question)?;
        validate_optional_text("market.question_id", market.question_id.as_deref())?;
        validate_optional_text("market.slug", market.market_slug.as_deref())?;
        validate_optional_text(
            "market.neg_risk_market_id",
            market.neg_risk_market_id.as_deref(),
        )?;
        validate_optional_text(
            "market.group_item_title",
            market.group_item_title.as_deref(),
        )?;
        validate_optional_text(
            "market.group_item_threshold",
            market.group_item_threshold.as_deref(),
        )?;
        let outcomes = parse_embedded_string_array("outcomes", &market.outcomes)?;
        let token_ids = parse_embedded_string_array("clob_token_ids", &market.clob_token_ids)?;
        anyhow::ensure!(
            outcomes.len() <= MAX_MARKET_OUTCOMES,
            "market {} exceeds the outcome bound",
            market.id,
        );
        anyhow::ensure!(
            token_ids.len() <= MAX_MARKET_OUTCOMES,
            "market {} exceeds the token bound",
            market.id,
        );
        ensure_unique_strings(&outcomes, "market outcome")?;
        ensure_unique_strings(&token_ids, "market token id")?;
        let instrument_ids = token_ids
            .iter()
            .map(|token_id| {
                InstrumentId::new(
                    Symbol::new(format!("{}-{token_id}", market.condition_id)),
                    *POLYMARKET_VENUE,
                )
            })
            .collect();

        let price_tick = market
            .order_price_min_tick_size
            .as_ref()
            .map(|value| canonical_decimal("market.price_tick", value))
            .transpose()?;
        let minimum_order_size = market
            .order_min_size
            .as_ref()
            .map(|value| canonical_decimal("market.minimum_order_size", value))
            .transpose()?;
        let fee_schedule = market
            .fee_schedule
            .as_ref()
            .map(
                |schedule| -> anyhow::Result<PolymarketFeeScheduleDefinition> {
                    Ok(PolymarketFeeScheduleDefinition {
                        rate: canonical_decimal("market.fee_schedule.rate", &schedule.rate)?,
                        exponent: canonical_decimal(
                            "market.fee_schedule.exponent",
                            &schedule.exponent,
                        )?,
                        taker_only: schedule.taker_only,
                        rebate_rate: canonical_decimal(
                            "market.fee_schedule.rebate_rate",
                            &schedule.rebate_rate,
                        )?,
                    })
                },
            )
            .transpose()?;

        Ok(Self {
            market_id: market.id,
            condition_id: market.condition_id,
            question_id: normalize_optional_text(market.question_id),
            market_slug: normalize_optional_text(market.market_slug),
            question: market.question,
            outcomes,
            token_ids,
            instrument_ids,
            active: market.active,
            closed: market.closed,
            accepting_orders: market.accepting_orders,
            enable_order_book: market.enable_order_book,
            neg_risk: market.neg_risk,
            neg_risk_market_id: normalize_optional_text(market.neg_risk_market_id),
            neg_risk_other: market.neg_risk_other,
            group_item_title: normalize_optional_text(market.group_item_title),
            group_item_threshold: normalize_optional_text(market.group_item_threshold),
            price_tick,
            minimum_order_size,
            fees_enabled: market.fees_enabled,
            fee_schedule,
        })
    }
}

fn canonical_decimal(
    field: &str,
    value: &crate::http::models::GammaDecimal,
) -> anyhow::Result<String> {
    let parsed: Decimal = value
        .as_str()
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {field}: {error}"))?;
    anyhow::ensure!(!parsed.is_sign_negative(), "{field} must be non-negative");
    Ok(parsed.normalize().to_string())
}

/// Canonical response to a complete active-event definition request.
///
/// The snapshot is descriptive public venue data. The application-owned topology catalog validates
/// it, assigns topology generations, and joins independent per-instrument book-health evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolymarketEventDefinitionSnapshot {
    events: Vec<PolymarketEventDefinition>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

impl PolymarketEventDefinitionSnapshot {
    pub(crate) fn try_new(
        mut events: Vec<PolymarketEventDefinition>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Self> {
        events.sort_by(|a, b| a.event_id.cmp(&b.event_id));
        let snapshot = Self {
            events,
            ts_event: ts_init,
            ts_init,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.ts_event == self.ts_init,
            "event definition snapshot timestamps must match",
        );
        anyhow::ensure!(
            self.events.len() <= MAX_EVENT_DEFINITIONS,
            "event definition snapshot exceeds the event bound",
        );
        anyhow::ensure!(
            self.events
                .windows(2)
                .all(|events| events[0].event_id < events[1].event_id),
            "event definitions must be sorted by unique event_id",
        );
        for event in &self.events {
            validate_event_definition(event)?;
        }
        Ok(())
    }

    /// Returns the canonical complete event definitions.
    #[must_use]
    pub fn events(&self) -> &[PolymarketEventDefinition] {
        &self.events
    }

    /// Returns the local observation timestamp.
    #[must_use]
    pub const fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }
}

impl<'de> Deserialize<'de> for PolymarketEventDefinitionSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireSnapshot {
            events: Vec<PolymarketEventDefinition>,
            ts_event: UnixNanos,
            ts_init: UnixNanos,
        }

        let wire = WireSnapshot::deserialize(deserializer)?;
        let snapshot = Self {
            events: wire.events,
            ts_event: wire.ts_event,
            ts_init: wire.ts_init,
        };
        snapshot.validate().map_err(D::Error::custom)?;
        Ok(snapshot)
    }
}

impl HasTsInit for PolymarketEventDefinitionSnapshot {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for PolymarketEventDefinitionSnapshot {
    fn type_name(&self) -> &'static str {
        POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME
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
        POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let snapshot = serde_json::from_value::<Self>(value)?;
        snapshot.validate()?;
        Ok(Arc::new(snapshot))
    }
}

/// One exact source-ordered token identity from CLOB V2 market info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketClobTokenDefinition {
    token_id: String,
    outcome: String,
}

impl PolymarketClobTokenDefinition {
    /// Returns the canonical CLOB token identifier.
    #[must_use]
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the CLOB outcome label paired with the token.
    #[must_use]
    pub fn outcome(&self) -> &str {
        &self.outcome
    }
}

/// Exact execution parameters for one CLOB V2 condition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketClobMarketInfo {
    condition_id: String,
    tokens: Vec<PolymarketClobTokenDefinition>,
    minimum_order_size: String,
    minimum_tick_size: String,
    accepting_orders: Option<bool>,
    neg_risk: bool,
    fee_rate: String,
    fee_exponent: String,
    taker_only: bool,
    version: String,
}

impl PolymarketClobMarketInfo {
    fn try_from_response(response: ClobMarketInfoResponse) -> anyhow::Result<Self> {
        let info = Self {
            condition_id: response.condition_id,
            tokens: response
                .tokens
                .into_iter()
                .map(|token| PolymarketClobTokenDefinition {
                    token_id: token.token_id,
                    outcome: token.outcome,
                })
                .collect(),
            minimum_order_size: canonical_decimal(
                "clob.minimum_order_size",
                &response.minimum_order_size,
            )?,
            minimum_tick_size: canonical_decimal(
                "clob.minimum_tick_size",
                &response.minimum_tick_size,
            )?,
            accepting_orders: response.accepting_orders,
            neg_risk: response.neg_risk,
            fee_rate: canonical_decimal("clob.fee_rate", &response.fee_details.rate)?,
            fee_exponent: canonical_decimal("clob.fee_exponent", &response.fee_details.exponent)?,
            taker_only: response.fee_details.taker_only,
            version: response.version,
        };
        validate_clob_market_info(&info)?;
        Ok(info)
    }

    #[must_use]
    pub fn condition_id(&self) -> &str {
        &self.condition_id
    }

    #[must_use]
    pub fn tokens(&self) -> &[PolymarketClobTokenDefinition] {
        &self.tokens
    }

    #[must_use]
    pub fn minimum_order_size(&self) -> &str {
        &self.minimum_order_size
    }

    #[must_use]
    pub fn minimum_tick_size(&self) -> &str {
        &self.minimum_tick_size
    }

    #[must_use]
    pub const fn accepting_orders(&self) -> Option<bool> {
        self.accepting_orders
    }

    #[must_use]
    pub const fn neg_risk(&self) -> bool {
        self.neg_risk
    }

    #[must_use]
    pub fn fee_rate(&self) -> &str {
        &self.fee_rate
    }

    #[must_use]
    pub fn fee_exponent(&self) -> &str {
        &self.fee_exponent
    }

    #[must_use]
    pub const fn taker_only(&self) -> bool {
        self.taker_only
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
}

/// Correlated canonical response containing exact CLOB V2 parameters for requested conditions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolymarketClobMarketInfoSnapshot {
    markets: Vec<PolymarketClobMarketInfo>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

impl PolymarketClobMarketInfoSnapshot {
    pub(crate) fn try_new(
        responses: Vec<ClobMarketInfoResponse>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!responses.is_empty(), "CLOB market-info snapshot is empty");
        let mut markets = responses
            .into_iter()
            .map(PolymarketClobMarketInfo::try_from_response)
            .collect::<anyhow::Result<Vec<_>>>()?;
        markets.sort_by(|left, right| left.condition_id.cmp(&right.condition_id));
        let snapshot = Self {
            markets,
            ts_event: ts_init,
            ts_init,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.ts_event == self.ts_init,
            "CLOB market-info snapshot timestamps must match"
        );
        anyhow::ensure!(
            !self.markets.is_empty() && self.markets.len() <= MAX_CLOB_MARKET_INFOS,
            "CLOB market-info snapshot violates the market bound"
        );
        anyhow::ensure!(
            self.markets
                .windows(2)
                .all(|markets| markets[0].condition_id < markets[1].condition_id),
            "CLOB market infos must be sorted by unique condition_id"
        );
        let mut token_ids = HashSet::new();
        for market in &self.markets {
            validate_clob_market_info(market)?;
            for token in &market.tokens {
                anyhow::ensure!(
                    token_ids.insert(token.token_id.as_str()),
                    "duplicate CLOB token id {}",
                    token.token_id
                );
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn markets(&self) -> &[PolymarketClobMarketInfo] {
        &self.markets
    }

    #[must_use]
    pub const fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }
}

impl<'de> Deserialize<'de> for PolymarketClobMarketInfoSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireSnapshot {
            markets: Vec<PolymarketClobMarketInfo>,
            ts_event: UnixNanos,
            ts_init: UnixNanos,
        }

        let wire = WireSnapshot::deserialize(deserializer)?;
        let snapshot = Self {
            markets: wire.markets,
            ts_event: wire.ts_event,
            ts_init: wire.ts_init,
        };
        snapshot.validate().map_err(D::Error::custom)?;
        Ok(snapshot)
    }
}

impl HasTsInit for PolymarketClobMarketInfoSnapshot {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for PolymarketClobMarketInfoSnapshot {
    fn type_name(&self) -> &'static str {
        POLYMARKET_CLOB_MARKET_INFO_SNAPSHOT_TYPE_NAME
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
        POLYMARKET_CLOB_MARKET_INFO_SNAPSHOT_TYPE_NAME
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let snapshot = serde_json::from_value::<Self>(value)?;
        snapshot.validate()?;
        Ok(Arc::new(snapshot))
    }
}

fn validate_clob_market_info(info: &PolymarketClobMarketInfo) -> anyhow::Result<()> {
    validate_required_id("clob.condition_id", &info.condition_id)?;
    validate_required_text("clob.version", &info.version)?;
    anyhow::ensure!(
        info.tokens.len() == 2,
        "CLOB binary market must have two tokens"
    );
    let mut outcomes = HashSet::new();
    let mut tokens = HashSet::new();
    for token in &info.tokens {
        validate_required_id("clob.token_id", &token.token_id)?;
        validate_required_text("clob.outcome", &token.outcome)?;
        anyhow::ensure!(
            tokens.insert(token.token_id.as_str()),
            "duplicate token in CLOB market info"
        );
        anyhow::ensure!(
            outcomes.insert(token.outcome.to_ascii_lowercase()),
            "duplicate outcome in CLOB market info"
        );
    }
    anyhow::ensure!(
        outcomes.contains("yes") && outcomes.contains("no"),
        "CLOB market info must pair YES and NO"
    );
    anyhow::ensure!(
        !validate_canonical_decimal("clob.minimum_order_size", &info.minimum_order_size)?.is_zero(),
        "clob.minimum_order_size must be positive"
    );
    anyhow::ensure!(
        !validate_canonical_decimal("clob.minimum_tick_size", &info.minimum_tick_size)?.is_zero(),
        "clob.minimum_tick_size must be positive"
    );
    validate_canonical_decimal("clob.fee_rate", &info.fee_rate)?;
    anyhow::ensure!(
        !validate_canonical_decimal("clob.fee_exponent", &info.fee_exponent)?.is_zero(),
        "clob.fee_exponent must be positive"
    );
    Ok(())
}

fn validate_event_definition(event: &PolymarketEventDefinition) -> anyhow::Result<()> {
    validate_required_id("event_id", &event.event_id)?;
    validate_optional_text("event.slug", event.slug.as_deref())?;
    validate_optional_text("event.title", event.title.as_deref())?;
    validate_optional_text("event.category", event.category.as_deref())?;
    validate_optional_text("event.start_date", event.start_date.as_deref())?;
    validate_optional_text("event.end_date", event.end_date.as_deref())?;
    validate_optional_text(
        "event.neg_risk_market_id",
        event.neg_risk_market_id.as_deref(),
    )?;
    anyhow::ensure!(
        event.markets.len() <= MAX_EVENT_MARKETS,
        "event {} exceeds the market-member bound",
        event.event_id,
    );
    anyhow::ensure!(
        event.tags.len() <= MAX_EVENT_TAGS,
        "event {} exceeds the tag bound",
        event.event_id,
    );
    anyhow::ensure!(
        event
            .markets
            .windows(2)
            .all(|markets| (&markets[0].condition_id, &markets[0].market_id)
                < (&markets[1].condition_id, &markets[1].market_id)),
        "event {} markets must be canonical and unique",
        event.event_id,
    );
    anyhow::ensure!(
        event.tags.windows(2).all(|tags| {
            (&tags[0].id, &tags[0].slug, &tags[0].label)
                < (&tags[1].id, &tags[1].slug, &tags[1].label)
        }),
        "event {} tags must be canonical and unique",
        event.event_id,
    );
    let mut market_ids = HashSet::new();
    let mut condition_ids = HashSet::new();
    let mut token_ids = HashSet::new();
    let mut tag_ids = HashSet::new();
    for tag in &event.tags {
        validate_required_id("tag.id", &tag.id)?;
        validate_optional_text("tag.label", tag.label.as_deref())?;
        validate_optional_text("tag.slug", tag.slug.as_deref())?;
        anyhow::ensure!(
            tag_ids.insert(tag.id.as_str()),
            "duplicate event tag id {}",
            tag.id,
        );
    }
    for market in &event.markets {
        validate_required_id("market.id", &market.market_id)?;
        validate_required_id("market.condition_id", &market.condition_id)?;
        validate_required_text("market.question", &market.question)?;
        validate_optional_text("market.question_id", market.question_id.as_deref())?;
        validate_optional_text("market.slug", market.market_slug.as_deref())?;
        validate_optional_text(
            "market.neg_risk_market_id",
            market.neg_risk_market_id.as_deref(),
        )?;
        validate_optional_text(
            "market.group_item_title",
            market.group_item_title.as_deref(),
        )?;
        validate_optional_text(
            "market.group_item_threshold",
            market.group_item_threshold.as_deref(),
        )?;
        if let Some(price_tick) = &market.price_tick {
            anyhow::ensure!(
                !validate_canonical_decimal("market.price_tick", price_tick)?.is_zero(),
                "market.price_tick must be positive",
            );
        }
        if let Some(minimum_order_size) = &market.minimum_order_size {
            anyhow::ensure!(
                !validate_canonical_decimal("market.minimum_order_size", minimum_order_size)?
                    .is_zero(),
                "market.minimum_order_size must be positive",
            );
        }
        if let Some(schedule) = &market.fee_schedule {
            validate_canonical_decimal("market.fee_schedule.rate", &schedule.rate)?;
            anyhow::ensure!(
                !validate_canonical_decimal("market.fee_schedule.exponent", &schedule.exponent,)?
                    .is_zero(),
                "market.fee_schedule.exponent must be positive",
            );
            validate_canonical_decimal("market.fee_schedule.rebate_rate", &schedule.rebate_rate)?;
        }
        anyhow::ensure!(
            market.outcomes.len() <= MAX_MARKET_OUTCOMES
                && market.token_ids.len() <= MAX_MARKET_OUTCOMES
                && market.instrument_ids.len() <= MAX_MARKET_OUTCOMES,
            "market {} exceeds the outcome/token bound",
            market.market_id,
        );
        anyhow::ensure!(
            market.outcomes.len() == market.token_ids.len()
                && market.token_ids.len() == market.instrument_ids.len(),
            "market {} outcome/token/instrument vectors must align",
            market.market_id,
        );
        ensure_unique_strings(&market.outcomes, "market outcome")?;
        ensure_unique_strings(&market.token_ids, "market token id")?;
        anyhow::ensure!(
            market.instrument_ids.iter().collect::<HashSet<_>>().len()
                == market.instrument_ids.len(),
            "market {} instrument ids must be unique",
            market.market_id,
        );
        for outcome in &market.outcomes {
            validate_required_text("market outcome", outcome)?;
        }
        for token_id in &market.token_ids {
            validate_required_id("market token id", token_id)?;
        }
        for (token_id, instrument_id) in market.token_ids.iter().zip(&market.instrument_ids) {
            let expected = InstrumentId::new(
                Symbol::new(format!("{}-{token_id}", market.condition_id)),
                *POLYMARKET_VENUE,
            );
            anyhow::ensure!(
                *instrument_id == expected,
                "market {} instrument id does not match condition/token identity",
                market.market_id,
            );
        }
        anyhow::ensure!(
            market_ids.insert(market.market_id.as_str()),
            "duplicate market id {}",
            market.market_id,
        );
        anyhow::ensure!(
            condition_ids.insert(market.condition_id.as_str()),
            "duplicate condition id {}",
            market.condition_id,
        );
        for token_id in &market.token_ids {
            anyhow::ensure!(
                token_ids.insert(token_id.as_str()),
                "duplicate event token id {token_id}",
            );
        }
    }
    Ok(())
}

fn parse_embedded_string_array(field: &str, raw: &str) -> anyhow::Result<Vec<String>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let values = serde_json::from_str::<Vec<String>>(raw)
        .map_err(|error| anyhow::anyhow!("invalid Gamma {field}: {error}"))?;
    for value in &values {
        validate_required_text(field, value)?;
    }
    Ok(values)
}

fn validate_required_id(field: &str, value: &str) -> anyhow::Result<()> {
    validate_required_text(field, value)?;
    anyhow::ensure!(value.trim() == value, "{field} must be canonical");
    Ok(())
}

fn validate_required_text(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "{field} must not be empty");
    anyhow::ensure!(
        value.len() <= MAX_DEFINITION_TEXT_BYTES,
        "{field} exceeds the text bound",
    );
    Ok(())
}

fn validate_optional_text(field: &str, value: Option<&str>) -> anyhow::Result<()> {
    if let Some(value) = value {
        anyhow::ensure!(
            value.len() <= MAX_DEFINITION_TEXT_BYTES,
            "{field} exceeds the text bound",
        );
    }
    Ok(())
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn validate_canonical_decimal(field: &str, value: &str) -> anyhow::Result<Decimal> {
    let parsed: Decimal = value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {field}: {error}"))?;
    anyhow::ensure!(!parsed.is_sign_negative(), "{field} must be non-negative");
    anyhow::ensure!(
        parsed.normalize().to_string() == value,
        "{field} is not canonical"
    );
    Ok(parsed)
}

fn ensure_unique_strings(values: &[String], field: &str) -> anyhow::Result<()> {
    let mut unique = HashSet::new();
    for value in values {
        anyhow::ensure!(unique.insert(value.as_str()), "duplicate {field} {value}");
    }
    Ok(())
}

fn ensure_unique_by<'a, T, F>(values: &'a [T], mut key: F, field: &str) -> anyhow::Result<()>
where
    F: FnMut(&'a T) -> &'a str,
{
    let mut unique = HashSet::new();
    for value in values {
        let key = key(value);
        anyhow::ensure!(unique.insert(key), "duplicate {field} {key}");
    }
    Ok(())
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
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketBookReadiness>();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<
        PolymarketEventDefinitionSnapshot,
    >();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<
        PolymarketClobMarketInfoSnapshot,
    >();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketRtdsCryptoPrice>();
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<PolymarketRtdsEquityPrice>();
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::data::custom::CustomDataTrait;
    use rstest::rstest;

    use super::{
        PolymarketClobMarketInfoSnapshot, PolymarketEventDefinition,
        PolymarketEventDefinitionSnapshot, register_polymarket_custom_data,
    };
    use crate::http::models::{ClobMarketInfoResponse, FeeSchedule, GammaEvent};

    fn gamma_events() -> Vec<GammaEvent> {
        serde_json::from_str(include_str!("../test_data/gamma_event.json"))
            .expect("valid Gamma event fixture")
    }

    #[rstest]
    fn test_register_polymarket_custom_data_is_idempotent() {
        register_polymarket_custom_data();
        register_polymarket_custom_data();
    }

    #[rstest]
    fn clob_market_info_snapshot_retains_exact_v2_execution_parameters() {
        let response: ClobMarketInfoResponse = serde_json::from_str(
            r#"{
                "c":"0xcondition",
                "t":[{"t":"11","o":"Yes"},{"t":"22","o":"No"}],
                "mos":5,
                "mts":0.001,
                "ao":true,
                "nr":true,
                "fd":{"r":0.0500,"e":1.0,"to":true},
                "v":"v1"
            }"#,
        )
        .expect("CLOB V2 market info");
        let snapshot =
            PolymarketClobMarketInfoSnapshot::try_new(vec![response], UnixNanos::from(42_u64))
                .expect("canonical market info snapshot");
        let market = &snapshot.markets()[0];
        assert_eq!(market.condition_id(), "0xcondition");
        assert_eq!(market.minimum_order_size(), "5");
        assert_eq!(market.minimum_tick_size(), "0.001");
        assert_eq!(market.fee_rate(), "0.05");
        assert_eq!(market.fee_exponent(), "1");
        assert!(market.taker_only());
        assert_eq!(market.accepting_orders(), Some(true));
        assert!(market.neg_risk());
        assert_eq!(market.version(), "v1");
        assert_eq!(market.tokens()[0].outcome(), "Yes");
        assert_eq!(market.tokens()[1].token_id(), "22");

        let restored = PolymarketClobMarketInfoSnapshot::from_json(
            serde_json::to_value(&snapshot).expect("snapshot json"),
        )
        .expect("validated custom data");
        assert_eq!(
            restored
                .as_any()
                .downcast_ref::<PolymarketClobMarketInfoSnapshot>(),
            Some(&snapshot)
        );
    }

    #[rstest]
    fn clob_market_info_retains_missing_accepting_orders_as_unknown() {
        let response: ClobMarketInfoResponse = serde_json::from_str(
            r#"{
                "c":"0xcondition",
                "t":[{"t":"11","o":"Yes"},{"t":"22","o":"No"}],
                "mos":5,
                "mts":0.001,
                "nr":true,
                "fd":{"r":0.05,"e":1,"to":true},
                "v":"v1"
            }"#,
        )
        .expect("live-compatible CLOB V2 market info without ao");
        let snapshot =
            PolymarketClobMarketInfoSnapshot::try_new(vec![response], UnixNanos::from(42_u64))
                .expect("canonical market info snapshot");

        assert_eq!(snapshot.markets()[0].accepting_orders(), None);
    }

    #[rstest]
    fn clob_market_info_snapshot_rejects_identity_and_fee_shape_corruption() {
        let invalid = serde_json::json!({
            "markets": [{
                "condition_id":"0xcondition",
                "tokens":[{"token_id":"11","outcome":"Yes"},{"token_id":"11","outcome":"No"}],
                "minimum_order_size":"5",
                "minimum_tick_size":"0.001",
                "accepting_orders":true,
                "neg_risk":true,
                "fee_rate":"0.0500",
                "fee_exponent":"1",
                "taker_only":true,
                "version":"v1"
            }],
            "ts_event":42,
            "ts_init":42
        });
        assert!(PolymarketClobMarketInfoSnapshot::from_json(invalid).is_err());
    }

    #[rstest]
    fn event_definition_canonicalizes_container_order_without_filtering_members() {
        let mut event = gamma_events().remove(0);
        let expected_market_count = event.markets.len();
        let first = PolymarketEventDefinition::try_from_gamma(event.clone())
            .expect("canonical event definition");

        event.markets.reverse();
        event.tags.reverse();
        let reordered = PolymarketEventDefinition::try_from_gamma(event)
            .expect("reordered source canonicalizes");

        assert_eq!(first, reordered);
        assert_eq!(first.markets().len(), expected_market_count);
        assert!(
            first
                .markets()
                .iter()
                .all(|market| !market.token_ids().is_empty())
        );
        assert!(first.markets().windows(2).all(|markets| {
            (markets[0].condition_id(), markets[0].market_id())
                < (markets[1].condition_id(), markets[1].market_id())
        }));
        let first_market = &first.markets()[0];
        for (token_id, instrument_id) in first_market
            .token_ids()
            .iter()
            .zip(first_market.instrument_ids())
        {
            assert_eq!(
                instrument_id.to_string(),
                format!("{}-{token_id}.POLYMARKET", first_market.condition_id())
            );
        }
        assert_eq!(first_market.price_tick(), Some("0.001"));
        assert_eq!(first_market.minimum_order_size(), Some("5"));
        assert_eq!(first_market.fees_enabled(), Some(false));
        assert!(first_market.fee_schedule().is_none());
    }

    #[rstest]
    fn event_definition_retains_exact_normalized_fee_schedule() {
        let mut event = gamma_events().remove(0);
        event.markets[0].fees_enabled = Some(true);
        event.markets[0].fee_schedule = Some(FeeSchedule {
            rate: "0.0500".parse().expect("rate"),
            exponent: "1.000".parse().expect("exponent"),
            taker_only: true,
            rebate_rate: "0.2500".parse().expect("rebate rate"),
        });

        let definition = PolymarketEventDefinition::try_from_gamma(event).expect("definition");
        let schedule = definition.markets()[0]
            .fee_schedule()
            .expect("fee schedule");
        assert_eq!(schedule.rate(), "0.05");
        assert_eq!(schedule.exponent(), "1");
        assert!(schedule.taker_only());
        assert_eq!(schedule.rebate_rate(), "0.25");
    }

    #[rstest]
    fn event_definition_retains_incomplete_and_nonbinary_members_for_catalog_rejection() {
        let mut event = gamma_events().remove(0);
        event.markets[0].clob_token_ids.clear();
        event.markets[0].outcomes = r#"["A","B","C"]"#.to_string();

        let definition = PolymarketEventDefinition::try_from_gamma(event)
            .expect("incomplete and nonbinary definitions are descriptive data");
        let member = definition
            .markets()
            .iter()
            .find(|market| market.token_ids().is_empty())
            .expect("incomplete member retained");
        assert_eq!(member.outcomes(), &["A", "B", "C"]);
    }

    #[rstest]
    fn duplicate_condition_market_and_token_identities_fail_closed() {
        let base = gamma_events().remove(0);

        let mut duplicate_condition = base.clone();
        duplicate_condition.markets[1].condition_id =
            duplicate_condition.markets[0].condition_id.clone();
        assert!(PolymarketEventDefinition::try_from_gamma(duplicate_condition).is_err());

        let mut duplicate_market = base.clone();
        duplicate_market.markets[1].id = duplicate_market.markets[0].id.clone();
        assert!(PolymarketEventDefinition::try_from_gamma(duplicate_market).is_err());

        let mut duplicate_token = base;
        let first_tokens = duplicate_token.markets[0].clob_token_ids.clone();
        duplicate_token.markets[1].clob_token_ids = first_tokens;
        assert!(PolymarketEventDefinition::try_from_gamma(duplicate_token).is_err());
    }

    #[rstest]
    fn snapshot_json_round_trip_revalidates_canonical_order() {
        let definitions = gamma_events()
            .into_iter()
            .map(PolymarketEventDefinition::try_from_gamma)
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("definitions");
        let snapshot =
            PolymarketEventDefinitionSnapshot::try_new(definitions, UnixNanos::from(42_u64))
                .expect("snapshot");
        let json = snapshot.to_json().expect("snapshot JSON");
        let restored = <PolymarketEventDefinitionSnapshot as CustomDataTrait>::from_json(
            serde_json::from_str(&json).expect("JSON value"),
        )
        .expect("validated restore");
        assert!(snapshot.eq_arc(restored.as_ref()));

        let mut malformed = serde_json::from_str::<serde_json::Value>(&json).expect("JSON value");
        malformed["events"][0]["markets"]
            .as_array_mut()
            .expect("markets")
            .reverse();
        assert!(
            <PolymarketEventDefinitionSnapshot as CustomDataTrait>::from_json(malformed).is_err()
        );

        let mut invalid_tick =
            serde_json::from_str::<serde_json::Value>(&json).expect("JSON value");
        invalid_tick["events"][0]["markets"][0]["price_tick"] = serde_json::json!("0");
        assert!(
            <PolymarketEventDefinitionSnapshot as CustomDataTrait>::from_json(invalid_tick)
                .is_err()
        );

        let mut mismatched_time =
            serde_json::from_str::<serde_json::Value>(&json).expect("JSON value");
        mismatched_time["ts_event"] = serde_json::json!(43);
        assert!(
            <PolymarketEventDefinitionSnapshot as CustomDataTrait>::from_json(mismatched_time)
                .is_err()
        );
    }
}
