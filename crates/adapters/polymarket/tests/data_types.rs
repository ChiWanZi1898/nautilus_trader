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

#![recursion_limit = "256"]

use nautilus_model::data::custom::CustomDataTrait;
use nautilus_polymarket::data_types::{
    POLYMARKET_BOOK_READINESS_TYPE_NAME, POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME,
    POLYMARKET_FRAME_COMMIT_TYPE_NAME, PolymarketBookReadiness, PolymarketBookReadinessReason,
    PolymarketBookReadinessState, PolymarketEventDefinitionSnapshot, PolymarketFrameCommit,
};

#[test]
fn frame_commit_is_a_public_typed_custom_data_contract() {
    fn assert_custom_data<T: CustomDataTrait>() {}

    assert_custom_data::<PolymarketFrameCommit>();
    assert_eq!(
        <PolymarketFrameCommit as CustomDataTrait>::type_name_static(),
        POLYMARKET_FRAME_COMMIT_TYPE_NAME,
    );
}

#[test]
fn book_readiness_is_a_validated_public_typed_custom_data_contract() {
    fn assert_custom_data<T: CustomDataTrait>() {}

    assert_custom_data::<PolymarketBookReadiness>();
    assert_eq!(
        <PolymarketBookReadiness as CustomDataTrait>::type_name_static(),
        POLYMARKET_BOOK_READINESS_TYPE_NAME,
    );

    let value = serde_json::json!({
        "instrument_id": "0xTOKEN.POLYMARKET",
        "shard_id": 3,
        "connection_generation": 7,
        "book_epoch": 9,
        "state": "READY",
        "reason": "SNAPSHOT_ACCEPTED",
        "snapshot_frame_id": 11,
        "ts_event": 42,
        "ts_init": 43
    });
    let restored = <PolymarketBookReadiness as CustomDataTrait>::from_json(value.clone())
        .expect("validated readiness");
    let readiness = restored
        .as_any()
        .downcast_ref::<PolymarketBookReadiness>()
        .expect("public downcast");
    assert_eq!(readiness.shard_id(), 3);
    assert_eq!(readiness.connection_generation(), 7);
    assert_eq!(readiness.book_epoch(), 9);
    assert_eq!(readiness.state(), PolymarketBookReadinessState::Ready);
    assert_eq!(
        readiness.reason(),
        PolymarketBookReadinessReason::SnapshotAccepted
    );
    assert_eq!(readiness.snapshot_frame_id(), Some(11));

    let mut invalid = value;
    invalid["snapshot_frame_id"] = serde_json::Value::Null;
    assert!(<PolymarketBookReadiness as CustomDataTrait>::from_json(invalid).is_err());
}

#[test]
fn event_definition_snapshot_is_a_public_typed_custom_data_contract() {
    fn assert_custom_data<T: CustomDataTrait>() {}

    assert_custom_data::<PolymarketEventDefinitionSnapshot>();
    assert_eq!(
        <PolymarketEventDefinitionSnapshot as CustomDataTrait>::type_name_static(),
        POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME,
    );

    let restored =
        <PolymarketEventDefinitionSnapshot as CustomDataTrait>::from_json(serde_json::json!({
            "events": [{
                "event_id": "event-1",
                "slug": "temperature-event",
                "title": "Temperature event",
                "category": "weather",
                "start_date": null,
                "end_date": null,
                "active": true,
                "closed": false,
                "archived": false,
                "restricted": false,
                "enable_order_book": true,
                "enable_neg_risk": true,
                "neg_risk": true,
                "neg_risk_market_id": "neg-risk-1",
                "tags": [{"id": "tag-1", "label": "Weather", "slug": "weather"}],
                "markets": [{
                    "market_id": "market-1",
                    "condition_id": "condition-1",
                    "question_id": "question-1",
                    "market_slug": "condition-1",
                    "question": "Will the temperature exceed 20C?",
                    "outcomes": ["Yes", "No"],
                    "token_ids": ["yes-token", "no-token"],
                    "active": true,
                    "closed": false,
                    "accepting_orders": true,
                    "enable_order_book": true,
                    "neg_risk": true,
                    "neg_risk_market_id": "neg-risk-1",
                    "neg_risk_other": false,
                    "group_item_title": "20C",
                    "group_item_threshold": "20",
                    "price_tick": "0.001",
                    "minimum_order_size": "5",
                    "fees_enabled": true,
                    "fee_schedule": {
                        "rate": "0.05",
                        "exponent": "1",
                        "taker_only": true,
                        "rebate_rate": "0.25"
                    }
                }]
            }],
            "ts_event": 42,
            "ts_init": 42
        }))
        .expect("validated public JSON contract");
    let snapshot = restored
        .as_any()
        .downcast_ref::<PolymarketEventDefinitionSnapshot>()
        .expect("public downcast");
    assert_eq!(snapshot.events()[0].event_id(), "event-1");
    assert_eq!(snapshot.events()[0].tags()[0].slug(), Some("weather"));
    assert_eq!(
        snapshot.events()[0].markets()[0].token_ids(),
        ["yes-token", "no-token"]
    );
    let market = &snapshot.events()[0].markets()[0];
    assert_eq!(market.price_tick(), Some("0.001"));
    assert_eq!(market.minimum_order_size(), Some("5"));
    assert_eq!(market.fees_enabled(), Some(true));
    let schedule = market.fee_schedule().expect("public fee schedule");
    assert_eq!(schedule.rate(), "0.05");
    assert_eq!(schedule.exponent(), "1");
    assert!(schedule.taker_only());
    assert_eq!(schedule.rebate_rate(), "0.25");
}
