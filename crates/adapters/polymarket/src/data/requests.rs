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

use std::{collections::BTreeSet, sync::Arc};

use anyhow::Context;
use futures_util::{StreamExt, stream};
use nautilus_common::{
    live::get_runtime,
    messages::{
        DataEvent, DataResponse,
        data::{
            BookResponse, CustomDataResponse, InstrumentResponse, InstrumentsResponse,
            RequestBookSnapshot, RequestCustomData, RequestInstrument, RequestInstruments,
            RequestTrades, TradesResponse,
        },
    },
};
use nautilus_core::{Params, datetime::datetime_to_unix_nanos};
use nautilus_model::{data::CustomData, instruments::Instrument};

use super::{
    PolymarketDataClient,
    dispatch::WsMessageContext,
    instruments::{cache_instrument_if_active, cache_instruments_if_active},
};
use crate::{
    common::consts::POLYMARKET_VENUE,
    data_types::{
        POLYMARKET_CLOB_MARKET_INFO_SNAPSHOT_TYPE_NAME,
        POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME, PolymarketClobMarketInfoSnapshot,
        PolymarketEventDefinitionSnapshot,
    },
    http::query::GetGammaEventsParams,
    providers::extract_condition_id,
    resolve::{
        PolymarketResolveRequestSummaryData, RESOLVE_REQUEST_TYPE_NAME, ResolveBatchErrorMode,
        ResolveRequestSummary, ResolveWatchSelectionMode, collect_resolve_watch_selection,
        fetch_and_apply_resolutions_by_condition_ids, parse_condition_ids_from_request_params,
        pause_resolve_watch_entries, request_params_has_explicit_condition_selector,
    },
};

const MAX_CLOB_MARKET_INFO_CONDITIONS: usize = 2_000;
const MAX_CLOB_CONDITION_ID_BYTES: usize = 512;

pub(super) fn request_data(client: &PolymarketDataClient, request: RequestCustomData) {
    if request.data_type.type_name() == POLYMARKET_CLOB_MARKET_INFO_SNAPSHOT_TYPE_NAME {
        request_clob_market_info_snapshot(client, request);
        return;
    }
    if request.data_type.type_name() == POLYMARKET_EVENT_DEFINITION_SNAPSHOT_TYPE_NAME {
        request_event_definition_snapshot(client, request);
        return;
    }

    if request.data_type.type_name() != RESOLVE_REQUEST_TYPE_NAME {
        log::debug!(
            "Ignoring unsupported custom data request type: {}",
            request.data_type.type_name()
        );
        return;
    }

    let RequestCustomData {
        data_type,
        request_id,
        client_id,
        params: request_params,
        start,
        end,
        ..
    } = request;

    let gamma_client = client.provider.http_client().clone();
    let sender = client.data_sender.clone();
    let start_nanos = datetime_to_unix_nanos(start);
    let end_nanos = datetime_to_unix_nanos(end);
    let clock = client.clock;
    let watchlist = client.resolve_poll_watchlist.clone();
    let resolve_poll_enabled = client.config.resolve_poll_enabled;
    let grace_secs = client.config.resolve_poll_grace_secs;
    let max_wait_secs = client.config.resolve_poll_max_wait_secs.max(grace_secs);
    let ctx = WsMessageContext {
        clock: client.clock,
        data_sender: client.data_sender.clone(),
        frame_counter: client.frame_counter.clone(),
        token_meta: client.token_meta.clone(),
        instruments: client.instruments.clone(),
        gamma_client: client.provider.http_client().clone(),
        clob_public_client: client.clob_public_client.clone(),
        filters: client.provider.filters(),
        order_books: client.order_books.clone(),
        last_quotes: client.last_quotes.clone(),
        active_quote_subs: client.active_quote_subs.clone(),
        active_delta_subs: client.active_delta_subs.clone(),
        active_trade_subs: client.active_trade_subs.clone(),
        resolve_poll_watchlist: client.resolve_poll_watchlist.clone(),
        resolve_watch_apply_mutex: client.resolve_watch_apply_mutex.clone(),
        pending_snapshot_after_tick_change: client.pending_snapshot_after_tick_change.clone(),
        expected_book_sources: client.expected_book_sources.clone(),
        ready_book_sources: client.ready_book_sources.clone(),
        book_epochs: client.book_epochs.clone(),
        book_readiness_mutex: client.book_readiness_mutex.clone(),
        market_data_shutdown: client.market_data_shutdown.clone(),
        new_market_inflight_keys: client.new_market_inflight_keys.clone(),
        new_market_fetch_semaphore: client.new_market_fetch_semaphore.clone(),
        rtds_feed: client.rtds_feed.clone(),
        subscribe_new_markets: client.config.subscribe_new_markets,
        drop_quotes_missing_side: client.config.drop_quotes_missing_side,
        new_market_filter: client.config.new_market_filter.clone(),
        cancellation_token: client.cancellation_token.clone(),
    };

    get_runtime().spawn(async move {
        let mut summary = ResolveRequestSummary {
            requested_condition_ids: Vec::new(),
            fetched_markets: 0,
            resolved_markets: 0,
            skipped_non_binary_markets: 0,
            clob_fallback_successes: 0,
            emitted_condition_ids: Vec::new(),
            failed_condition_ids: Vec::new(),
            used_watchlist_fallback: false,
            timed_out_watchlist: 0,
            error: None,
        };

        let has_explicit_selector =
            request_params_has_explicit_condition_selector(&request_params);
        let mut condition_ids = parse_condition_ids_from_request_params(&request_params);
        if condition_ids.is_empty() {
            if has_explicit_selector {
                summary.error = Some(
                    "No valid Polymarket condition_ids could be resolved from request params"
                        .to_string(),
                );
            } else {
                summary.used_watchlist_fallback = true;
                let snapshot = watchlist.load();
                let selection_mode = if resolve_poll_enabled {
                    ResolveWatchSelectionMode::ManualFallback
                } else {
                    ResolveWatchSelectionMode::ManualAllEligible
                };
                let selection = collect_resolve_watch_selection(
                    &snapshot,
                    clock.get_time_ns(),
                    grace_secs,
                    max_wait_secs,
                    selection_mode,
                );
                drop(snapshot);

                pause_resolve_watch_entries(&watchlist, &selection.pause_condition_ids);
                summary.timed_out_watchlist = selection.timed_out_watchlist;
                condition_ids = selection.condition_ids;
            }
        }

        summary.requested_condition_ids = condition_ids.clone();

        let stats = fetch_and_apply_resolutions_by_condition_ids(
            &gamma_client,
            &ctx.clob_public_client,
            &ctx.resolve_context(),
            &condition_ids,
            ResolveBatchErrorMode::StopOnFirstError,
        )
        .await;
        summary.fetched_markets = stats.fetched_markets;
        summary.resolved_markets = stats.resolved_markets;
        summary.skipped_non_binary_markets = stats.skipped_non_binary_markets;
        summary.clob_fallback_successes = stats.clob_fallback_successes;
        summary.emitted_condition_ids = stats.emitted_condition_ids;
        summary.failed_condition_ids = stats.failed_condition_ids;
        if summary.error.is_none() {
            summary.error = stats.error;
        }

        log::debug!(
            "Polymarket manual resolve request requested={} fetched={} resolved={} emitted={} failed={} skipped_non_binary={} clob_fallback_successes={} timed_out_watchlist={} used_watchlist_fallback={}",
            summary.requested_condition_ids.len(),
            summary.fetched_markets,
            summary.resolved_markets,
            summary.emitted_condition_ids.len(),
            summary.failed_condition_ids.len(),
            summary.skipped_non_binary_markets,
            summary.clob_fallback_successes,
            summary.timed_out_watchlist,
            summary.used_watchlist_fallback,
        );

        let ts_now = clock.get_time_ns();
        let payload = Arc::new(PolymarketResolveRequestSummaryData::from_summary(
            summary, ts_now,
        ));
        let custom = CustomData::new(payload, data_type.clone());

        let response = DataResponse::Data(CustomDataResponse::new(
            request_id,
            client_id,
            Some(*POLYMARKET_VENUE),
            data_type,
            custom,
            start_nanos,
            end_nanos,
            ts_now,
            request_params,
        ));

        if let Err(e) = sender.send(DataEvent::Response(response)) {
            log::error!("Failed to send resolve custom data response: {e}");
        }
    });
}

fn request_clob_market_info_snapshot(client: &PolymarketDataClient, request: RequestCustomData) {
    if request.start.is_some() || request.end.is_some() || request.limit.is_some() {
        log::error!(
            "Rejected bounded Polymarket CLOB market-info request {} with range or limit",
            request.request_id,
        );
        return;
    }
    let condition_ids = match parse_exact_clob_condition_ids(&request.params) {
        Ok(condition_ids) => condition_ids,
        Err(error) => {
            log::error!(
                "Rejected Polymarket CLOB market-info request {}: {error}",
                request.request_id,
            );
            return;
        }
    };

    let RequestCustomData {
        data_type,
        request_id,
        client_id,
        params: request_params,
        start,
        end,
        ..
    } = request;
    let clob_client = client.clob_public_client.clone();
    let sender = client.data_sender.clone();
    let clock = client.clock;
    let start_nanos = datetime_to_unix_nanos(start);
    let end_nanos = datetime_to_unix_nanos(end);

    get_runtime().spawn(async move {
        let results = stream::iter(condition_ids.into_iter().map(|condition_id| {
            let clob_client = clob_client.clone();
            async move {
                let response = clob_client
                    .get_clob_market_info(&condition_id)
                    .await
                    .with_context(|| format!("CLOB market info {condition_id}"))?;
                anyhow::ensure!(
                    response.condition_id == condition_id,
                    "CLOB market-info condition identity mismatch"
                );
                Ok::<_, anyhow::Error>(response)
            }
        }))
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
        let responses = match results.into_iter().collect::<anyhow::Result<Vec<_>>>() {
            Ok(responses) => responses,
            Err(error) => {
                log::error!("Failed Polymarket CLOB market-info request {request_id}: {error}");
                return;
            }
        };
        let ts_now = clock.get_time_ns();
        let snapshot = match PolymarketClobMarketInfoSnapshot::try_new(responses, ts_now) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::error!(
                    "Invalid Polymarket CLOB market-info response for request {request_id}: {error}"
                );
                return;
            }
        };
        let payload = Arc::new(snapshot);
        let custom = CustomData::new(payload, data_type.clone());
        let response = DataResponse::Data(CustomDataResponse::new(
            request_id,
            client_id,
            Some(*POLYMARKET_VENUE),
            data_type,
            custom,
            start_nanos,
            end_nanos,
            ts_now,
            request_params,
        ));
        if let Err(error) = sender.send(DataEvent::Response(response)) {
            log::error!("Failed to send Polymarket CLOB market-info snapshot: {error}");
        }
    });
}

fn parse_exact_clob_condition_ids(params: &Option<Params>) -> anyhow::Result<Vec<String>> {
    let params = params
        .as_ref()
        .context("missing exact condition selector")?;
    anyhow::ensure!(!params.is_empty(), "missing exact condition selector");
    anyhow::ensure!(
        params
            .keys()
            .all(|key| key == "condition_id" || key == "condition_ids"),
        "unsupported market-info request parameter",
    );

    let mut condition_ids = Vec::new();
    if let Some(value) = params.get("condition_id") {
        let value = value.as_str().context("condition_id must be a string")?;
        condition_ids.push(value.to_string());
    }
    if let Some(value) = params.get("condition_ids") {
        match value {
            serde_json::Value::String(value) => condition_ids.push(value.clone()),
            serde_json::Value::Array(values) => {
                anyhow::ensure!(
                    values.len() <= MAX_CLOB_MARKET_INFO_CONDITIONS,
                    "condition_ids exceeds the raw request bound",
                );
                for value in values {
                    condition_ids.push(
                        value
                            .as_str()
                            .context("every condition_ids entry must be a string")?
                            .to_string(),
                    );
                }
            }
            _ => anyhow::bail!("condition_ids must be a string or array of strings"),
        }
    }
    anyhow::ensure!(
        !condition_ids.is_empty() && condition_ids.len() <= MAX_CLOB_MARKET_INFO_CONDITIONS,
        "market-info request requires 1..={MAX_CLOB_MARKET_INFO_CONDITIONS} condition ids",
    );
    for condition_id in &condition_ids {
        anyhow::ensure!(
            !condition_id.is_empty()
                && condition_id.len() <= MAX_CLOB_CONDITION_ID_BYTES
                && condition_id.trim() == condition_id,
            "condition id is empty, non-canonical, or exceeds the text bound",
        );
    }
    condition_ids.sort();
    condition_ids.dedup();
    Ok(condition_ids)
}

fn request_event_definition_snapshot(client: &PolymarketDataClient, request: RequestCustomData) {
    if request.start.is_some()
        || request.end.is_some()
        || request.limit.is_some()
        || request
            .params
            .as_ref()
            .is_some_and(|params| !params.is_empty())
    {
        log::error!(
            "Polymarket event definition snapshots use the adapter-owned complete weather scope"
        );
        return;
    }

    let RequestCustomData {
        data_type,
        request_id,
        client_id,
        params: request_params,
        start,
        end,
        ..
    } = request;
    let gamma_client = client.provider.http_client().clone();
    let sender = client.data_sender.clone();
    let instruments_cache = client.instruments.clone();
    let token_meta = client.token_meta.clone();
    let clock = client.clock;
    let start_nanos = datetime_to_unix_nanos(start);
    let end_nanos = datetime_to_unix_nanos(end);

    get_runtime().spawn(async move {
        let params = GetGammaEventsParams {
            active: Some(true),
            closed: Some(false),
            archived: Some(false),
            tag_slug: Some("weather".to_string()),
            max_events: Some(10_001),
            ..Default::default()
        };
        let (definitions, instruments) = match gamma_client
            .request_event_definitions_with_instruments_by_params(params)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                log::error!("Failed to request complete Polymarket event definitions: {error}");
                return;
            }
        };
        let ts_now = clock.get_time_ns();
        let previously_cached = instruments_cache
            .load()
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let active_instruments = cache_instruments_if_active(
            ts_now,
            &instruments_cache,
            &token_meta,
            instruments,
        )
        .into_iter()
        .filter(|instrument| !previously_cached.contains(&instrument.id()))
        .collect::<Vec<_>>();
        let snapshot = match PolymarketEventDefinitionSnapshot::try_new(definitions, ts_now) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::error!("Invalid complete Polymarket event definition snapshot: {error}");
                return;
            }
        };
        let payload = Arc::new(snapshot);
        let custom = CustomData::new(payload, data_type.clone());
        let response = DataResponse::Data(CustomDataResponse::new(
            request_id,
            client_id,
            Some(*POLYMARKET_VENUE),
            data_type,
            custom,
            start_nanos,
            end_nanos,
            ts_now,
            request_params,
        ));
        if let Err(error) = sender.send(DataEvent::Response(response)) {
            log::error!("Failed to send Polymarket event definition snapshot: {error}");
            return;
        }
        let published = active_instruments.len();
        for instrument in active_instruments {
            if let Err(error) = sender.send(DataEvent::Instrument(instrument)) {
                log::error!("Failed to publish snapshot-hydrated Polymarket instrument: {error}");
                return;
            }
        }
        log::debug!(
            "Hydrated the complete Polymarket event snapshot and published {published} new instruments"
        );
    });
}

pub(super) fn request_instruments(client: &PolymarketDataClient, request: RequestInstruments) {
    let sender = client.data_sender.clone();
    let http = client.provider.http_client().clone();
    let filters = client.provider.filters();
    let instrument_config = client.provider.config().clone();
    let instruments_cache = client.instruments.clone();
    let token_meta = client.token_meta.clone();
    let request_id = request.request_id;
    let client_id = request.client_id.unwrap_or(client.client_id);
    let venue = *POLYMARKET_VENUE;
    let start_nanos = datetime_to_unix_nanos(request.start);
    let end_nanos = datetime_to_unix_nanos(request.end);
    let params = request.params;
    let clock = client.clock;

    get_runtime().spawn(async move {
        let instruments = if instrument_config.should_load_all() || instrument_config.has_load_ids()
        {
            crate::providers::fetch_configured_instruments(&http, &instrument_config, &filters)
                .await
        } else {
            crate::providers::fetch_instruments(&http, &filters).await
        };

        let instruments = match instruments {
            Ok(instruments) => instruments,
            Err(e) => {
                log::error!("Failed to fetch Polymarket instruments: {e}");
                return;
            }
        };

        for instrument in &instruments {
            if !cache_instrument_if_active(
                clock.get_time_ns(),
                &instruments_cache,
                &token_meta,
                instrument,
            ) {
                log::debug!(
                    "Skipping expired instrument {} during request_instruments cache update",
                    instrument.id()
                );
            }
        }

        let response = DataResponse::Instruments(InstrumentsResponse::new(
            request_id,
            client_id,
            venue,
            instruments,
            start_nanos,
            end_nanos,
            clock.get_time_ns(),
            params,
        ));

        if let Err(e) = sender.send(DataEvent::Response(response)) {
            log::error!("Failed to send instruments response: {e}");
        }
    });
}

pub(super) fn request_instrument(client: &PolymarketDataClient, request: RequestInstrument) {
    let instrument_id = request.instrument_id;
    let http = client.provider.http_client().clone();
    let sender = client.data_sender.clone();
    let instruments_cache = client.instruments.clone();
    let token_meta = client.token_meta.clone();
    let client_id = request.client_id.unwrap_or(client.client_id);
    let request_id = request.request_id;
    let start = request.start;
    let end = request.end;
    let params = request.params;
    let clock = client.clock;

    get_runtime().spawn(async move {
        let condition_id = match extract_condition_id(&instrument_id) {
            Ok(cid) => cid,
            Err(e) => {
                log::error!("Failed to extract condition_id for {instrument_id}: {e}");
                return;
            }
        };

        let query_params = crate::http::query::GetGammaMarketsParams {
            condition_ids: Some(vec![condition_id]),
            ..Default::default()
        };

        let instrument = match http.request_instruments_by_params(query_params).await {
            Ok(instruments) => instruments.into_iter().find(|i| i.id() == instrument_id),
            Err(e) => {
                log::error!("Failed to fetch instrument {instrument_id} from Gamma API: {e}");
                return;
            }
        };

        if let Some(inst) = instrument {
            if cache_instrument_if_active(clock.get_time_ns(), &instruments_cache, &token_meta, &inst)
            {
                // Publish onto the data bus so other clients (e.g. the exec
                // client's token map) can update from the same fetch.
                if let Err(e) = sender.send(DataEvent::Instrument(inst.clone())) {
                    log::warn!("Failed to publish instrument {instrument_id}: {e}");
                }
            } else {
                log::debug!(
                    "Skipping expired instrument {instrument_id} during request_instrument cache update"
                );
            }

            let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                request_id,
                client_id,
                instrument_id,
                inst,
                datetime_to_unix_nanos(start),
                datetime_to_unix_nanos(end),
                clock.get_time_ns(),
                params,
            )));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send instrument response: {e}");
            }
        } else {
            log::error!("Instrument {instrument_id} not found on Polymarket");
        }
    });
}

pub(super) fn request_book_snapshot(
    client: &PolymarketDataClient,
    request: RequestBookSnapshot,
) -> anyhow::Result<()> {
    let instrument_id = request.instrument_id;
    let instrument = client.ensure_market_data_request_allowed(instrument_id)?;

    let token_id = instrument.raw_symbol().as_str().to_string();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let clob_client = client.clob_public_client.clone();
    let sender = client.data_sender.clone();
    let client_id = request.client_id.unwrap_or(client.client_id);
    let request_id = request.request_id;
    let params = request.params;
    let clock = client.clock;

    get_runtime().spawn(async move {
        match clob_client
            .request_book_snapshot(instrument_id, &token_id, price_precision, size_precision)
            .await
            .context("failed to request book snapshot from Polymarket")
        {
            Ok(book) => {
                let response = DataResponse::Book(BookResponse::new(
                    request_id,
                    client_id,
                    instrument_id,
                    book,
                    None,
                    None,
                    clock.get_time_ns(),
                    params,
                ));

                if let Err(e) = sender.send(DataEvent::Response(response)) {
                    log::error!("Failed to send book snapshot response: {e}");
                }
            }
            Err(e) => log::error!("Book snapshot request failed: {e:?}"),
        }
    });

    Ok(())
}

pub(super) fn request_trades(
    client: &PolymarketDataClient,
    request: RequestTrades,
) -> anyhow::Result<()> {
    let instrument_id = request.instrument_id;
    let instrument = client.ensure_market_data_request_allowed(instrument_id)?;

    let condition_id = extract_condition_id(&instrument_id)?;
    let token_id = instrument.raw_symbol().as_str().to_string();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let limit = request.limit.map(|n| n.get() as u32);

    let data_api_client = client.data_api_client.clone();
    let sender = client.data_sender.clone();
    let client_id = request.client_id.unwrap_or(client.client_id);
    let request_id = request.request_id;
    let params = request.params;
    let clock = client.clock;
    let start_nanos = datetime_to_unix_nanos(request.start);
    let end_nanos = datetime_to_unix_nanos(request.end);

    get_runtime().spawn(async move {
        match data_api_client
            .request_trade_ticks(
                instrument_id,
                &condition_id,
                &token_id,
                price_precision,
                size_precision,
                start_nanos,
                end_nanos,
                limit,
            )
            .await
            .context("failed to request trades from Polymarket Data API")
        {
            Ok(trades) => {
                let response = DataResponse::Trades(TradesResponse::new(
                    request_id,
                    client_id,
                    instrument_id,
                    trades,
                    start_nanos,
                    end_nanos,
                    clock.get_time_ns(),
                    params,
                ));

                if let Err(e) = sender.send(DataEvent::Response(response)) {
                    log::error!("Failed to send trades response: {e}");
                }
            }
            Err(e) => {
                log::error!("Trade request failed for {instrument_id}: {e:?}");

                let response = DataResponse::Trades(TradesResponse::new(
                    request_id,
                    client_id,
                    instrument_id,
                    Vec::new(),
                    start_nanos,
                    end_nanos,
                    clock.get_time_ns(),
                    params,
                ));

                if let Err(e) = sender.send(DataEvent::Response(response)) {
                    log::error!("Failed to send empty trades response: {e}");
                }
            }
        }
    });

    Ok(())
}

#[cfg(test)]
mod clob_market_info_request_tests {
    use nautilus_core::Params;
    use rstest::rstest;

    use super::{MAX_CLOB_MARKET_INFO_CONDITIONS, parse_exact_clob_condition_ids};

    #[rstest]
    fn exact_condition_selectors_are_canonicalized() {
        let mut params = Params::new();
        params.insert("condition_id".to_string(), serde_json::json!("0x02"));
        params.insert(
            "condition_ids".to_string(),
            serde_json::json!(["0x02", "0x01"]),
        );

        assert_eq!(
            parse_exact_clob_condition_ids(&Some(params)).expect("exact selectors"),
            ["0x01".to_string(), "0x02".to_string()],
        );
    }

    #[rstest]
    fn malformed_selector_is_not_partially_accepted() {
        let mut params = Params::new();
        params.insert("condition_id".to_string(), serde_json::json!("0x01"));
        params.insert("condition_ids".to_string(), serde_json::json!(["0x02", 3]));

        assert!(parse_exact_clob_condition_ids(&Some(params)).is_err());
    }

    #[rstest]
    fn raw_selector_count_is_bounded_before_deduplication() {
        let mut params = Params::new();
        params.insert(
            "condition_ids".to_string(),
            serde_json::json!(vec!["0x01"; MAX_CLOB_MARKET_INFO_CONDITIONS + 1]),
        );

        assert!(parse_exact_clob_condition_ids(&Some(params)).is_err());
    }
}
