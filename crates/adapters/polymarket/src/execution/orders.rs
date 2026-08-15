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

use nautilus_common::{
    cache::InstrumentLookupError,
    messages::execution::{ModifyOrder, SubmitOrder, SubmitOrderList},
};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderType, TimeInForce},
    identifiers::VenueOrderId,
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::{
    PolymarketExecutionClient,
    activation::{ExpectedSubmitActivation, activate_expected_submit},
    cancellations::execute_deferred_cancel,
    order_builder::PolymarketOrderBuilder,
    parse::{compute_commission, instrument_fee_exponent, instrument_taker_fee},
    reports::fetch_collateral_balance_pusd,
    responses::{
        check_fok_status, emit_market_order_submitted, handle_batch_order_responses,
        handle_order_response, handle_single_order_response, handle_unknown_submit_result,
        reject_submit_order,
    },
    submitter::{MarketBuyFeeContext, MarketOrderSubmitRequest, UnknownSubmitError},
    types::{
        BatchLimitOrderContext, LimitOrderSubmitRequest, PreparedLimitHttpRequest,
        SignedLimitOrderSubmission,
    },
};
use crate::{
    common::consts::{BATCH_ORDER_LIMIT, POLYMARKET_PREPARE_ALL_OR_NONE_PARAM},
    evidence::{
        PolymarketEvidenceBridge, PolymarketEvidenceError, PolymarketHandoffStarted,
        PolymarketMutationEndpoint, PolymarketMutationEvidence, PolymarketSignedLimitEvidence,
        PolymarketSubmitPrepared,
    },
};

fn requires_prepare_all_or_none(cmd: &SubmitOrderList) -> bool {
    cmd.params
        .as_ref()
        .and_then(|params| params.get_bool(POLYMARKET_PREPARE_ALL_OR_NONE_PARAM))
        == Some(true)
}

fn deny_prepare_all_or_none_batch(
    emitter: &nautilus_live::ExecutionEventEmitter,
    orders: &[OrderAny],
    reason: &str,
) {
    for order in orders {
        emitter.emit_order_denied(order, reason);
    }
}

fn prepared_request_matches_orders(
    prepared: &PreparedLimitHttpRequest,
    orders: &[BatchLimitOrderContext],
) -> bool {
    prepared.client_order_ids().iter().copied().eq(orders
        .iter()
        .map(|batch_order| batch_order.order.client_order_id()))
}

fn build_submit_prepared_evidence(
    prepared: &PreparedLimitHttpRequest,
    submissions: &[SignedLimitOrderSubmission],
    orders: &[OrderAny],
) -> Result<PolymarketSubmitPrepared, PolymarketEvidenceError> {
    if submissions.len() != orders.len()
        || submissions.len() != prepared.expected_venue_order_ids().len()
    {
        return Err(PolymarketEvidenceError::InvalidFact);
    }
    let endpoint = match prepared.endpoint() {
        super::types::LimitHttpRequestEndpoint::Single => PolymarketMutationEndpoint::Single,
        super::types::LimitHttpRequestEndpoint::Batch => PolymarketMutationEndpoint::Batch,
    };
    let legs = submissions
        .iter()
        .zip(orders)
        .map(|(submission, order)| {
            PolymarketSignedLimitEvidence::new(
                submission.order().clone(),
                submission.order_type(),
                submission.post_only(),
                submission.expected_venue_order_id(),
                order.client_order_id(),
            )
        })
        .collect();
    PolymarketSubmitPrepared::try_new(endpoint, prepared.body_sha256(), legs)
}

async fn append_mutation_evidence(
    bridge: &dyn PolymarketEvidenceBridge,
    fact: &PolymarketMutationEvidence<'_>,
) -> Result<(), PolymarketEvidenceError> {
    let ack = bridge.append_mutation(fact).await?;
    if ack.fact_id() != fact.fact_id() {
        return Err(PolymarketEvidenceError::InvalidAcknowledgement);
    }
    Ok(())
}

impl PolymarketExecutionClient {
    pub(super) fn submit_limit_order(&self, order: OrderAny) {
        if let Err(reason) = PolymarketOrderBuilder::validate_limit_order(&order) {
            self.emitter.emit_order_denied(&order, &reason);
            return;
        }

        let instrument = match self.resolve_instrument(&order) {
            Some(i) => i,
            None => return,
        };

        if let Err(reason) =
            PolymarketOrderBuilder::validate_limit_price(&order, instrument.price_increment())
        {
            self.emitter.emit_order_denied(&order, &reason);
            return;
        }

        let neg_risk = self.get_neg_risk(&order.instrument_id());
        let token_id = instrument.raw_symbol().to_string();
        let tick_decimals = instrument.price_precision() as u32;
        let price = order.price().unwrap();
        let quantity = order.quantity();
        let tif = order.time_in_force();
        let post_only = order.is_post_only();
        let side = order.order_side();
        let expire_time = order.expire_time();
        let request = LimitOrderSubmitRequest {
            token_id,
            side,
            price,
            quantity,
            time_in_force: tif,
            post_only,
            neg_risk,
            expire_time,
            tick_decimals,
        };

        self.emitter.emit_order_submitted(&order);

        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let fill_tracker = self.fill_tracker.clone();
        let order_identities = self.order_identities.clone();
        let pending_submits = self.pending_submits.clone();
        let pending_cancels = self.pending_cancels.clone();
        let account_id = self.core.account_id;
        let size_precision = instrument.size_precision();
        let price_precision = instrument.price_precision();
        let pre_activate_expected_order_ids = self.config.pre_activate_expected_order_ids;
        let evidence_bridge = self.evidence_bridge.clone();

        self.spawn_task("submit_limit_order", async move {
            let submission = match submitter.prepare_limit_order_submission(&request).await {
                Ok(submission) => submission,
                Err(e) => {
                    reject_submit_order(&order, &format!("{e}"), &emitter, clock, &pending_cancels);
                    return Ok(());
                }
            };

            let prepared_request = match submitter
                .prepare_single_limit_http_request(&submission, order.client_order_id())
            {
                Ok(prepared) => prepared,
                Err(e) => {
                    reject_submit_order(&order, &format!("{e}"), &emitter, clock, &pending_cancels);
                    return Ok(());
                }
            };

            let prepared_evidence = if let Some(bridge) = &evidence_bridge {
                let evidence = match build_submit_prepared_evidence(
                    &prepared_request,
                    std::slice::from_ref(&submission),
                    std::slice::from_ref(&order),
                ) {
                    Ok(evidence) => evidence,
                    Err(error) => {
                        reject_submit_order(
                            &order,
                            &format!("Durable prepared evidence failed: {error}"),
                            &emitter,
                            clock,
                            &pending_cancels,
                        );
                        return Ok(());
                    }
                };
                if let Err(error) = append_mutation_evidence(
                    bridge.as_ref(),
                    &PolymarketMutationEvidence::SubmitPrepared(&evidence),
                )
                .await
                {
                    reject_submit_order(
                        &order,
                        &format!("Durable prepared evidence failed: {error}"),
                        &emitter,
                        clock,
                        &pending_cancels,
                    );
                    return Ok(());
                }
                Some(evidence)
            } else {
                None
            };

            let expected_venue_order_id = prepared_request.expected_venue_order_ids()[0];
            let mut activation = if pre_activate_expected_order_ids {
                match activate_expected_submit(
                    &order,
                    expected_venue_order_id,
                    &fill_tracker,
                    &order_identities,
                    &pending_submits,
                ) {
                    Ok(activation) => Some(activation),
                    Err(reason) => {
                        reject_submit_order(
                            &order,
                            &format!("Expected signed identity activation failed: {reason}"),
                            &emitter,
                            clock,
                            &pending_cancels,
                        );
                        return Ok(());
                    }
                }
            } else {
                None
            };
            if let (Some(bridge), Some(prepared_evidence)) = (&evidence_bridge, &prepared_evidence)
            {
                if !prepared_request.body_hash_matches() {
                    reject_submit_order(
                        &order,
                        "Prepared HTTP request body hash changed before durable handoff",
                        &emitter,
                        clock,
                        &pending_cancels,
                    );
                    return Ok(());
                }
                let handoff = PolymarketHandoffStarted::new(prepared_evidence);
                if let Err(error) = append_mutation_evidence(
                    bridge.as_ref(),
                    &PolymarketMutationEvidence::HandoffStarted(handoff),
                )
                .await
                {
                    reject_submit_order(
                        &order,
                        &format!("Durable HTTP handoff evidence failed: {error}"),
                        &emitter,
                        clock,
                        &pending_cancels,
                    );
                    return Ok(());
                }
            }
            if let Some(activation) = &mut activation {
                activation.mark_http_handoff_started();
            }
            match submitter
                .post_prepared_single_limit_request(prepared_request)
                .await
            {
                Ok(response) => {
                    if let Some((order_id_str, venue_order_id)) = handle_order_response(
                        Ok(response),
                        &order,
                        &emitter,
                        clock,
                        &fill_tracker,
                        &order_identities,
                        &pending_cancels,
                        account_id,
                        size_precision,
                        price_precision,
                    ) {
                        execute_deferred_cancel(
                            &submitter,
                            &order,
                            &order_id_str,
                            venue_order_id,
                            &emitter,
                            &pending_cancels,
                            clock,
                        )
                        .await;
                    }
                }
                Err(e) if e.is_submit_outcome_unknown() => {
                    if let Some((order_id_str, venue_order_id)) = handle_unknown_submit_result(
                        &order,
                        expected_venue_order_id,
                        &e.to_string(),
                        None,
                        &emitter,
                        clock,
                        &fill_tracker,
                        &order_identities,
                        &pending_submits,
                        &pending_cancels,
                        account_id,
                        size_precision,
                        price_precision,
                    ) {
                        execute_deferred_cancel(
                            &submitter,
                            &order,
                            &order_id_str,
                            venue_order_id,
                            &emitter,
                            &pending_cancels,
                            clock,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    reject_submit_order(&order, &format!("{e}"), &emitter, clock, &pending_cancels);
                }
            }
            Ok(())
        });
    }

    pub(super) fn submit_market_order(&self, order: OrderAny) {
        if let Err(reason) = PolymarketOrderBuilder::validate_market_order(&order) {
            self.emitter.emit_order_denied(&order, &reason);
            return;
        }

        let instrument = match self.resolve_instrument(&order) {
            Some(i) => i,
            None => return,
        };

        let neg_risk = self.get_neg_risk(&order.instrument_id());
        let token_id = instrument.raw_symbol().to_string();
        let tick_decimals = instrument.price_precision() as u32;
        let side = order.order_side();
        let amount = order.quantity();
        let time_in_force = order.time_in_force();
        let is_quote_qty = order.is_quote_quantity();

        let needs_fee_adjustment = side == OrderSide::Buy && is_quote_qty;
        let fee_rate = if needs_fee_adjustment {
            instrument_taker_fee(&instrument)
        } else {
            Decimal::ZERO
        };
        let fee_exponent = if needs_fee_adjustment {
            instrument_fee_exponent(&instrument)
        } else {
            1.0
        };

        let submitter = self.submitter.clone();
        let http_client = self.http_client.clone();
        let signature_type = self.config.signature_type;
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let fill_tracker = self.fill_tracker.clone();
        let order_identities = self.order_identities.clone();
        let pending_submits = self.pending_submits.clone();
        let pending_cancels = self.pending_cancels.clone();
        let account_id = self.core.account_id;
        let size_precision = instrument.size_precision();
        let price_precision = instrument.price_precision();

        self.spawn_task("submit_market_order", async move {
            let fee_context = if needs_fee_adjustment {
                match fetch_collateral_balance_pusd(&http_client, signature_type).await {
                    Ok(balance) => Some(MarketBuyFeeContext {
                        user_pusd_balance: balance,
                        fee_rate,
                        fee_exponent,
                        builder_taker_fee_rate: Decimal::ZERO,
                    }),
                    Err(e) => {
                        emitter.emit_order_denied(
                            &order,
                            &format!("Failed to fetch pUSD balance for fee adjustment: {e}"),
                        );
                        return Ok(());
                    }
                }
            } else {
                None
            };

            match submitter
                .submit_market_order(MarketOrderSubmitRequest {
                    token_id,
                    side,
                    amount,
                    time_in_force,
                    neg_risk,
                    tick_decimals,
                    fee_context,
                })
                .await
            {
                Ok(result) => {
                    let mut order = order;
                    emit_market_order_submitted(
                        &mut order,
                        is_quote_qty,
                        side,
                        amount,
                        result.expected_base_qty,
                        result.response.success,
                        size_precision,
                        &emitter,
                        clock,
                    );

                    if result.response.success
                        && let Some(order_id) = result.response.order_id.as_ref()
                    {
                        let venue_order_id = VenueOrderId::from(order_id.as_str());
                        if venue_order_id != result.expected_venue_order_id {
                            log::warn!(
                                "Market submit returned order ID {venue_order_id}, expected {}",
                                result.expected_venue_order_id
                            );
                        }
                    }

                    let fok_order_id = result
                        .response
                        .order_id
                        .as_ref()
                        .filter(|_| result.response.success && time_in_force == TimeInForce::Fok)
                        .cloned();

                    if let Some((order_id_str, venue_order_id)) = handle_order_response(
                        Ok(result.response),
                        &order,
                        &emitter,
                        clock,
                        &fill_tracker,
                        &order_identities,
                        &pending_cancels,
                        account_id,
                        size_precision,
                        price_precision,
                    ) {
                        execute_deferred_cancel(
                            &submitter,
                            &order,
                            &order_id_str,
                            venue_order_id,
                            &emitter,
                            &pending_cancels,
                            clock,
                        )
                        .await;
                    }

                    if let Some(order_id) = fok_order_id {
                        check_fok_status(
                            &submitter,
                            &order_id,
                            &order,
                            &fill_tracker,
                            &emitter,
                            account_id,
                            size_precision,
                            price_precision,
                            clock,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    if let Some(unknown) = e.downcast_ref::<UnknownSubmitError>() {
                        let mut order = order;
                        emit_market_order_submitted(
                            &mut order,
                            is_quote_qty,
                            side,
                            amount,
                            unknown.expected_base_qty.unwrap_or_default(),
                            true,
                            size_precision,
                            &emitter,
                            clock,
                        );

                        let fill_tracker_quantity = if is_quote_qty && side == OrderSide::Buy {
                            unknown
                                .expected_base_qty
                                .and_then(|qty| Quantity::from_decimal_dp(qty, size_precision).ok())
                        } else {
                            None
                        };

                        if let Some((order_id_str, venue_order_id)) = handle_unknown_submit_result(
                            &order,
                            unknown.expected_venue_order_id,
                            &unknown.reason,
                            fill_tracker_quantity,
                            &emitter,
                            clock,
                            &fill_tracker,
                            &order_identities,
                            &pending_submits,
                            &pending_cancels,
                            account_id,
                            size_precision,
                            price_precision,
                        ) {
                            execute_deferred_cancel(
                                &submitter,
                                &order,
                                &order_id_str,
                                venue_order_id,
                                &emitter,
                                &pending_cancels,
                                clock,
                            )
                            .await;
                        }
                    } else {
                        let ts_now = clock.get_time_ns();
                        emitter.emit_order_rejected(&order, &format!("{e}"), ts_now, false);
                    }
                }
            }
            Ok(())
        });
    }

    pub(super) fn resolve_instrument(&self, order: &OrderAny) -> Option<InstrumentAny> {
        let instrument = self
            .core
            .cache()
            .instrument(&order.instrument_id())
            .cloned();

        match instrument {
            Some(i) => Some(i),
            None => {
                self.emitter.emit_order_denied(
                    order,
                    &InstrumentLookupError::not_found(order.instrument_id()).to_string(),
                );
                None
            }
        }
    }

    pub(super) fn submit_order_command(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        match order.order_type() {
            OrderType::Limit => self.submit_limit_order(order),
            OrderType::Market if self.evidence_bridge.is_some() => self.emitter.emit_order_denied(
                &order,
                "Native market orders are unavailable on the durable evidence path; use an explicit aggressive LIMIT FOK",
            ),
            OrderType::Market => self.submit_market_order(order),
            _ => {
                self.emitter.emit_order_denied(
                    &order,
                    &format!(
                        "Unsupported order type for Polymarket: {:?}",
                        order.order_type()
                    ),
                );
            }
        }
        Ok(())
    }

    pub(super) fn submit_order_list_command(&self, cmd: &SubmitOrderList) {
        let mut batch_orders = Vec::with_capacity(cmd.order_inits.len());
        let prepare_all_or_none = requires_prepare_all_or_none(cmd);
        let mut plan_orders = Vec::with_capacity(cmd.order_inits.len());
        let mut plan_failure = if self.evidence_bridge.is_some() && !prepare_all_or_none {
            Some(
                "Durable Polymarket evidence requires a prepare-all-or-none order list".to_string(),
            )
        } else if prepare_all_or_none && !(1..=BATCH_ORDER_LIMIT).contains(&cmd.order_inits.len()) {
            Some(format!(
                "Prepare-all-or-none order list must contain 1..={BATCH_ORDER_LIMIT} orders, found {}",
                cmd.order_inits.len()
            ))
        } else {
            None
        };
        let neg_risk_index = self.neg_risk_index.load();

        for order_init in &cmd.order_inits {
            let Some(order) = self
                .core
                .cache()
                .order(&order_init.client_order_id)
                .map(|o| o.clone())
            else {
                log::warn!(
                    "Order not found in cache for {}",
                    order_init.client_order_id
                );
                if prepare_all_or_none && plan_failure.is_none() {
                    plan_failure = Some(format!(
                        "Prepare-all-or-none order {} was not found in cache",
                        order_init.client_order_id
                    ));
                }
                continue;
            };

            if order.is_closed() {
                log::warn!("Cannot submit closed order {}", order.client_order_id());
                if prepare_all_or_none && plan_failure.is_none() {
                    plan_failure = Some(format!(
                        "Prepare-all-or-none order {} is already closed",
                        order.client_order_id()
                    ));
                }
                continue;
            }

            if prepare_all_or_none {
                plan_orders.push(order.clone());
            }

            match order.order_type() {
                OrderType::Limit => {}
                OrderType::Market => {
                    if prepare_all_or_none {
                        if plan_failure.is_none() {
                            plan_failure = Some(format!(
                                "Prepare-all-or-none order {} has unsupported order type Market",
                                order.client_order_id()
                            ));
                        }
                        continue;
                    }
                    self.submit_market_order(order);
                    continue;
                }
                other => {
                    let reason = format!("Unsupported order type for Polymarket: {other:?}");
                    if prepare_all_or_none {
                        if plan_failure.is_none() {
                            plan_failure = Some(format!(
                                "Prepare-all-or-none order {} failed validation: {reason}",
                                order.client_order_id()
                            ));
                        }
                    } else {
                        self.emitter.emit_order_denied(&order, &reason);
                    }
                    continue;
                }
            }

            if let Err(reason) = PolymarketOrderBuilder::validate_limit_order(&order) {
                if prepare_all_or_none {
                    if plan_failure.is_none() {
                        plan_failure = Some(format!(
                            "Prepare-all-or-none order {} failed validation: {reason}",
                            order.client_order_id()
                        ));
                    }
                } else {
                    self.emitter.emit_order_denied(&order, &reason);
                }
                continue;
            }

            let instrument = if prepare_all_or_none {
                match self
                    .core
                    .cache()
                    .instrument(&order.instrument_id())
                    .cloned()
                {
                    Some(instrument) => instrument,
                    None => {
                        let reason =
                            InstrumentLookupError::not_found(order.instrument_id()).to_string();
                        if plan_failure.is_none() {
                            plan_failure = Some(format!(
                                "Prepare-all-or-none order {} failed resolution: {reason}",
                                order.client_order_id()
                            ));
                        }
                        continue;
                    }
                }
            } else {
                match self.resolve_instrument(&order) {
                    Some(instrument) => instrument,
                    None => continue,
                }
            };

            if let Err(reason) =
                PolymarketOrderBuilder::validate_limit_price(&order, instrument.price_increment())
            {
                if prepare_all_or_none {
                    if plan_failure.is_none() {
                        plan_failure = Some(format!(
                            "Prepare-all-or-none order {} failed validation: {reason}",
                            order.client_order_id()
                        ));
                    }
                } else {
                    self.emitter.emit_order_denied(&order, &reason);
                }
                continue;
            }

            let price = order
                .price()
                .expect("validated limit order must have a price");
            batch_orders.push(BatchLimitOrderContext {
                request: LimitOrderSubmitRequest {
                    token_id: instrument.raw_symbol().to_string(),
                    side: order.order_side(),
                    price,
                    quantity: order.quantity(),
                    time_in_force: order.time_in_force(),
                    post_only: order.is_post_only(),
                    neg_risk: Self::get_neg_risk_from_snapshot(
                        &neg_risk_index,
                        &order.instrument_id(),
                    ),
                    expire_time: order.expire_time(),
                    tick_decimals: instrument.price_precision() as u32,
                },
                size_precision: instrument.size_precision(),
                price_precision: instrument.price_precision(),
                order,
            });
        }

        if let Some(reason) = plan_failure {
            deny_prepare_all_or_none_batch(&self.emitter, &plan_orders, &reason);
            return;
        }

        if batch_orders.is_empty() {
            return;
        }

        if !prepare_all_or_none && batch_orders.len() == 1 {
            let batch_order = batch_orders.pop().expect("len checked");
            self.submit_limit_order(batch_order.order);
            return;
        }

        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let fill_tracker = self.fill_tracker.clone();
        let order_identities = self.order_identities.clone();
        let pending_submits = self.pending_submits.clone();
        let pending_cancels = self.pending_cancels.clone();
        let pending_tasks = self.pending_tasks.clone();
        let account_id = self.core.account_id;
        let pre_activate_expected_order_ids =
            self.config.pre_activate_expected_order_ids && prepare_all_or_none;
        let evidence_bridge = self.evidence_bridge.clone();

        self.spawn_task("submit_order_list", async move {
            if !prepare_all_or_none {
                for batch_order in &batch_orders {
                    emitter.emit_order_submitted(&batch_order.order);
                }
            }

            let requests: Vec<LimitOrderSubmitRequest> =
                batch_orders.iter().map(|bo| bo.request.clone()).collect();
            let prepare_results = submitter.prepare_limit_order_submissions(&requests).await;

            if prepare_all_or_none
                && let Some((failed_index, error)) = prepare_results
                    .iter()
                    .enumerate()
                    .find_map(|(index, result)| result.as_ref().err().map(|error| (index, error)))
            {
                let failed_order = &batch_orders[failed_index].order;
                let reason = format!(
                    "Prepare-all-or-none order {} failed preparation; no orders were submitted: {error}",
                    failed_order.client_order_id()
                );
                deny_prepare_all_or_none_batch(&emitter, &plan_orders, &reason);
                return Ok(());
            }

            let mut prepared_orders = Vec::with_capacity(batch_orders.len());
            let mut submissions = Vec::with_capacity(batch_orders.len());

            for (batch_order, result) in batch_orders.into_iter().zip(prepare_results) {
                match result {
                    Ok(submission) => {
                        prepared_orders.push(batch_order);
                        submissions.push(submission);
                    }
                    Err(e) => {
                        reject_submit_order(
                            &batch_order.order,
                            &format!("{e}"),
                            &emitter,
                            clock,
                            &pending_cancels,
                        );
                    }
                }
            }

            if submissions.is_empty() {
                return Ok(());
            }

            let mut prepared_all_request = if prepare_all_or_none {
                let client_order_ids = prepared_orders
                    .iter()
                    .map(|batch_order| batch_order.order.client_order_id())
                    .collect();
                let result = if submissions.len() == 1 {
                    submitter.prepare_single_limit_http_request(
                        &submissions[0],
                        prepared_orders[0].order.client_order_id(),
                    )
                } else {
                    submitter.prepare_batch_limit_http_request(&submissions, client_order_ids)
                };
                match result {
                    Ok(prepared) if prepared_request_matches_orders(&prepared, &prepared_orders) => {
                        Some(prepared)
                    }
                    Ok(_) => {
                        let reason = "Prepare-all-or-none exact HTTP request identity ordering mismatch; no orders were submitted";
                        deny_prepare_all_or_none_batch(&emitter, &plan_orders, reason);
                        return Ok(());
                    }
                    Err(error) => {
                        let reason = format!(
                            "Prepare-all-or-none exact HTTP request preparation failed; no orders were submitted: {error}"
                        );
                        deny_prepare_all_or_none_batch(&emitter, &plan_orders, &reason);
                        return Ok(());
                    }
                }
            } else {
                None
            };

            let prepared_evidence = if let (Some(bridge), Some(prepared_request)) =
                (&evidence_bridge, &prepared_all_request)
            {
                let orders: Vec<OrderAny> = prepared_orders
                    .iter()
                    .map(|batch_order| batch_order.order.clone())
                    .collect();
                let evidence = match build_submit_prepared_evidence(
                    prepared_request,
                    &submissions,
                    &orders,
                ) {
                    Ok(evidence) => evidence,
                    Err(error) => {
                        deny_prepare_all_or_none_batch(
                            &emitter,
                            &plan_orders,
                            &format!("Durable prepared evidence failed: {error}"),
                        );
                        return Ok(());
                    }
                };
                if let Err(error) = append_mutation_evidence(
                    bridge.as_ref(),
                    &PolymarketMutationEvidence::SubmitPrepared(&evidence),
                )
                .await
                {
                    deny_prepare_all_or_none_batch(
                        &emitter,
                        &plan_orders,
                        &format!("Durable prepared evidence failed: {error}"),
                    );
                    return Ok(());
                }
                Some(evidence)
            } else {
                None
            };

            let mut activations: Vec<ExpectedSubmitActivation> =
                Vec::with_capacity(submissions.len());
            if pre_activate_expected_order_ids {
                for (batch_order, submission) in prepared_orders.iter().zip(&submissions) {
                    match activate_expected_submit(
                        &batch_order.order,
                        submission.expected_venue_order_id,
                        &fill_tracker,
                        &order_identities,
                        &pending_submits,
                    ) {
                        Ok(activation) => activations.push(activation),
                        Err(reason) => {
                            let reason = format!(
                                "Prepare-all-or-none expected signed identity activation failed; no orders were submitted: {reason}"
                            );
                            deny_prepare_all_or_none_batch(&emitter, &plan_orders, &reason);
                            return Ok(());
                        }
                    }
                }
            }

            if prepare_all_or_none {
                for batch_order in &prepared_orders {
                    emitter.emit_order_submitted(&batch_order.order);
                }
            }

            if let (Some(bridge), Some(prepared_request), Some(prepared_evidence)) = (
                &evidence_bridge,
                &prepared_all_request,
                &prepared_evidence,
            ) {
                if !prepared_request.body_hash_matches() {
                    deny_prepare_all_or_none_batch(
                        &emitter,
                        &plan_orders,
                        "Prepared HTTP request body hash changed before durable handoff",
                    );
                    return Ok(());
                }
                let handoff = PolymarketHandoffStarted::new(prepared_evidence);
                if let Err(error) = append_mutation_evidence(
                    bridge.as_ref(),
                    &PolymarketMutationEvidence::HandoffStarted(handoff),
                )
                .await
                {
                    deny_prepare_all_or_none_batch(
                        &emitter,
                        &plan_orders,
                        &format!("Durable HTTP handoff evidence failed: {error}"),
                    );
                    return Ok(());
                }
            }

            let total = submissions.len();
            let mut offset = 0;
            while offset < total {
                let end = (offset + BATCH_ORDER_LIMIT).min(total);
                let mut submissions_chunk = submissions[offset..end].to_vec();
                let mut orders_chunk = prepared_orders[offset..end].to_vec();
                if pre_activate_expected_order_ids {
                    for activation in &mut activations[offset..end] {
                        activation.mark_http_handoff_started();
                    }
                }

                if submissions_chunk.len() == 1 {
                    let submission = submissions_chunk.pop().expect("len 1");
                    let batch_order = orders_chunk.pop().expect("len 1");
                    let prepared_request = if let Some(prepared) = prepared_all_request.take() {
                        prepared
                    } else {
                        match submitter.prepare_single_limit_http_request(
                            &submission,
                            batch_order.order.client_order_id(),
                        ) {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                reject_submit_order(
                                    &batch_order.order,
                                    &format!("{error}"),
                                    &emitter,
                                    clock,
                                    &pending_cancels,
                                );
                                offset = end;
                                continue;
                            }
                        }
                    };
                    let expected_venue_order_id = prepared_request.expected_venue_order_ids()[0];
                    handle_single_order_response(
                        submitter
                            .post_prepared_single_limit_request(prepared_request)
                            .await,
                        batch_order,
                        expected_venue_order_id,
                        &submitter,
                        &emitter,
                        clock,
                        &fill_tracker,
                        &order_identities,
                        &pending_submits,
                        &pending_cancels,
                        account_id,
                    )
                    .await;
                } else {
                    let prepared_request = if let Some(prepared) = prepared_all_request.take() {
                        prepared
                    } else {
                        let client_order_ids = orders_chunk
                            .iter()
                            .map(|batch_order| batch_order.order.client_order_id())
                            .collect();
                        match submitter.prepare_batch_limit_http_request(
                            &submissions_chunk,
                            client_order_ids,
                        ) {
                            Ok(prepared)
                                if prepared_request_matches_orders(&prepared, &orders_chunk) =>
                            {
                                prepared
                            }
                            Ok(_) => {
                                for batch_order in orders_chunk {
                                    reject_submit_order(
                                        &batch_order.order,
                                        "Prepared HTTP request identity ordering mismatch",
                                        &emitter,
                                        clock,
                                        &pending_cancels,
                                    );
                                }
                                offset = end;
                                continue;
                            }
                            Err(error) => {
                                for batch_order in orders_chunk {
                                    reject_submit_order(
                                        &batch_order.order,
                                        &format!("{error}"),
                                        &emitter,
                                        clock,
                                        &pending_cancels,
                                    );
                                }
                                offset = end;
                                continue;
                            }
                        }
                    };
                    let expected_venue_order_ids =
                        prepared_request.expected_venue_order_ids().to_vec();

                    match submitter
                        .post_prepared_batch_limit_request(prepared_request)
                        .await
                    {
                        Ok(responses) => {
                            handle_batch_order_responses(
                                responses,
                                orders_chunk,
                                expected_venue_order_ids,
                                &submitter,
                                &emitter,
                                clock,
                                &fill_tracker,
                                &order_identities,
                                &pending_submits,
                                &pending_cancels,
                                &pending_tasks,
                                account_id,
                            )
                            .await;
                        }
                        Err(e) if e.is_submit_outcome_unknown() => {
                            for (batch_order, expected_venue_order_id) in
                                orders_chunk.into_iter().zip(expected_venue_order_ids)
                            {
                                if let Some((order_id_str, venue_order_id)) =
                                    handle_unknown_submit_result(
                                        &batch_order.order,
                                        expected_venue_order_id,
                                        &e.to_string(),
                                        None,
                                        &emitter,
                                        clock,
                                        &fill_tracker,
                                        &order_identities,
                                        &pending_submits,
                                        &pending_cancels,
                                        account_id,
                                        batch_order.size_precision,
                                        batch_order.price_precision,
                                    )
                                {
                                    execute_deferred_cancel(
                                        &submitter,
                                        &batch_order.order,
                                        &order_id_str,
                                        venue_order_id,
                                        &emitter,
                                        &pending_cancels,
                                        clock,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            for batch_order in orders_chunk {
                                reject_submit_order(
                                    &batch_order.order,
                                    &format!("{e}"),
                                    &emitter,
                                    clock,
                                    &pending_cancels,
                                );
                            }
                        }
                    }
                }

                offset = end;
            }

            Ok(())
        });
    }

    pub(super) fn modify_order_command(&self, cmd: &ModifyOrder) {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone());

        if let Some(order) = order {
            let venue_order_id = order.venue_order_id();
            let ts_now = self.clock.get_time_ns();
            self.emitter.emit_order_modify_rejected(
                &order,
                venue_order_id,
                "Order modification not supported on Polymarket",
                ts_now,
            );
        }
    }

    pub(super) fn calculate_commission_impl(
        &self,
        instrument: &InstrumentAny,
        last_qty: Quantity,
        last_px: Price,
        liquidity_side: LiquiditySide,
    ) -> Money {
        let fee_rate = instrument_taker_fee(instrument);
        let fee_exponent = instrument_fee_exponent(instrument);
        let commission = compute_commission(
            fee_rate,
            fee_exponent,
            last_qty.as_decimal(),
            last_px.as_decimal(),
            liquidity_side,
        );

        Money::new(commission, instrument.quote_currency())
    }
}
