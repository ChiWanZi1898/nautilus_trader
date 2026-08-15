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

//! WebSocket message handler for the Polymarket CLOB API.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use nautilus_network::{
    RECONNECTED,
    websocket::{AuthTracker, SubscriptionState, WebSocketClient},
};
use serde_json::value::RawValue;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender}; // tokio-import-ok
use tokio_tungstenite::tungstenite::Message;

use super::{
    client::WsChannel,
    messages::{
        MarketInitialSubscribeRequest, MarketSubscribeRequest, MarketUnsubscribeRequest,
        MarketWsMessage, PolymarketConnectionMessage, PolymarketWsAuth, PolymarketWsMessage,
        UserSubscribeRequest, UserWsMessage,
    },
};
use crate::{
    common::credential::Credential, evidence::PolymarketEvidenceBridge,
    evidence_v2::PolymarketAuthenticatedUserFrameV2,
};

/// Commands sent from the outer client to the inner message handler.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Set the WebSocketClient for the handler to use.
    SetClient(WebSocketClient),
    /// Disconnect the WebSocket connection.
    Disconnect,
    /// Add asset IDs to the market-channel subscription set and send a subscribe message.
    SubscribeMarket(Vec<String>),
    /// Remove asset IDs from the subscription set (no wire message needed).
    UnsubscribeMarket(Vec<String>),
    /// Send the authenticated subscribe message on the user channel.
    SubscribeUser,
}

pub(super) struct FeedHandler {
    signal: Arc<AtomicBool>,
    channel: WsChannel,
    client: Option<WebSocketClient>,
    cmd_rx: UnboundedReceiver<HandlerCommand>,
    raw_rx: UnboundedReceiver<(u64, Message)>,
    out_tx: UnboundedSender<PolymarketConnectionMessage>,
    credential: Option<Credential>,
    subscriptions: SubscriptionState,
    auth_tracker: AuthTracker,
    // True once SubscribeUser has been explicitly requested by the caller
    user_subscribed: bool,
    // True once the current market-channel session has sent its initial subscribe payload.
    market_subscription_initialized: bool,
    // Overflow buffer for batched frames, drained before reading the next raw message
    message_buffer: Vec<PolymarketConnectionMessage>,
    // Whether to include `custom_feature_enabled: true` in the initial subscribe
    subscribe_new_markets: bool,
    evidence_bridge: Option<Arc<dyn PolymarketEvidenceBridge>>,
    evidence_account_address: Option<String>,
    user_session_epoch_counter: Arc<AtomicU64>,
    user_evidence_sequence: Arc<AtomicU64>,
    user_session_epoch: u64,
    user_frame_sequence: u64,
}

impl FeedHandler {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        signal: Arc<AtomicBool>,
        channel: WsChannel,
        cmd_rx: UnboundedReceiver<HandlerCommand>,
        raw_rx: UnboundedReceiver<(u64, Message)>,
        out_tx: UnboundedSender<PolymarketConnectionMessage>,
        credential: Option<Credential>,
        subscriptions: SubscriptionState,
        auth_tracker: AuthTracker,
        user_subscribed: bool,
        subscribe_new_markets: bool,
        evidence_bridge: Option<Arc<dyn PolymarketEvidenceBridge>>,
        evidence_account_address: Option<String>,
        user_session_epoch_counter: Arc<AtomicU64>,
        user_evidence_sequence: Arc<AtomicU64>,
        user_session_epoch: u64,
    ) -> Self {
        Self {
            signal,
            channel,
            client: None,
            cmd_rx,
            raw_rx,
            out_tx,
            credential,
            subscriptions,
            auth_tracker,
            user_subscribed,
            market_subscription_initialized: false,
            message_buffer: Vec::new(),
            subscribe_new_markets,
            evidence_bridge,
            evidence_account_address,
            user_session_epoch_counter,
            user_evidence_sequence,
            user_session_epoch,
            user_frame_sequence: 1,
        }
    }

    pub(super) fn send(&self, msg: PolymarketConnectionMessage) -> Result<(), String> {
        self.out_tx
            .send(msg)
            .map_err(|e| format!("Failed to send message: {e}"))
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(Ordering::Relaxed)
    }

    async fn fail_user_evidence(&self, reason: &str) {
        log::error!("Authenticated user evidence lane failed: {reason}");
        self.auth_tracker.fail(reason.to_owned());
        if let Some(client) = &self.client {
            client.disconnect().await;
        }
        self.signal.store(true, Ordering::SeqCst);
    }

    async fn send_subscribe_market(&mut self, asset_ids: &[String]) {
        let Some(ref client) = self.client else {
            log::warn!("No client available for market subscribe");
            return;
        };

        for id in asset_ids {
            self.subscriptions.mark_subscribe(id);
        }

        let payload = if self.market_subscription_initialized {
            serde_json::to_string(&MarketSubscribeRequest {
                assets_ids: asset_ids.to_vec(),
                operation: "subscribe",
                custom_feature_enabled: self.subscribe_new_markets,
            })
        } else {
            serde_json::to_string(&MarketInitialSubscribeRequest {
                assets_ids: asset_ids.to_vec(),
                msg_type: "market",
                custom_feature_enabled: self.subscribe_new_markets,
            })
        };

        match payload {
            Ok(payload) => {
                if let Err(e) = client.send_text(payload, None).await {
                    for id in asset_ids {
                        self.subscriptions.mark_failure(id);
                    }
                    log::error!("Failed to send market subscribe: {e}");
                } else {
                    self.market_subscription_initialized = true;
                    // Polymarket has no server ACK, treat successful send as confirmation
                    for id in asset_ids {
                        self.subscriptions.confirm_subscribe(id);
                    }
                }
            }
            Err(e) => {
                for id in asset_ids {
                    self.subscriptions.mark_failure(id);
                }
                log::error!("Failed to serialize market subscribe request: {e}");
            }
        }
    }

    async fn send_unsubscribe_market(&self, asset_ids: &[String]) {
        let Some(ref client) = self.client else {
            log::warn!("No client available for market unsubscribe");
            return;
        };

        let req = MarketUnsubscribeRequest {
            assets_ids: asset_ids.to_vec(),
            operation: "unsubscribe",
        };

        match serde_json::to_string(&req) {
            Ok(payload) => {
                if let Err(e) = client.send_text(payload, None).await {
                    log::error!("Failed to send market unsubscribe: {e}");
                }
            }
            Err(e) => log::error!("Failed to serialize market unsubscribe request: {e}"),
        }
    }

    async fn send_subscribe_user(&self) {
        let Some(ref client) = self.client else {
            log::warn!("No client available for user subscribe");
            return;
        };
        let Some(cred) = &self.credential else {
            log::error!("User channel subscribe requires credential");
            return;
        };

        let req = UserSubscribeRequest {
            auth: PolymarketWsAuth {
                api_key: cred.api_key().to_string(),
                secret: cred.api_secret(),
                passphrase: cred.passphrase().to_string(),
            },
            markets: vec![],
            assets_ids: vec![],
            msg_type: "user",
        };

        // Begin auth tracking; discard receiver, state is queried via is_authenticated()
        drop(self.auth_tracker.begin());

        match serde_json::to_string(&req) {
            Ok(payload) => {
                // auth_tracker.succeed() is NOT called here; sending the request only
                // confirms delivery to the server, not that the credentials were accepted.
                // succeed() is called in next() when the server actually sends user-channel
                // data, which is the real confirmation that authentication worked.
                if let Err(e) = client.send_text(payload, None).await {
                    self.auth_tracker.fail(e.to_string());
                    log::error!("Failed to send user subscribe: {e}");
                }
            }
            Err(e) => {
                self.auth_tracker.fail(format!("Serialize error: {e}"));
                log::error!("Failed to serialize user subscribe request: {e}");
            }
        }
    }

    async fn resubscribe_all(&mut self) {
        match self.channel {
            WsChannel::Market => {
                let ids = self.subscriptions.all_topics();
                if ids.is_empty() {
                    return;
                }
                log::info!(
                    "Resubscribing to {} market assets after reconnect",
                    ids.len()
                );
                self.send_subscribe_market(&ids).await;
            }
            WsChannel::User => {
                if self.user_subscribed {
                    log::info!("Re-authenticating user channel after reconnect");
                    self.send_subscribe_user().await;
                }
            }
        }
    }

    fn parse_messages(&self, text: &str) -> Vec<PolymarketWsMessage> {
        // When `subscribe_new_markets` is enabled, Polymarket's WSS periodically
        // sends the plain-text string "NO NEW ASSETS" as a heartbeat/ack.
        if text == "NO NEW ASSETS" {
            return vec![];
        }

        match self.channel {
            WsChannel::Market => {
                if let Ok(msgs) = serde_json::from_str::<Vec<&RawValue>>(text) {
                    msgs.into_iter()
                        .filter_map(|raw| match MarketWsMessage::parse(raw.get()) {
                            Ok(msg) => Some(PolymarketWsMessage::Market(msg)),
                            Err(e) => {
                                log::warn!("Failed to parse market WS batch element: {e}");
                                None
                            }
                        })
                        .collect()
                } else if let Ok(msg) = MarketWsMessage::parse(text) {
                    vec![PolymarketWsMessage::Market(msg)]
                } else {
                    log::warn!("Failed to parse market WS message: {text}");
                    vec![]
                }
            }
            WsChannel::User => {
                if let Ok(msgs) = UserWsMessage::parse_batch(text) {
                    msgs.into_iter().map(PolymarketWsMessage::User).collect()
                } else if let Ok(msg) = UserWsMessage::parse(text) {
                    vec![PolymarketWsMessage::User(msg)]
                } else {
                    log::warn!("Failed to parse authenticated user WS message");
                    vec![]
                }
            }
        }
    }

    pub(super) async fn next(&mut self) -> Option<PolymarketConnectionMessage> {
        if !self.message_buffer.is_empty() {
            return Some(self.message_buffer.remove(0));
        }

        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            log::debug!("Setting WebSocket client in handler");
                            self.client = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            log::debug!("Handler received disconnect command");

                            if let Some(ref client) = self.client {
                                client.disconnect().await;
                            }
                            self.signal.store(true, Ordering::SeqCst);
                            return None;
                        }
                        HandlerCommand::SubscribeMarket(ids) => {
                            self.send_subscribe_market(&ids).await;
                        }
                        HandlerCommand::UnsubscribeMarket(ids) => {
                            for id in &ids {
                                self.subscriptions.mark_unsubscribe(id);
                            }
                            self.send_unsubscribe_market(&ids).await;
                            for id in &ids {
                                self.subscriptions.confirm_unsubscribe(id);
                            }
                        }
                        HandlerCommand::SubscribeUser => {
                            self.user_subscribed = true;
                            self.send_subscribe_user().await;
                        }
                    }
                }
                Some((transport_epoch, raw)) = self.raw_rx.recv() => {
                    match raw {
                        Message::Text(text) => {
                            if text == RECONNECTED {
                                if self.channel == WsChannel::User {
                                    let next_epoch = self.user_session_epoch_counter.fetch_update(
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                        |current| current.checked_add(1),
                                    );
                                    let Ok(previous_epoch) = next_epoch else {
                                        log::error!("Polymarket user session epoch overflow");
                                        self.signal.store(true, Ordering::SeqCst);
                                        return None;
                                    };
                                    self.user_session_epoch = previous_epoch + 1;
                                    self.user_frame_sequence = 1;
                                }
                                self.market_subscription_initialized = false;
                                self.resubscribe_all().await;
                                return Some(PolymarketConnectionMessage {
                                    transport_epoch,
                                    message: PolymarketWsMessage::Reconnected,
                                });
                            }
                            let msgs = if self.channel == WsChannel::User
                                && let Some(bridge) = &self.evidence_bridge
                            {
                                let Some(account_address) = self.evidence_account_address.as_deref()
                                else {
                                    self.fail_user_evidence("missing account projection identity").await;
                                    return None;
                                };
                                let Some(credential) = self.credential.as_ref() else {
                                    self.fail_user_evidence("missing authenticated credential").await;
                                    return None;
                                };
                                let api_key = credential.api_key().to_string();
                                let fact = match PolymarketAuthenticatedUserFrameV2::project(
                                    &text,
                                    self.user_session_epoch,
                                    self.user_frame_sequence,
                                    account_address,
                                    &api_key,
                                ) {
                                    Ok(fact) => fact,
                                    Err(_) => {
                                        self.fail_user_evidence("strict V2 projection failed").await;
                                        return None;
                                    }
                                };
                                let messages = match fact.to_dispatch_messages() {
                                    Ok(messages) => messages,
                                    Err(_) => {
                                        self.fail_user_evidence("normalized V2 dispatch failed")
                                            .await;
                                        return None;
                                    }
                                };
                                let Some(expected_evidence_sequence) = self
                                    .user_evidence_sequence
                                    .load(Ordering::SeqCst)
                                    .checked_add(1)
                                else {
                                    self.fail_user_evidence("evidence sequence overflow").await;
                                    return None;
                                };
                                match bridge.append_authenticated_user_frame(&fact).await {
                                    Ok(ack)
                                        if ack.fact_id() == fact.fact_id()
                                            && ack.sequence() == expected_evidence_sequence => {}
                                    Ok(_) => {
                                        self.fail_user_evidence("durable acknowledgement mismatch")
                                            .await;
                                        return None;
                                    }
                                    Err(_) => {
                                        self.fail_user_evidence("durable append failed").await;
                                        return None;
                                    }
                                }
                                let Some(next_sequence) = self.user_frame_sequence.checked_add(1)
                                else {
                                    self.fail_user_evidence("frame sequence overflow").await;
                                    return None;
                                };
                                self.user_evidence_sequence
                                    .store(expected_evidence_sequence, Ordering::SeqCst);
                                self.user_frame_sequence = next_sequence;
                                messages
                                    .into_iter()
                                    .map(PolymarketWsMessage::User)
                                    .collect()
                            } else {
                                self.parse_messages(&text)
                            };
                            if msgs.is_empty() {
                                continue;
                            }
                            // Receiving any user-channel data confirms the server accepted the
                            // credentials; mark auth as successful on the first delivery.
                            if self.channel == WsChannel::User {
                                self.auth_tracker.succeed();
                            }
                            // Buffer msgs[1..] so they are returned in order on subsequent
                            // next() calls; returning first directly preserves 0,1,2,...,n order
                            let mut iter = msgs.into_iter().map(|message| {
                                PolymarketConnectionMessage {
                                    transport_epoch,
                                    message,
                                }
                            });
                            let first = iter.next().unwrap();
                            self.message_buffer.extend(iter);
                            return Some(first);
                        }
                        Message::Ping(data) => {
                            if let Some(ref client) = self.client
                                && let Err(e) = client.send_pong(data.to_vec()).await
                            {
                                log::warn!("Failed to send pong: {e}");
                            }
                        }
                        Message::Close(_) => {
                            log::debug!("WebSocket close frame received");
                            return None;
                        }
                        _ => {}
                    }
                }
                else => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::{
        common::enums::PolymarketOrderSide,
        evidence::{
            PolymarketEvidenceAck, PolymarketEvidenceError, PolymarketEvidenceRecovery,
            PolymarketMutationEvidence,
        },
        evidence_v2::PolymarketAuthenticatedUserFrameV2,
    };

    #[derive(Debug, Default)]
    struct TestFrameBridge {
        frames: tokio::sync::Mutex<Vec<Vec<u8>>>,
        fail: AtomicBool,
        mismatch_ack: AtomicBool,
        mismatch_sequence: AtomicBool,
        evidence_sequence: AtomicU64,
    }

    #[async_trait]
    impl PolymarketEvidenceBridge for TestFrameBridge {
        fn recover(&self) -> Result<PolymarketEvidenceRecovery, PolymarketEvidenceError> {
            PolymarketEvidenceRecovery::try_new([0x41; 32], 0, 0, Vec::new(), Vec::new())
        }

        fn acknowledge_recovery(
            &self,
            mutation_high_watermark: u64,
            inbound_high_watermark: u64,
        ) -> Result<(), PolymarketEvidenceError> {
            if mutation_high_watermark == 0 && inbound_high_watermark == 0 {
                Ok(())
            } else {
                Err(PolymarketEvidenceError::InvalidAcknowledgement)
            }
        }

        async fn append_mutation(
            &self,
            _fact: &PolymarketMutationEvidence<'_>,
        ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError> {
            Err(PolymarketEvidenceError::Unavailable)
        }

        async fn append_authenticated_user_frame(
            &self,
            fact: &PolymarketAuthenticatedUserFrameV2,
        ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(PolymarketEvidenceError::Durability);
            }
            self.frames
                .lock()
                .await
                .push(fact.canonical_bytes().to_vec());
            let fact_id = if self.mismatch_ack.load(Ordering::SeqCst) {
                [0x7f; 32]
            } else {
                *fact.fact_id()
            };
            let sequence = self.evidence_sequence.fetch_add(1, Ordering::SeqCst) + 1;
            let sequence = if self.mismatch_sequence.load(Ordering::SeqCst) {
                sequence + 1
            } else {
                sequence
            };
            PolymarketEvidenceAck::try_new(fact_id, sequence)
        }
    }

    #[fixture]
    fn market_handler() -> FeedHandler {
        feed_handler(WsChannel::Market)
    }

    #[fixture]
    fn user_handler() -> FeedHandler {
        feed_handler(WsChannel::User)
    }

    fn feed_handler(channel: WsChannel) -> FeedHandler {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();

        FeedHandler::new(
            Arc::new(AtomicBool::new(false)),
            channel,
            cmd_rx,
            raw_rx,
            out_tx,
            None,
            SubscriptionState::new(':'),
            AuthTracker::new(),
            false,
            false,
            None,
            None,
            Arc::new(AtomicU64::new(u64::from(channel == WsChannel::User))),
            Arc::new(AtomicU64::new(0)),
            u64::from(channel == WsChannel::User),
        )
    }

    fn market_handler_with_raw() -> (FeedHandler, UnboundedSender<(u64, Message)>) {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            FeedHandler::new(
                Arc::new(AtomicBool::new(false)),
                WsChannel::Market,
                cmd_rx,
                raw_rx,
                out_tx,
                None,
                SubscriptionState::new(':'),
                AuthTracker::new(),
                false,
                false,
                None,
                None,
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                0,
            ),
            raw_tx,
        )
    }

    fn user_handler_with_bridge(
        bridge: Arc<TestFrameBridge>,
    ) -> (FeedHandler, UnboundedSender<(u64, Message)>) {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            FeedHandler::new(
                Arc::new(AtomicBool::new(false)),
                WsChannel::User,
                cmd_rx,
                raw_rx,
                out_tx,
                Some(
                    Credential::new(
                        "00000000-0000-0000-0000-000000000001",
                        "Zml4dHVyZQ==",
                        "fixture-passphrase".to_owned(),
                    )
                    .unwrap(),
                ),
                SubscriptionState::new(':'),
                AuthTracker::new(),
                false,
                false,
                Some(bridge),
                Some("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned()),
                Arc::new(AtomicU64::new(1)),
                Arc::new(AtomicU64::new(0)),
                1,
            ),
            raw_tx,
        )
    }

    #[rstest]
    #[tokio::test]
    async fn authenticated_batch_is_durable_once_before_any_element_is_released() {
        let bridge = Arc::new(TestFrameBridge::default());
        let (mut handler, raw_tx) = user_handler_with_bridge(bridge.clone());
        let raw = include_str!("../../test_data/ws_user_batch_msg.json");
        raw_tx
            .send((0, Message::Text(raw.to_string().into())))
            .expect("send raw frame");

        assert!(matches!(
            handler.next().await,
            Some(PolymarketConnectionMessage {
                message: PolymarketWsMessage::User(_),
                ..
            })
        ));
        let retained = bridge.frames.lock().await;
        assert_eq!(retained.len(), 1);
        assert!(
            !retained[0]
                .windows("00000000-0000-0000-0000-000000000001".len())
                .any(|window| window == b"00000000-0000-0000-0000-000000000001")
        );
        drop(retained);
        assert!(matches!(
            handler.next().await,
            Some(PolymarketConnectionMessage {
                message: PolymarketWsMessage::User(_),
                ..
            })
        ));
        assert_eq!(bridge.frames.lock().await.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn durability_failure_releases_no_authenticated_element() {
        let bridge = Arc::new(TestFrameBridge::default());
        bridge.fail.store(true, Ordering::SeqCst);
        let (mut handler, raw_tx) = user_handler_with_bridge(bridge);
        raw_tx
            .send((
                0,
                Message::Text(
                    include_str!("../../test_data/ws_user_batch_msg.json")
                        .to_string()
                        .into(),
                ),
            ))
            .expect("send raw frame");

        assert!(handler.next().await.is_none());
        assert!(handler.is_stopped());
        assert!(handler.message_buffer.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn mismatched_durable_ack_releases_no_authenticated_element() {
        let bridge = Arc::new(TestFrameBridge::default());
        bridge.mismatch_ack.store(true, Ordering::SeqCst);
        let (mut handler, raw_tx) = user_handler_with_bridge(bridge.clone());
        raw_tx
            .send((
                0,
                Message::Text(
                    include_str!("../../test_data/ws_user_batch_msg.json")
                        .to_string()
                        .into(),
                ),
            ))
            .expect("send raw frame");

        assert!(handler.next().await.is_none());
        assert!(handler.is_stopped());
        assert!(handler.message_buffer.is_empty());
        assert_eq!(bridge.frames.lock().await.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn gapped_durable_ack_sequence_releases_no_authenticated_element() {
        let bridge = Arc::new(TestFrameBridge::default());
        bridge.mismatch_sequence.store(true, Ordering::SeqCst);
        let (mut handler, raw_tx) = user_handler_with_bridge(bridge.clone());
        raw_tx
            .send((
                0,
                Message::Text(
                    include_str!("../../test_data/ws_user_batch_msg.json")
                        .to_string()
                        .into(),
                ),
            ))
            .expect("send raw frame");

        assert!(handler.next().await.is_none());
        assert!(handler.is_stopped());
        assert!(handler.message_buffer.is_empty());
        assert_eq!(bridge.frames.lock().await.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn market_batch_members_retain_the_raw_transport_epoch() {
        let (mut handler, raw_tx) = market_handler_with_raw();
        raw_tx
            .send((
                7,
                Message::Text(
                    include_str!("../../test_data/ws_market_mixed_known_unknown.json")
                        .to_string()
                        .into(),
                ),
            ))
            .expect("send raw market frame");

        let first = handler.next().await.expect("first parsed member");
        let second = handler.next().await.expect("second parsed member");
        assert_eq!(first.transport_epoch, 7);
        assert_eq!(second.transport_epoch, 7);
    }

    #[rstest]
    fn test_parse_market_batch_skips_unknown_event(market_handler: FeedHandler) {
        let messages = market_handler.parse_messages(include_str!(
            "../../test_data/ws_market_mixed_known_unknown.json"
        ));

        assert_eq!(messages.len(), 2);

        let PolymarketWsMessage::Market(MarketWsMessage::PriceChange(quotes)) = &messages[0] else {
            panic!("Expected first message to be a price change");
        };
        assert_eq!(
            quotes.market.as_str(),
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(quotes.timestamp, "1700000000001");
        assert_eq!(quotes.price_changes.len(), 1);

        let quote = &quotes.price_changes[0];
        assert_eq!(quote.asset_id.as_str(), "101");
        assert_eq!(quote.price, "0.37");
        assert_eq!(quote.side, PolymarketOrderSide::Buy);
        assert_eq!(quote.size, "12.5");
        assert_eq!(quote.hash, "price-change-hash");
        assert_eq!(quote.best_bid.as_deref(), Some("0.36"));
        assert_eq!(quote.best_ask.as_deref(), Some("0.38"));

        let PolymarketWsMessage::Market(MarketWsMessage::LastTradePrice(trade)) = &messages[1]
        else {
            panic!("Expected second message to be a last trade price");
        };
        assert_eq!(
            trade.market.as_str(),
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        assert_eq!(trade.asset_id.as_str(), "202");
        assert_eq!(trade.fee_rate_bps, "17");
        assert_eq!(trade.price, "0.63");
        assert_eq!(trade.side, PolymarketOrderSide::Sell);
        assert_eq!(trade.size, "4.25");
        assert_eq!(trade.timestamp, "1700000000003");
        assert_eq!(trade.transaction_hash.as_deref(), Some("0xtrade-hash"));
    }

    #[rstest]
    fn test_parse_market_single_message(market_handler: FeedHandler) {
        let messages = market_handler.parse_messages(include_str!(
            "../../test_data/ws_market_last_trade_msg.json"
        ));

        assert_eq!(messages.len(), 1);

        let PolymarketWsMessage::Market(MarketWsMessage::LastTradePrice(trade)) = &messages[0]
        else {
            panic!("Expected a last trade price");
        };
        assert_eq!(
            trade.market.as_str(),
            "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917"
        );
        assert_eq!(
            trade.asset_id.as_str(),
            "71321045679252212594626385532706912750332728571942532289631379312455583992563"
        );
        assert_eq!(trade.fee_rate_bps, "0");
        assert_eq!(trade.price, "0.51");
        assert_eq!(trade.side, PolymarketOrderSide::Buy);
        assert_eq!(trade.size, "25.0");
        assert_eq!(trade.timestamp, "1703875202000");
        assert!(trade.transaction_hash.is_none());
    }

    #[rstest]
    fn test_parse_user_batch(user_handler: FeedHandler) {
        let messages =
            user_handler.parse_messages(include_str!("../../test_data/ws_user_batch_msg.json"));
        let actual: Vec<UserWsMessage> = messages
            .into_iter()
            .map(|message| match message {
                PolymarketWsMessage::User(message) => message,
                other => panic!("Expected user message, received {other:?}"),
            })
            .collect();
        let expected: Vec<UserWsMessage> =
            serde_json::from_str(include_str!("../../test_data/ws_user_batch_msg.json"))
                .expect("user batch fixture should deserialize");

        assert_eq!(actual, expected);
    }
}
