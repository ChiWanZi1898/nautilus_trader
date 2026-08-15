// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! External-crate proof for the application-owned evidence factory seam.

use std::sync::Arc;

use async_trait::async_trait;
use nautilus_common::factories::ExecutionClientFactory;
use nautilus_polymarket::{
    evidence::{
        PolymarketAuthenticatedUserFrame, PolymarketEvidenceAck, PolymarketEvidenceBridge,
        PolymarketEvidenceError, PolymarketEvidenceRecovery, PolymarketMutationEvidence,
    },
    factories::PolymarketEvidenceExecutionClientFactory,
};

#[derive(Debug)]
struct ExternalEvidenceBridge;

#[async_trait]
impl PolymarketEvidenceBridge for ExternalEvidenceBridge {
    fn recover(&self) -> Result<PolymarketEvidenceRecovery, PolymarketEvidenceError> {
        PolymarketEvidenceRecovery::try_new([0x11; 32], 0, 0, Vec::new(), Vec::new())
    }

    fn acknowledge_recovery(
        &self,
        _mutation_high_watermark: u64,
        _inbound_high_watermark: u64,
    ) -> Result<(), PolymarketEvidenceError> {
        Ok(())
    }

    async fn append_mutation(
        &self,
        _fact: &PolymarketMutationEvidence<'_>,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError> {
        Err(PolymarketEvidenceError::Unavailable)
    }

    async fn append_authenticated_user_frame(
        &self,
        _fact: &PolymarketAuthenticatedUserFrame,
    ) -> Result<PolymarketEvidenceAck, PolymarketEvidenceError> {
        Err(PolymarketEvidenceError::Unavailable)
    }
}

#[test]
fn external_application_can_construct_bridge_aware_execution_factory() {
    let bridge: Arc<dyn PolymarketEvidenceBridge> = Arc::new(ExternalEvidenceBridge);
    let factory = PolymarketEvidenceExecutionClientFactory::new(bridge);
    let factory: Box<dyn ExecutionClientFactory> = Box::new(factory);

    assert_eq!(factory.name(), "POLYMARKET");
    assert_eq!(factory.config_type(), "PolymarketExecClientConfig");
}
