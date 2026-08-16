//! Lock-free latency counters for the Polymarket LIMIT submit hot path.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PolymarketSubmitLatencyStage {
    pub last_ns: u64,
    pub max_ns: u64,
    pub sum_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PolymarketSubmitLatencySnapshot {
    pub samples: u64,
    pub task_queue: PolymarketSubmitLatencyStage,
    pub prepare_and_sign: PolymarketSubmitLatencyStage,
    pub encode_request: PolymarketSubmitLatencyStage,
    pub pre_http: PolymarketSubmitLatencyStage,
    pub http_round_trip: PolymarketSubmitLatencyStage,
}

#[derive(Default)]
struct StageCounters {
    last_ns: AtomicU64,
    max_ns: AtomicU64,
    sum_ns: AtomicU64,
}

impl StageCounters {
    fn record(&self, value: u64) {
        self.last_ns.store(value, Ordering::Relaxed);
        self.sum_ns.fetch_add(value, Ordering::Relaxed);
        self.max_ns.fetch_max(value, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PolymarketSubmitLatencyStage {
        PolymarketSubmitLatencyStage {
            last_ns: self.last_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.last_ns.store(0, Ordering::Relaxed);
        self.max_ns.store(0, Ordering::Relaxed);
        self.sum_ns.store(0, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct SubmitLatencyCounters {
    samples: AtomicU64,
    task_queue: StageCounters,
    prepare_and_sign: StageCounters,
    encode_request: StageCounters,
    pre_http: StageCounters,
    http_round_trip: StageCounters,
}

static COUNTERS: SubmitLatencyCounters = SubmitLatencyCounters {
    samples: AtomicU64::new(0),
    task_queue: StageCounters {
        last_ns: AtomicU64::new(0),
        max_ns: AtomicU64::new(0),
        sum_ns: AtomicU64::new(0),
    },
    prepare_and_sign: StageCounters {
        last_ns: AtomicU64::new(0),
        max_ns: AtomicU64::new(0),
        sum_ns: AtomicU64::new(0),
    },
    encode_request: StageCounters {
        last_ns: AtomicU64::new(0),
        max_ns: AtomicU64::new(0),
        sum_ns: AtomicU64::new(0),
    },
    pre_http: StageCounters {
        last_ns: AtomicU64::new(0),
        max_ns: AtomicU64::new(0),
        sum_ns: AtomicU64::new(0),
    },
    http_round_trip: StageCounters {
        last_ns: AtomicU64::new(0),
        max_ns: AtomicU64::new(0),
        sum_ns: AtomicU64::new(0),
    },
};

#[must_use]
pub fn polymarket_submit_latency_snapshot() -> PolymarketSubmitLatencySnapshot {
    PolymarketSubmitLatencySnapshot {
        samples: COUNTERS.samples.load(Ordering::Relaxed),
        task_queue: COUNTERS.task_queue.snapshot(),
        prepare_and_sign: COUNTERS.prepare_and_sign.snapshot(),
        encode_request: COUNTERS.encode_request.snapshot(),
        pre_http: COUNTERS.pre_http.snapshot(),
        http_round_trip: COUNTERS.http_round_trip.snapshot(),
    }
}

pub fn reset_polymarket_submit_latency() {
    COUNTERS.samples.store(0, Ordering::Relaxed);
    COUNTERS.task_queue.reset();
    COUNTERS.prepare_and_sign.reset();
    COUNTERS.encode_request.reset();
    COUNTERS.pre_http.reset();
    COUNTERS.http_round_trip.reset();
}

pub(crate) fn record_limit_submit_latency(
    task_queue_ns: u64,
    prepare_and_sign_ns: u64,
    encode_request_ns: u64,
    pre_http_ns: u64,
    http_round_trip_ns: u64,
) {
    COUNTERS.task_queue.record(task_queue_ns);
    COUNTERS.prepare_and_sign.record(prepare_and_sign_ns);
    COUNTERS.encode_request.record(encode_request_ns);
    COUNTERS.pre_http.record(pre_http_ns);
    COUNTERS.http_round_trip.record(http_round_trip_ns);
    COUNTERS.samples.fetch_add(1, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_resets_fixed_atomic_metrics() {
        reset_polymarket_submit_latency();
        record_limit_submit_latency(1, 2, 3, 4, 5);
        record_limit_submit_latency(2, 3, 4, 5, 6);

        let snapshot = polymarket_submit_latency_snapshot();
        assert_eq!(snapshot.samples, 2);
        assert_eq!(snapshot.task_queue.last_ns, 2);
        assert_eq!(snapshot.task_queue.max_ns, 2);
        assert_eq!(snapshot.task_queue.sum_ns, 3);
        assert_eq!(snapshot.http_round_trip.sum_ns, 11);

        reset_polymarket_submit_latency();
        assert_eq!(polymarket_submit_latency_snapshot(), Default::default());
    }
}
