//! Timer-heap entry with stable ordering for equal deadlines.

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Instant;

pub(super) struct TimerEntry {
    pub(super) when: Instant,
    pub(super) seq: u64,
    pub(super) callback: Arc<super::super::callbacks::ReadyCallback>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when && self.seq == other.seq
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, so reverse both keys to pop the earliest
        // deadline first and preserve insertion order for ties.
        other
            .when
            .cmp(&self.when)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    use super::TimerEntry;
    use crate::engine::callbacks::{CallbackKind, ReadyCallback};

    fn timer_entry(py: Python<'_>, when: Instant, seq: u64) -> TimerEntry {
        TimerEntry {
            when,
            seq,
            callback: Arc::new(ReadyCallback::new(
                py,
                seq,
                CallbackKind::Timer,
                py.None(),
                PyTuple::empty(py).unbind(),
                py.None(),
                false,
            )),
        }
    }

    #[test]
    fn heap_pops_earliest_deadline_and_preserves_tie_order() {
        crate::initialize_python_for_tests();
        Python::attach(|py| {
            let now = Instant::now();
            let mut heap = BinaryHeap::new();
            heap.push(timer_entry(py, now + Duration::from_millis(20), 0));
            heap.push(timer_entry(py, now, 2));
            heap.push(timer_entry(py, now, 1));

            let popped = heap.pop().expect("first timer");
            assert_eq!(popped.when, now);
            assert_eq!(popped.seq, 1);

            let popped = heap.pop().expect("second timer");
            assert_eq!(popped.when, now);
            assert_eq!(popped.seq, 2);

            let popped = heap.pop().expect("third timer");
            assert_eq!(popped.when, now + Duration::from_millis(20));
            assert_eq!(popped.seq, 0);
            assert!(heap.is_empty());
        });
    }
}
