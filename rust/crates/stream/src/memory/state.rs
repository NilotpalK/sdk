#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    replay_root: Option<usize>,
    replay_count: usize,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    #[cfg(not(target_arch = "wasm32"))]
    reserved: Arc<std::sync::Mutex<(usize, usize)>>,
}

impl State {
    fn replay(&self, key: &[u8]) -> Option<&Retained> {
        let mut next = self.replay_root;
        while let Some(index) = next {
            let entry = self.retained.get(index)?;
            match entry.replay.as_deref()?.cmp(key) {
                std::cmp::Ordering::Equal => return Some(entry),
                std::cmp::Ordering::Less => next = entry.replay_right,
                std::cmp::Ordering::Greater => next = entry.replay_left,
            }
        }
        None
    }
    fn replay_count(&self) -> usize {
        self.replay_count
    }
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        self.retained
            .iter()
            .rev()
            .filter_map(Retained::envelope)
            .find(|envelope| envelope.commit_id == *id)
    }
}
