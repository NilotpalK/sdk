#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    replays: Vec<usize>,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    replay_allocation: Allocation,
    #[cfg(not(target_arch = "wasm32"))]
    reserved: Arc<std::sync::Mutex<(usize, usize)>>,
}

impl State {
    fn replay(&self, key: &[u8]) -> Option<&Retained> {
        let position = self
            .replays
            .binary_search_by(|index| {
                self.retained
                    .get(*index)
                    .and_then(|entry| entry.replay.as_deref())
                    .cmp(&Some(key))
            })
            .ok()?;
        self.replays
            .get(position)
            .and_then(|index| self.retained.get(*index))
    }
    fn replay_count(&self) -> usize {
        self.replays.len()
    }
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        self.retained
            .iter()
            .rev()
            .filter_map(Retained::envelope)
            .find(|envelope| envelope.commit_id == *id)
    }
}
