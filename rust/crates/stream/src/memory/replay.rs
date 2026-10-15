impl State {
    // Links reference entries already in the append-only, originally funded inventory.
    fn replay_entry(&self, index: usize) -> &Retained {
        self.retained
            .get(index)
            .unwrap_or_else(|| unreachable!("retained replay link"))
    }

    fn replay_entry_mut(&mut self, index: usize) -> &mut Retained {
        self.retained
            .get_mut(index)
            .unwrap_or_else(|| unreachable!("retained replay link"))
    }

    fn replay_height(&self, index: Option<usize>) -> u8 {
        index.map_or(0, |index| self.replay_entry(index).replay_height)
    }

    fn update_replay_height(&mut self, index: usize) {
        let entry = self.replay_entry(index);
        let height = 1 + self
            .replay_height(entry.replay_left)
            .max(self.replay_height(entry.replay_right));
        self.replay_entry_mut(index).replay_height = height;
    }

    fn rotate_replay_left(&mut self, root: usize) -> usize {
        let Some(right) = self.replay_entry(root).replay_right else {
            return root;
        };
        self.replay_entry_mut(root).replay_right = self.replay_entry(right).replay_left;
        self.replay_entry_mut(right).replay_left = Some(root);
        self.update_replay_height(root);
        self.update_replay_height(right);
        right
    }

    fn rotate_replay_right(&mut self, root: usize) -> usize {
        let Some(left) = self.replay_entry(root).replay_left else {
            return root;
        };
        self.replay_entry_mut(root).replay_left = self.replay_entry(left).replay_right;
        self.replay_entry_mut(left).replay_right = Some(root);
        self.update_replay_height(root);
        self.update_replay_height(left);
        left
    }

    fn insert_replay(&mut self, root: Option<usize>, index: usize) -> usize {
        let Some(root) = root else { return index };
        if self.replay_entry(index).replay.as_deref() < self.replay_entry(root).replay.as_deref() {
            let left = self.insert_replay(self.replay_entry(root).replay_left, index);
            self.replay_entry_mut(root).replay_left = Some(left);
        } else {
            let right = self.insert_replay(self.replay_entry(root).replay_right, index);
            self.replay_entry_mut(root).replay_right = Some(right);
        }
        self.update_replay_height(root);
        let entry = self.replay_entry(root);
        let left = self.replay_height(entry.replay_left);
        let right = self.replay_height(entry.replay_right);
        if left > right + 1 {
            if let Some(child) = entry.replay_left {
                let entry = self.replay_entry(child);
                if self.replay_height(entry.replay_right) > self.replay_height(entry.replay_left) {
                    let child = self.rotate_replay_left(child);
                    self.replay_entry_mut(root).replay_left = Some(child);
                }
            }
            self.rotate_replay_right(root)
        } else if right > left + 1 {
            if let Some(child) = entry.replay_right {
                let entry = self.replay_entry(child);
                if self.replay_height(entry.replay_left) > self.replay_height(entry.replay_right) {
                    let child = self.rotate_replay_right(child);
                    self.replay_entry_mut(root).replay_right = Some(child);
                }
            }
            self.rotate_replay_left(root)
        } else {
            root
        }
    }
}
