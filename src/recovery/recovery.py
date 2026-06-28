import logging


class RecoveryManager:
    def __init__(self):
        self.logger = logging.getLogger("recovery")

    def recover(self, wal, shared_memory_mod, node_id):
        entries = wal.read_all()
        if not entries:
            self.logger.info("No WAL entries — nothing to recover")
            return {"committed": set(), "active": set(), "aborted": set()}

        checkpoint_lsn = 0
        checkpoint_active = set()
        for entry in entries:
            if entry.entry_type == "CHECKPOINT":
                checkpoint_lsn = entry.lsn
                active_str = entry.after_value
                checkpoint_active = set(active_str.split(",")) if active_str else set()

        relevant = [e for e in entries if e.lsn >= checkpoint_lsn]

        committed, aborted, began = set(), set(), set()
        for entry in relevant:
            if entry.entry_type == "BEGIN":
                began.add(entry.txn_id)
            elif entry.entry_type in ("COMMIT", "GLOBAL_COMMIT"):
                committed.add(entry.txn_id)
            elif entry.entry_type in ("ABORT", "GLOBAL_ABORT"):
                aborted.add(entry.txn_id)

        active = (began | checkpoint_active) - committed - aborted

        self.logger.info(
            f"Analysis: {len(committed)} committed, {len(active)} active (to undo), "
            f"{len(aborted)} already aborted"
        )

        redo_count = 0
        for entry in relevant:
            if entry.entry_type == "WRITE" and entry.txn_id in committed:
                resource_key = entry.resource
                if ":" in resource_key:
                    resource, key = resource_key.split(":", 1)
                else:
                    resource, key = resource_key, resource_key
                shared_memory_mod.write_record(node_id, resource, key, entry.after_value, timestamp=0)
                redo_count += 1
        self.logger.info(f"Redo: re-applied {redo_count} writes from committed transactions")

        undo_count = 0
        for entry in reversed(relevant):
            if entry.entry_type == "WRITE" and entry.txn_id in active:
                resource_key = entry.resource
                if ":" in resource_key:
                    resource, key = resource_key.split(":", 1)
                else:
                    resource, key = resource_key, resource_key
                if entry.before_value:
                    shared_memory_mod.write_record(node_id, resource, key, entry.before_value, timestamp=0)
                else:
                    shared_memory_mod.delete_record(node_id, resource, key)
                undo_count += 1
        self.logger.info(f"Undo: reverted {undo_count} writes from active transactions")

        for txn_id in active:
            wal.log_abort(txn_id)
        if active:
            self.logger.info(f"Logged ABORT for {len(active)} incomplete transactions")

        return {"committed": committed, "active": active, "aborted": aborted}

    def checkpoint(self, wal, txn_manager):
        active_txns = []
        with txn_manager._lock:
            for txn_id, txn in txn_manager._transactions.items():
                if txn.status.value == "active":
                    active_txns.append(txn_id)
        wal.log_checkpoint(active_txns)
        self.logger.info(f"Checkpoint: {len(active_txns)} active transactions")
