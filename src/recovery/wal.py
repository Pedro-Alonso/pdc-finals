import os
import time
from dataclasses import dataclass

from src.config import DATA_DIR


@dataclass
class LogEntry:
    lsn: int
    txn_id: str
    entry_type: str
    resource: str
    before_value: str
    after_value: str
    timestamp: float


class WriteAheadLog:
    def __init__(self, node_id):
        self.node_id = node_id
        self._dir = os.path.join(DATA_DIR, f"node_{node_id}")
        self._path = os.path.join(self._dir, "wal.log")
        os.makedirs(self._dir, exist_ok=True)
        self.next_lsn = self._init_lsn()

    def _init_lsn(self):
        if not os.path.exists(self._path):
            return 1
        last_lsn = 0
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|", 6)
                if len(parts) == 7:
                    last_lsn = max(last_lsn, int(parts[0]))
        return last_lsn + 1

    def _serialize(self, entry):
        return (
            f"{entry.lsn}|{entry.txn_id}|{entry.entry_type}|"
            f"{entry.resource}|{entry.before_value}|{entry.after_value}|"
            f"{entry.timestamp}"
        )

    def _deserialize(self, line):
        parts = line.strip().split("|", 6)
        if len(parts) != 7:
            return None
        return LogEntry(
            lsn=int(parts[0]),
            txn_id=parts[1],
            entry_type=parts[2],
            resource=parts[3],
            before_value=parts[4],
            after_value=parts[5],
            timestamp=float(parts[6]),
        )

    def log(self, entry):
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(self._serialize(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _log_entry(self, txn_id, entry_type, resource="", before="", after=""):
        entry = LogEntry(
            lsn=self.next_lsn,
            txn_id=txn_id,
            entry_type=entry_type,
            resource=resource,
            before_value=before,
            after_value=after,
            timestamp=time.time(),
        )
        self.next_lsn += 1
        self.log(entry)
        return entry

    def log_begin(self, txn_id):
        return self._log_entry(txn_id, "BEGIN")

    def log_write(self, txn_id, resource, before, after):
        return self._log_entry(txn_id, "WRITE", resource=resource, before=before, after=after)

    def log_commit(self, txn_id):
        return self._log_entry(txn_id, "COMMIT")

    def log_abort(self, txn_id):
        return self._log_entry(txn_id, "ABORT")

    def log_checkpoint(self, active_txns):
        return self._log_entry("SYSTEM", "CHECKPOINT", after=",".join(active_txns))

    def log_prepare(self, txn_id):
        return self._log_entry(txn_id, "PREPARE")

    def log_global_commit(self, txn_id):
        return self._log_entry(txn_id, "GLOBAL_COMMIT")

    def log_global_abort(self, txn_id):
        return self._log_entry(txn_id, "GLOBAL_ABORT")

    def log_vote_commit(self, txn_id):
        return self._log_entry(txn_id, "VOTE_COMMIT")

    def log_vote_abort(self, txn_id):
        return self._log_entry(txn_id, "VOTE_ABORT")

    def read_all(self):
        if not os.path.exists(self._path):
            return []
        entries = []
        with open(self._path, "r", encoding="utf-8") as f:
            for line in f:
                entry = self._deserialize(line)
                if entry:
                    entries.append(entry)
        return entries

    def read_from(self, lsn):
        return [e for e in self.read_all() if e.lsn >= lsn]

    def clear(self):
        if os.path.exists(self._path):
            with open(self._path, "w", encoding="utf-8") as f:
                f.truncate(0)
        self.next_lsn = 1
