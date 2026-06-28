import time
import threading
import logging
from enum import Enum

from src.storage.lock_manager import LockManager, LockType
from src.storage.concurrency import ConcurrencyControl
from src.storage.deadlock import DeadlockDetector
from src.storage import shared_memory


class TransactionStatus(Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"


class Transaction:
    def __init__(self, txn_id, parent_txn_id=None):
        self.txn_id = txn_id
        self.status = TransactionStatus.ACTIVE
        self.read_set = {}
        self.write_buffer = {}
        self.locks_held = set()
        self.parent_txn_id = parent_txn_id
        self.start_time = time.time()


class TransactionManager:
    def __init__(self, node_id, lock_manager=None, wal=None):
        self.node_id = node_id
        self.lock_manager = lock_manager or LockManager()
        self.concurrency = ConcurrencyControl(self.lock_manager)
        self.deadlock_detector = DeadlockDetector(
            self.lock_manager,
            on_victim=self._abort_victim,
        )
        self.wal = wal
        self._lock = threading.Lock()
        self._transactions = {}
        self._seq = 0
        self.logger = logging.getLogger(f"txn_manager.{node_id}")

    def begin(self, parent_txn_id=None):
        with self._lock:
            self._seq += 1
            txn_id = f"node_{self.node_id}_txn_{self._seq}"
            txn = Transaction(txn_id, parent_txn_id=parent_txn_id)
            self._transactions[txn_id] = txn
        if self.wal:
            self.wal.log_begin(txn_id)
        self.logger.info(f"BEGIN {txn_id}" + (f" (child of {parent_txn_id})" if parent_txn_id else ""))
        return txn_id

    def read(self, txn_id, resource, key, timeout=10.0):
        txn = self._get_active(txn_id)
        if txn is None:
            return None

        buf_key = f"{resource}:{key}"
        if buf_key in txn.write_buffer:
            return txn.write_buffer[buf_key]

        if not self.concurrency.acquire_read(txn_id, resource, timeout=timeout):
            self.logger.warning(f"{txn_id} failed to acquire S-lock on {resource}")
            return None

        txn.locks_held.add((resource, LockType.SHARED))
        record = shared_memory.read_record(self.node_id, resource, key)
        value = record["value"] if record else None
        txn.read_set[buf_key] = value
        return value

    def write(self, txn_id, resource, key, value, timeout=10.0):
        txn = self._get_active(txn_id)
        if txn is None:
            return False

        has_shared = any(r == resource and lt == LockType.SHARED for r, lt in txn.locks_held)
        if has_shared:
            upgraded = self.lock_manager.upgrade(txn_id, resource)
            if not upgraded:
                if not self.concurrency.acquire_write(txn_id, resource, timeout=timeout):
                    self.logger.warning(f"{txn_id} failed to upgrade to X-lock on {resource}")
                    return False
        elif not any(r == resource and lt == LockType.EXCLUSIVE for r, lt in txn.locks_held):
            if not self.concurrency.acquire_write(txn_id, resource, timeout=timeout):
                self.logger.warning(f"{txn_id} failed to acquire X-lock on {resource}")
                return False

        txn.locks_held.discard((resource, LockType.SHARED))
        txn.locks_held.add((resource, LockType.EXCLUSIVE))

        buf_key = f"{resource}:{key}"
        if self.wal:
            old_record = shared_memory.read_record(self.node_id, resource, key)
            before = old_record["value"] if old_record else ""
            self.wal.log_write(txn_id, buf_key, before, value)
        txn.write_buffer[buf_key] = value
        self.logger.debug(f"{txn_id} buffered write {key}={value} on {resource}")
        return True

    def commit(self, txn_id):
        txn = self._get_active(txn_id)
        if txn is None:
            return False

        if self.wal:
            self.wal.log_commit(txn_id)

        ts = int(time.time() * 1000)
        for buf_key, value in txn.write_buffer.items():
            resource, key = buf_key.split(":", 1)
            shared_memory.write_record(self.node_id, resource, key, value, timestamp=ts)

        txn.status = TransactionStatus.COMMITTED
        self.logger.info(f"COMMIT {txn_id} ({len(txn.write_buffer)} writes)")

        if txn.parent_txn_id and txn.parent_txn_id in self._transactions:
            self.lock_manager.transfer_locks(txn_id, txn.parent_txn_id)
            parent = self._transactions[txn.parent_txn_id]
            parent.locks_held.update(txn.locks_held)
        else:
            self.concurrency.release_all(txn_id)

        return True

    def abort(self, txn_id):
        with self._lock:
            txn = self._transactions.get(txn_id)
        if txn is None or txn.status != TransactionStatus.ACTIVE:
            return

        if self.wal:
            self.wal.log_abort(txn_id)
        txn.write_buffer.clear()
        txn.status = TransactionStatus.ABORTED
        self.logger.info(f"ABORT {txn_id}")
        self.concurrency.release_all(txn_id)

    def get_transaction(self, txn_id):
        with self._lock:
            return self._transactions.get(txn_id)

    def _get_active(self, txn_id):
        with self._lock:
            txn = self._transactions.get(txn_id)
        if txn is None:
            self.logger.error(f"Transaction {txn_id} not found")
            return None
        if txn.status != TransactionStatus.ACTIVE:
            self.logger.error(f"Transaction {txn_id} is {txn.status.value}")
            return None
        return txn

    def _abort_victim(self, txn_id):
        self.logger.warning(f"Deadlock victim: aborting {txn_id}")
        self.abort(txn_id)

    def start(self):
        self.deadlock_detector.start()

    def stop(self):
        self.deadlock_detector.stop()
