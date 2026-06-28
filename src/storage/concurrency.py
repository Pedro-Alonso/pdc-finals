import threading
import logging

from src.storage.lock_manager import LockManager, LockType


class ConcurrencyControl:
    """Strict Two-Phase Locking (S2PL).

    Growing phase: locks acquired as needed, never released.
    Shrinking phase: all locks released only at commit/abort.
    """

    def __init__(self, lock_manager):
        self.lock_manager = lock_manager
        self._lock = threading.Lock()
        self._wait_events = {}
        self.logger = logging.getLogger("concurrency")

    def acquire_read(self, txn_id, resource, timeout=10.0):
        return self._acquire(txn_id, resource, LockType.SHARED, timeout)

    def acquire_write(self, txn_id, resource, timeout=10.0):
        return self._acquire(txn_id, resource, LockType.EXCLUSIVE, timeout)

    def _acquire(self, txn_id, resource, lock_type, timeout):
        granted = self.lock_manager.acquire(txn_id, resource, lock_type)
        if granted:
            self.logger.debug(
                f"{txn_id} acquired {lock_type.value}-lock on {resource}"
            )
            return True

        event_key = (txn_id, resource)
        event = threading.Event()
        with self._lock:
            self._wait_events[event_key] = event

        self.logger.debug(
            f"{txn_id} waiting for {lock_type.value}-lock on {resource}"
        )
        success = event.wait(timeout=timeout)

        with self._lock:
            self._wait_events.pop(event_key, None)

        if success:
            granted = self.lock_manager.acquire(txn_id, resource, lock_type)
            if granted:
                self.logger.debug(
                    f"{txn_id} acquired {lock_type.value}-lock on {resource} (after wait)"
                )
                return True

        self.logger.debug(f"{txn_id} timed out waiting for lock on {resource}")
        return False

    def release_all(self, txn_id):
        granted_map = self.lock_manager.release_all(txn_id)
        for resource, granted_txns in granted_map.items():
            for gtxn in granted_txns:
                event_key = (gtxn, resource)
                with self._lock:
                    event = self._wait_events.get(event_key)
                if event:
                    event.set()

    def notify_released(self, resource):
        with self._lock:
            for (txn_id, res), event in list(self._wait_events.items()):
                if res == resource:
                    event.set()
