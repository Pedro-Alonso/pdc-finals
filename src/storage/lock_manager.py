import threading
from enum import Enum


class LockType(Enum):
    SHARED = "S"
    EXCLUSIVE = "X"


class LockEntry:
    def __init__(self, resource):
        self.resource = resource
        self.lock_type = None
        self.holders = set()
        self.wait_queue = []


class LockManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._locks = {}
        self._txn_locks = {}

    def _get_entry(self, resource):
        if resource not in self._locks:
            self._locks[resource] = LockEntry(resource)
        return self._locks[resource]

    def acquire(self, txn_id, resource, lock_type):
        with self._lock:
            entry = self._get_entry(resource)

            if txn_id in entry.holders:
                if entry.lock_type == LockType.EXCLUSIVE:
                    return True
                if lock_type == LockType.SHARED:
                    return True
                if lock_type == LockType.EXCLUSIVE and len(entry.holders) == 1:
                    entry.lock_type = LockType.EXCLUSIVE
                    self._record_lock(txn_id, resource, LockType.EXCLUSIVE)
                    return True
                if lock_type == LockType.EXCLUSIVE and len(entry.holders) > 1:
                    if (txn_id, lock_type) not in entry.wait_queue:
                        entry.wait_queue.append((txn_id, lock_type))
                    return False

            if not entry.holders:
                entry.holders.add(txn_id)
                entry.lock_type = lock_type
                self._record_lock(txn_id, resource, lock_type)
                return True

            if entry.lock_type == LockType.SHARED and lock_type == LockType.SHARED:
                entry.holders.add(txn_id)
                self._record_lock(txn_id, resource, lock_type)
                return True

            if (txn_id, lock_type) not in entry.wait_queue:
                entry.wait_queue.append((txn_id, lock_type))
            return False

    def release(self, txn_id, resource):
        with self._lock:
            self._release_internal(txn_id, resource)

    def _release_internal(self, txn_id, resource):
        entry = self._locks.get(resource)
        if not entry:
            return
        entry.holders.discard(txn_id)
        if txn_id in self._txn_locks:
            self._txn_locks[txn_id].discard(resource)

        if not entry.holders:
            entry.lock_type = None
            self._grant_waiting(entry)

    def _grant_waiting(self, entry):
        if not entry.wait_queue:
            return []
        granted = []
        first_txn, first_type = entry.wait_queue[0]
        entry.wait_queue.pop(0)
        entry.holders.add(first_txn)
        entry.lock_type = first_type
        self._record_lock(first_txn, entry.resource, first_type)
        granted.append(first_txn)

        if first_type == LockType.SHARED:
            still_waiting = []
            for txn_id, lt in entry.wait_queue:
                if lt == LockType.SHARED:
                    entry.holders.add(txn_id)
                    self._record_lock(txn_id, entry.resource, lt)
                    granted.append(txn_id)
                else:
                    still_waiting.append((txn_id, lt))
                    break
            entry.wait_queue = still_waiting + entry.wait_queue[len(granted) - 1:]
            remaining = []
            seen = set()
            for item in entry.wait_queue:
                if item[0] not in seen and item[0] not in entry.holders:
                    remaining.append(item)
                    seen.add(item[0])
            entry.wait_queue = remaining

        return granted

    def release_all(self, txn_id):
        with self._lock:
            resources = list(self._txn_locks.get(txn_id, set()))
            granted = {}
            for resource in resources:
                entry = self._locks.get(resource)
                if not entry:
                    continue
                entry.holders.discard(txn_id)
                if not entry.holders:
                    entry.lock_type = None
                    newly_granted = self._grant_waiting(entry)
                    if newly_granted:
                        granted[resource] = newly_granted
            self._txn_locks.pop(txn_id, None)
            return granted

    def transfer_locks(self, from_txn, to_txn):
        with self._lock:
            resources = list(self._txn_locks.get(from_txn, set()))
            for resource in resources:
                entry = self._locks.get(resource)
                if not entry:
                    continue
                if from_txn in entry.holders:
                    entry.holders.discard(from_txn)
                    entry.holders.add(to_txn)
                    self._record_lock(to_txn, resource, entry.lock_type)
            self._txn_locks.pop(from_txn, None)

    def upgrade(self, txn_id, resource):
        with self._lock:
            entry = self._locks.get(resource)
            if not entry:
                return False
            if txn_id not in entry.holders:
                return False
            if entry.lock_type == LockType.EXCLUSIVE:
                return True
            if len(entry.holders) == 1:
                entry.lock_type = LockType.EXCLUSIVE
                self._record_lock(txn_id, resource, LockType.EXCLUSIVE)
                return True
            return False

    def get_holders(self, resource):
        with self._lock:
            entry = self._locks.get(resource)
            if not entry:
                return set()
            return set(entry.holders)

    def get_waiting(self, resource):
        with self._lock:
            entry = self._locks.get(resource)
            if not entry:
                return []
            return [txn_id for txn_id, _ in entry.wait_queue]

    def get_wait_edges(self):
        with self._lock:
            edges = {}
            for resource, entry in self._locks.items():
                if not entry.holders or not entry.wait_queue:
                    continue
                for waiter, _ in entry.wait_queue:
                    if waiter not in edges:
                        edges[waiter] = set()
                    edges[waiter].update(entry.holders)
            return edges

    def _record_lock(self, txn_id, resource, lock_type):
        if txn_id not in self._txn_locks:
            self._txn_locks[txn_id] = set()
        self._txn_locks[txn_id].add(resource)
