import os
import time
import threading
import shutil

import pytest

from src.storage.lock_manager import LockManager, LockType
from src.storage.deadlock import DeadlockDetector
from src.storage.concurrency import ConcurrencyControl
from src.storage.transaction import TransactionManager, TransactionStatus
from src.storage import shared_memory
from src.config import DATA_DIR


TEST_NODE = 99


@pytest.fixture(autouse=True)
def clean_storage():
    shared_memory.init_storage(TEST_NODE)
    yield
    path = os.path.join(DATA_DIR, f"node_{TEST_NODE}")
    if os.path.exists(path):
        shutil.rmtree(path)


class TestLockManager:
    def test_shared_compatible(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.SHARED) is True
        assert lm.acquire("t2", "res_a", LockType.SHARED) is True
        holders = lm.get_holders("res_a")
        assert holders == {"t1", "t2"}

    def test_exclusive_incompatible(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.SHARED) is True
        assert lm.acquire("t2", "res_a", LockType.EXCLUSIVE) is False
        assert lm.get_waiting("res_a") == ["t2"]

    def test_exclusive_exclusive_incompatible(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.EXCLUSIVE) is True
        assert lm.acquire("t2", "res_a", LockType.EXCLUSIVE) is False

    def test_exclusive_blocks_shared(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.EXCLUSIVE) is True
        assert lm.acquire("t2", "res_a", LockType.SHARED) is False

    def test_release_grants_waiting(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.EXCLUSIVE) is True
        assert lm.acquire("t2", "res_a", LockType.SHARED) is False

        lm.release("t1", "res_a")

        holders = lm.get_holders("res_a")
        assert "t2" in holders

    def test_release_all(self):
        lm = LockManager()
        lm.acquire("t1", "res_a", LockType.EXCLUSIVE)
        lm.acquire("t1", "res_b", LockType.SHARED)

        lm.release_all("t1")

        assert lm.get_holders("res_a") == set()
        assert lm.get_holders("res_b") == set()

    def test_upgrade(self):
        lm = LockManager()
        assert lm.acquire("t1", "res_a", LockType.SHARED) is True
        assert lm.upgrade("t1", "res_a") is True
        holders = lm.get_holders("res_a")
        assert holders == {"t1"}

    def test_upgrade_fails_with_multiple_holders(self):
        lm = LockManager()
        lm.acquire("t1", "res_a", LockType.SHARED)
        lm.acquire("t2", "res_a", LockType.SHARED)
        assert lm.upgrade("t1", "res_a") is False

    def test_transfer_locks(self):
        lm = LockManager()
        lm.acquire("child", "res_a", LockType.EXCLUSIVE)
        lm.transfer_locks("child", "parent")
        holders = lm.get_holders("res_a")
        assert "parent" in holders
        assert "child" not in holders

    def test_wait_edges(self):
        lm = LockManager()
        lm.acquire("t1", "res_a", LockType.EXCLUSIVE)
        lm.acquire("t2", "res_a", LockType.EXCLUSIVE)
        edges = lm.get_wait_edges()
        assert "t2" in edges
        assert "t1" in edges["t2"]


class TestDeadlockDetector:
    def test_no_cycle(self):
        lm = LockManager()
        lm.acquire("t1", "res_a", LockType.EXCLUSIVE)
        lm.acquire("t2", "res_a", LockType.EXCLUSIVE)
        dd = DeadlockDetector(lm)
        cycle = dd.detect_cycle()
        assert cycle is None

    def test_simple_cycle(self):
        lm = LockManager()
        lm.acquire("t1", "res_a", LockType.EXCLUSIVE)
        lm.acquire("t2", "res_b", LockType.EXCLUSIVE)
        lm.acquire("t1", "res_b", LockType.EXCLUSIVE)
        lm.acquire("t2", "res_a", LockType.EXCLUSIVE)

        dd = DeadlockDetector(lm)
        cycle = dd.detect_cycle()
        assert cycle is not None
        assert "t1" in cycle
        assert "t2" in cycle

    def test_victim_selection_youngest(self):
        lm = LockManager()
        dd = DeadlockDetector(lm)
        cycle = ["node_0_txn_1", "node_0_txn_5", "node_0_txn_3", "node_0_txn_1"]
        victim = dd.select_victim(cycle)
        assert victim == "node_0_txn_5"


class TestConcurrencyControl:
    def test_acquire_read(self):
        lm = LockManager()
        cc = ConcurrencyControl(lm)
        assert cc.acquire_read("t1", "res_a") is True

    def test_acquire_write(self):
        lm = LockManager()
        cc = ConcurrencyControl(lm)
        assert cc.acquire_write("t1", "res_a") is True

    def test_write_blocks_read(self):
        lm = LockManager()
        cc = ConcurrencyControl(lm)
        cc.acquire_write("t1", "res_a")
        assert cc.acquire_read("t2", "res_a", timeout=0.5) is False

    def test_release_all_unblocks(self):
        lm = LockManager()
        cc = ConcurrencyControl(lm)
        cc.acquire_write("t1", "res_a")

        result = [None]

        def try_read():
            result[0] = cc.acquire_read("t2", "res_a", timeout=3.0)

        t = threading.Thread(target=try_read)
        t.start()
        time.sleep(0.3)
        cc.release_all("t1")
        t.join(timeout=5)

        assert result[0] is True


class TestTransactionManager:
    def _make_manager(self):
        return TransactionManager(TEST_NODE)

    def test_begin(self):
        tm = self._make_manager()
        txn_id = tm.begin()
        assert txn_id.startswith(f"node_{TEST_NODE}_txn_")
        txn = tm.get_transaction(txn_id)
        assert txn.status == TransactionStatus.ACTIVE

    def test_commit_persists(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        txn_id = tm.begin()
        tm.write(txn_id, "accounts.txt", "saldo", "900")
        assert tm.commit(txn_id) is True

        record = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert record["value"] == "900"

    def test_abort_discards(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        txn_id = tm.begin()
        tm.write(txn_id, "accounts.txt", "saldo", "500")
        tm.abort(txn_id)

        record = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert record["value"] == "1000"

    def test_read_your_writes(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        txn_id = tm.begin()
        tm.write(txn_id, "accounts.txt", "saldo", "750")
        value = tm.read(txn_id, "accounts.txt", "saldo")
        assert value == "750"

    def test_s2pl_prevents_lost_update(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        t1 = tm.begin()
        t2 = tm.begin()

        tm.write(t1, "accounts.txt", "saldo", "900")

        result = [None]

        def t2_write():
            result[0] = tm.write(t2, "accounts.txt", "saldo", "800", timeout=0.5)

        thread = threading.Thread(target=t2_write)
        thread.start()
        time.sleep(0.2)

        tm.commit(t1)
        thread.join(timeout=3)

        record = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert record["value"] == "900"

    def test_s2pl_prevents_dirty_read(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        t1 = tm.begin()
        tm.write(t1, "accounts.txt", "saldo", "500")

        read_value = [None]

        def t2_read():
            t2 = tm.begin()
            read_value[0] = tm.read(t2, "accounts.txt", "saldo", timeout=0.5)

        thread = threading.Thread(target=t2_read)
        thread.start()
        thread.join(timeout=3)

        assert read_value[0] is None

        tm.abort(t1)

        t3 = tm.begin()
        val = tm.read(t3, "accounts.txt", "saldo")
        assert val == "1000"

    def test_nested_transaction_lock_inheritance(self):
        tm = self._make_manager()
        shared_memory.write_record(TEST_NODE, "accounts.txt", "key_a", "100", 0)

        parent = tm.begin()
        tm.read(parent, "accounts.txt", "key_a")

        child = tm.begin(parent_txn_id=parent)
        tm.write(child, "accounts.txt", "key_b", "200")
        tm.commit(child)

        holders_b = tm.lock_manager.get_holders("accounts.txt")
        assert parent in holders_b

        tm.commit(parent)

    def test_nested_transaction_abort_releases(self):
        tm = self._make_manager()

        parent = tm.begin()
        child = tm.begin(parent_txn_id=parent)
        tm.write(child, "accounts.txt", "key_c", "300")
        tm.abort(child)

        holders = tm.lock_manager.get_holders("accounts.txt")
        assert child not in holders

        tm.commit(parent)
