import os
import shutil

import pytest

from src.recovery.wal import WriteAheadLog, LogEntry
from src.recovery.recovery import RecoveryManager
from src.storage.transaction import TransactionManager
from src.storage import shared_memory
from src.config import DATA_DIR


TEST_NODE = 98


@pytest.fixture(autouse=True)
def clean_storage():
    shared_memory.init_storage(TEST_NODE)
    yield
    path = os.path.join(DATA_DIR, f"node_{TEST_NODE}")
    if os.path.exists(path):
        shutil.rmtree(path)


class TestWriteAheadLog:
    def test_log_and_read(self):
        wal = WriteAheadLog(TEST_NODE)
        wal.log_begin("txn_1")
        wal.log_write("txn_1", "accounts.txt:saldo", "1000", "900")
        wal.log_commit("txn_1")

        entries = wal.read_all()
        assert len(entries) == 3
        assert entries[0].entry_type == "BEGIN"
        assert entries[0].txn_id == "txn_1"
        assert entries[1].entry_type == "WRITE"
        assert entries[1].resource == "accounts.txt:saldo"
        assert entries[1].before_value == "1000"
        assert entries[1].after_value == "900"
        assert entries[2].entry_type == "COMMIT"

    def test_persistence(self):
        wal = WriteAheadLog(TEST_NODE)
        wal.log_begin("txn_1")
        wal.log_write("txn_1", "res:key", "old", "new")
        wal.log_commit("txn_1")

        wal2 = WriteAheadLog(TEST_NODE)
        entries = wal2.read_all()
        assert len(entries) == 3
        assert wal2.next_lsn == 4

    def test_lsn_increments(self):
        wal = WriteAheadLog(TEST_NODE)
        e1 = wal.log_begin("txn_1")
        e2 = wal.log_write("txn_1", "r:k", "a", "b")
        assert e1.lsn == 1
        assert e2.lsn == 2

    def test_read_from_lsn(self):
        wal = WriteAheadLog(TEST_NODE)
        wal.log_begin("txn_1")
        wal.log_write("txn_1", "r:k", "a", "b")
        wal.log_commit("txn_1")

        entries = wal.read_from(2)
        assert len(entries) == 2
        assert entries[0].entry_type == "WRITE"

    def test_checkpoint_entry(self):
        wal = WriteAheadLog(TEST_NODE)
        wal.log_checkpoint(["txn_1", "txn_2"])
        entries = wal.read_all()
        assert len(entries) == 1
        assert entries[0].entry_type == "CHECKPOINT"
        assert entries[0].after_value == "txn_1,txn_2"

    def test_2pc_entries(self):
        wal = WriteAheadLog(TEST_NODE)
        wal.log_prepare("txn_1")
        wal.log_vote_commit("txn_1")
        wal.log_global_commit("txn_1")

        entries = wal.read_all()
        types = [e.entry_type for e in entries]
        assert types == ["PREPARE", "VOTE_COMMIT", "GLOBAL_COMMIT"]


class TestRecoveryManager:
    def test_redo_committed(self):
        wal = WriteAheadLog(TEST_NODE)
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        wal.log_begin("txn_1")
        wal.log_write("txn_1", "accounts.txt:saldo", "1000", "750")
        wal.log_commit("txn_1")

        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        rm = RecoveryManager()
        wal2 = WriteAheadLog(TEST_NODE)
        result = rm.recover(wal2, shared_memory, TEST_NODE)

        assert "txn_1" in result["committed"]
        rec = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert rec["value"] == "750"

    def test_undo_active(self):
        wal = WriteAheadLog(TEST_NODE)
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        wal.log_begin("txn_1")
        wal.log_write("txn_1", "accounts.txt:saldo", "1000", "500")

        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "500", 0)

        rm = RecoveryManager()
        wal2 = WriteAheadLog(TEST_NODE)
        result = rm.recover(wal2, shared_memory, TEST_NODE)

        assert "txn_1" in result["active"]
        rec = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert rec["value"] == "1000"

    def test_ignores_aborted(self):
        wal = WriteAheadLog(TEST_NODE)
        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        wal.log_begin("txn_1")
        wal.log_write("txn_1", "accounts.txt:saldo", "1000", "500")
        wal.log_abort("txn_1")

        rm = RecoveryManager()
        wal2 = WriteAheadLog(TEST_NODE)
        result = rm.recover(wal2, shared_memory, TEST_NODE)

        assert "txn_1" in result["aborted"]
        assert "txn_1" not in result["active"]
        assert "txn_1" not in result["committed"]

    def test_checkpoint_recovery(self):
        wal = WriteAheadLog(TEST_NODE)

        wal.log_begin("txn_old")
        wal.log_write("txn_old", "accounts.txt:saldo", "1000", "800")
        wal.log_commit("txn_old")
        wal.log_checkpoint([])

        wal.log_begin("txn_new")
        wal.log_write("txn_new", "accounts.txt:saldo", "800", "600")
        wal.log_commit("txn_new")

        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "800", 0)

        rm = RecoveryManager()
        wal2 = WriteAheadLog(TEST_NODE)
        result = rm.recover(wal2, shared_memory, TEST_NODE)

        rec = shared_memory.read_record(TEST_NODE, "accounts.txt", "saldo")
        assert rec["value"] == "600"

    def test_undo_deletes_new_key(self):
        wal = WriteAheadLog(TEST_NODE)

        wal.log_begin("txn_1")
        wal.log_write("txn_1", "accounts.txt:new_key", "", "new_value")

        shared_memory.write_record(TEST_NODE, "accounts.txt", "new_key", "new_value", 0)

        rm = RecoveryManager()
        wal2 = WriteAheadLog(TEST_NODE)
        rm.recover(wal2, shared_memory, TEST_NODE)

        rec = shared_memory.read_record(TEST_NODE, "accounts.txt", "new_key")
        assert rec is None


class TestTransactionManagerWithWAL:
    def test_wal_integration(self):
        wal = WriteAheadLog(TEST_NODE)
        tm = TransactionManager(TEST_NODE, wal=wal)

        shared_memory.write_record(TEST_NODE, "accounts.txt", "saldo", "1000", 0)

        txn = tm.begin()
        tm.write(txn, "accounts.txt", "saldo", "900")
        tm.commit(txn)

        entries = wal.read_all()
        types = [e.entry_type for e in entries]
        assert "BEGIN" in types
        assert "WRITE" in types
        assert "COMMIT" in types

    def test_wal_abort_logged(self):
        wal = WriteAheadLog(TEST_NODE)
        tm = TransactionManager(TEST_NODE, wal=wal)

        txn = tm.begin()
        tm.write(txn, "accounts.txt", "saldo", "500")
        tm.abort(txn)

        entries = wal.read_all()
        types = [e.entry_type for e in entries]
        assert "ABORT" in types
