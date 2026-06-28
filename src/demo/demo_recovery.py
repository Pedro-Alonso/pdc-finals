import os
import shutil
import logging

from src.recovery.wal import WriteAheadLog
from src.recovery.recovery import RecoveryManager
from src.storage.transaction import TransactionManager
from src.storage import shared_memory
from src.config import DATA_DIR


DEMO_NODE = 60
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")


def setup():
    shared_memory.init_storage(DEMO_NODE)


def cleanup():
    path = os.path.join(DATA_DIR, f"node_{DEMO_NODE}")
    if os.path.exists(path):
        shutil.rmtree(path)


def read_current(key, resource="accounts.txt"):
    rec = shared_memory.read_record(DEMO_NODE, resource, key)
    return rec["value"] if rec else None


def scenario_wal_commit():
    print("\n" + "=" * 60)
    print("SCENARIO 1: WAL + Normal commit")
    print("=" * 60)
    cleanup()
    setup()

    wal = WriteAheadLog(DEMO_NODE)
    tm = TransactionManager(DEMO_NODE, wal=wal)

    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "1000", 0)

    txn = tm.begin()
    print(f"  BEGIN {txn}")

    tm.write(txn, "accounts.txt", "saldo", "900")
    tm.write(txn, "accounts.txt", "bonus", "50")
    tm.write(txn, "accounts.txt", "taxa", "10")
    print(f"  Wrote saldo=900, bonus=50, taxa=10")

    tm.commit(txn)
    print(f"  COMMIT {txn}")

    entries = wal.read_all()
    print(f"\n  WAL entries ({len(entries)}):")
    for e in entries:
        print(f"    LSN={e.lsn} type={e.entry_type} resource={e.resource} "
              f"before={e.before_value!r} after={e.after_value!r}")

    assert len(entries) == 5, f"Expected 5 entries (BEGIN + 3 WRITE + COMMIT), got {len(entries)}"

    types = [e.entry_type for e in entries]
    assert types == ["BEGIN", "WRITE", "WRITE", "WRITE", "COMMIT"]

    assert read_current("saldo") == "900"
    assert read_current("bonus") == "50"
    assert read_current("taxa") == "10"
    print("\n  OK - PASSED")
    cleanup()


def scenario_crash_before_commit():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Crash before commit (Undo)")
    print("=" * 60)
    cleanup()
    setup()

    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "1000", 0)
    shared_memory.write_record(DEMO_NODE, "accounts.txt", "bonus", "100", 0)

    wal = WriteAheadLog(DEMO_NODE)
    tm = TransactionManager(DEMO_NODE, wal=wal)

    txn = tm.begin()
    tm.write(txn, "accounts.txt", "saldo", "500")
    tm.write(txn, "accounts.txt", "bonus", "999")
    print(f"  {txn} wrote saldo=500, bonus=999 (uncommitted)")

    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "500", 0)
    shared_memory.write_record(DEMO_NODE, "accounts.txt", "bonus", "999", 0)
    print("  --- SIMULATING CRASH (data written but no COMMIT in WAL) ---")

    print(f"  Data after crash: saldo={read_current('saldo')}, bonus={read_current('bonus')}")

    wal2 = WriteAheadLog(DEMO_NODE)
    rm = RecoveryManager()
    result = rm.recover(wal2, shared_memory, DEMO_NODE)
    print(f"\n  Recovery result: {result}")

    saldo = read_current("saldo")
    bonus = read_current("bonus")
    print(f"  After recovery: saldo={saldo}, bonus={bonus}")

    assert saldo == "1000", f"Expected saldo=1000 (undo), got {saldo}"
    assert bonus == "100", f"Expected bonus=100 (undo), got {bonus}"

    post_entries = wal2.read_all()
    abort_entries = [e for e in post_entries if e.entry_type == "ABORT"]
    assert len(abort_entries) == 1, f"Expected 1 ABORT entry, got {len(abort_entries)}"
    print("  OK - PASSED")
    cleanup()


def scenario_crash_after_commit():
    print("\n" + "=" * 60)
    print("SCENARIO 3: Crash after commit (Redo)")
    print("=" * 60)
    cleanup()
    setup()

    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "1000", 0)

    wal = WriteAheadLog(DEMO_NODE)
    tm = TransactionManager(DEMO_NODE, wal=wal)

    txn = tm.begin()
    tm.write(txn, "accounts.txt", "saldo", "750")
    tm.write(txn, "accounts.txt", "bonus", "25")
    tm.commit(txn)
    print(f"  {txn} committed (saldo=750, bonus=25)")

    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "1000", 0)
    shared_memory.delete_record(DEMO_NODE, "accounts.txt", "bonus")
    print("  --- SIMULATING CRASH (data corrupted/lost after commit) ---")
    print(f"  Data after crash: saldo={read_current('saldo')}, bonus={read_current('bonus')}")

    wal2 = WriteAheadLog(DEMO_NODE)
    rm = RecoveryManager()
    result = rm.recover(wal2, shared_memory, DEMO_NODE)
    print(f"\n  Recovery result: {result}")

    saldo = read_current("saldo")
    bonus = read_current("bonus")
    print(f"  After recovery: saldo={saldo}, bonus={bonus}")

    assert saldo == "750", f"Expected saldo=750 (redo), got {saldo}"
    assert bonus == "25", f"Expected bonus=25 (redo), got {bonus}"
    print("  OK - PASSED")
    cleanup()


def main():
    print("=" * 60)
    print("  RECOVERY DEMO")
    print("  Write-Ahead Log | Redo | Undo")
    print("=" * 60)

    scenario_wal_commit()
    scenario_crash_before_commit()
    scenario_crash_after_commit()

    print("\n" + "=" * 60)
    print("  ALL RECOVERY SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
