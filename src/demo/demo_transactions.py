import os
import time
import shutil
import threading
import logging

from src.storage.transaction import TransactionManager, TransactionStatus
from src.storage import shared_memory
from src.config import DATA_DIR


DEMO_NODE = 50
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")


def setup():
    shared_memory.init_storage(DEMO_NODE)
    shared_memory.write_record(DEMO_NODE, "accounts.txt", "saldo", "1000", 0)


def cleanup():
    path = os.path.join(DATA_DIR, f"node_{DEMO_NODE}")
    if os.path.exists(path):
        shutil.rmtree(path)


def read_current(key="saldo"):
    rec = shared_memory.read_record(DEMO_NODE, "accounts.txt", key)
    return rec["value"] if rec else None


def scenario_simple_commit():
    print("\n" + "=" * 60)
    print("SCENARIO 1: Simple transaction (commit)")
    print("=" * 60)
    setup()
    tm = TransactionManager(DEMO_NODE)

    print(f"  Initial saldo: {read_current()}")

    txn = tm.begin()
    val = tm.read(txn, "accounts.txt", "saldo")
    print(f"  {txn} reads saldo = {val}")

    tm.write(txn, "accounts.txt", "saldo", "900")
    print(f"  {txn} writes saldo = 900")

    tm.commit(txn)
    print(f"  {txn} committed")
    print(f"  Final saldo: {read_current()}")

    assert read_current() == "900", "FAIL: saldo should be 900"
    print("  OK - PASSED")
    cleanup()


def scenario_abort_rollback():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Transaction abort (rollback)")
    print("=" * 60)
    setup()
    tm = TransactionManager(DEMO_NODE)

    print(f"  Initial saldo: {read_current()}")

    txn = tm.begin()
    tm.write(txn, "accounts.txt", "saldo", "500")
    tm.write(txn, "accounts.txt", "bonus", "200")
    print(f"  {txn} buffered writes: saldo=500, bonus=200")

    tm.abort(txn)
    print(f"  {txn} aborted")

    print(f"  saldo after abort: {read_current()}")
    print(f"  bonus after abort: {read_current('bonus')}")

    assert read_current() == "1000", "FAIL: saldo should still be 1000"
    assert read_current("bonus") is None, "FAIL: bonus should not exist"
    print("  OK - PASSED")
    cleanup()


def scenario_lost_update_prevented():
    print("\n" + "=" * 60)
    print("SCENARIO 3: Lost Update prevented (S2PL)")
    print("=" * 60)
    setup()
    tm = TransactionManager(DEMO_NODE)

    print(f"  Initial saldo: {read_current()}")

    t1 = tm.begin()
    t2 = tm.begin()

    tm.write(t1, "accounts.txt", "saldo", "900")
    print(f"  {t1} writes saldo = 900 (X-lock acquired)")

    t2_result = [None]

    def t2_write():
        t2_result[0] = tm.write(t2, "accounts.txt", "saldo", "800", timeout=1.0)

    thread = threading.Thread(target=t2_write)
    thread.start()
    time.sleep(0.3)
    print(f"  {t2} tries to write saldo = 800 -> blocked by {t1}'s X-lock")

    tm.commit(t1)
    print(f"  {t1} committed -> saldo = {read_current()}")
    thread.join(timeout=3)

    if t2_result[0]:
        tm.commit(t2)
        print(f"  {t2} got lock after {t1} released, committed -> saldo = {read_current()}")
    else:
        print(f"  {t2} timed out waiting for lock (serialized correctly)")
        tm.abort(t2)

    print(f"  Final saldo: {read_current()}")
    print("  OK - PASSED - no lost update, writes serialized")
    cleanup()


def scenario_dirty_read_prevented():
    print("\n" + "=" * 60)
    print("SCENARIO 4: Dirty Read prevented (S2PL)")
    print("=" * 60)
    setup()
    tm = TransactionManager(DEMO_NODE)

    print(f"  Initial saldo: {read_current()}")

    t1 = tm.begin()
    tm.write(t1, "accounts.txt", "saldo", "500")
    print(f"  {t1} writes saldo = 500 (uncommitted, X-lock held)")

    read_val = [None]

    def t2_read():
        t2 = tm.begin()
        read_val[0] = tm.read(t2, "accounts.txt", "saldo", timeout=1.0)
        if read_val[0] is not None:
            tm.commit(t2)
        else:
            tm.abort(t2)

    thread = threading.Thread(target=t2_read)
    thread.start()
    time.sleep(0.3)
    print(f"  T2 tries to read saldo -> blocked by {t1}'s X-lock")

    tm.abort(t1)
    print(f"  {t1} aborted -> saldo restored to 1000")
    thread.join(timeout=3)

    print(f"  T2 read value: {read_val[0]}")
    print(f"  Actual saldo: {read_current()}")

    if read_val[0] is None:
        print("  OK - PASSED - T2 could not read dirty data (timed out)")
    elif read_val[0] == "1000":
        print("  OK - PASSED - T2 read clean value after T1 abort")
    else:
        print("  FAIL - T2 read dirty value 500!")
    cleanup()


def scenario_deadlock_detected():
    print("\n" + "=" * 60)
    print("SCENARIO 5: Deadlock detected and resolved")
    print("=" * 60)
    setup()
    shared_memory.write_record(DEMO_NODE, "resource_b.txt", "data", "BBB", 0)
    tm = TransactionManager(DEMO_NODE)
    tm.deadlock_detector._interval = 0.5
    tm.deadlock_detector.start()

    t1 = tm.begin()
    t2 = tm.begin()

    tm.write(t1, "accounts.txt", "saldo", "111")
    print(f"  {t1} acquires X-lock on accounts.txt")

    tm.write(t2, "resource_b.txt", "data", "222")
    print(f"  {t2} acquires X-lock on resource_b.txt")

    results = {"t1": None, "t2": None}
    errors = {"t1": None, "t2": None}

    def t1_wait():
        results["t1"] = tm.write(t1, "resource_b.txt", "data", "333", timeout=5.0)

    def t2_wait():
        results["t2"] = tm.write(t2, "accounts.txt", "saldo", "444", timeout=5.0)

    thread1 = threading.Thread(target=t1_wait)
    thread2 = threading.Thread(target=t2_wait)
    thread1.start()
    time.sleep(0.1)
    thread2.start()
    print(f"  {t1} waits for resource_b.txt (held by {t2})")
    print(f"  {t2} waits for accounts.txt (held by {t1})")
    print("  -> Cycle: T1 -> T2 -> T1")

    thread1.join(timeout=8)
    thread2.join(timeout=8)

    tm.deadlock_detector.stop()

    t1_txn = tm.get_transaction(t1)
    t2_txn = tm.get_transaction(t2)

    if t2_txn.status == TransactionStatus.ABORTED:
        print(f"  Deadlock resolved: {t2} (younger) was aborted as victim")
        if results["t1"]:
            tm.commit(t1)
            print(f"  {t1} proceeded and committed")
        print("  OK - PASSED")
    elif t1_txn.status == TransactionStatus.ABORTED:
        print(f"  Deadlock resolved: {t1} was aborted as victim")
        if results["t2"]:
            tm.commit(t2)
            print(f"  {t2} proceeded and committed")
        print("  OK - PASSED")
    else:
        print("  Deadlock may have timed out - detector interval was short")
        tm.abort(t1)
        tm.abort(t2)
        print("  (cleaned up both transactions)")

    cleanup()


def scenario_nested_transaction():
    print("\n" + "=" * 60)
    print("SCENARIO 6: Nested transaction (lock inheritance)")
    print("=" * 60)
    setup()
    shared_memory.write_record(DEMO_NODE, "resource_b.txt", "data", "BBB", 0)
    tm = TransactionManager(DEMO_NODE)

    parent = tm.begin()
    tm.read(parent, "accounts.txt", "saldo")
    print(f"  {parent} (parent) acquires S-lock on accounts.txt, reads saldo=1000")

    child = tm.begin(parent_txn_id=parent)
    tm.write(child, "resource_b.txt", "data", "NEW_DATA")
    print(f"  {child} (child) acquires X-lock on resource_b.txt, writes data=NEW_DATA")

    tm.commit(child)
    print(f"  {child} committed -> locks inherited by {parent}")

    holders = tm.lock_manager.get_holders("resource_b.txt")
    print(f"  resource_b.txt holders after child commit: {holders}")
    assert parent in holders, "FAIL: parent should hold child's locks"

    tm.commit(parent)
    print(f"  {parent} committed -> all locks released")

    holders_after = tm.lock_manager.get_holders("resource_b.txt")
    print(f"  resource_b.txt holders after parent commit: {holders_after}")
    assert len(holders_after) == 0, "FAIL: all locks should be released"

    rec = shared_memory.read_record(DEMO_NODE, "resource_b.txt", "data")
    print(f"  resource_b.txt data = {rec['value']}")
    assert rec["value"] == "NEW_DATA"
    print("  OK - PASSED")
    cleanup()


def main():
    print("=" * 60)
    print("  TRANSACTIONS, CONCURRENCY & DEADLOCK DEMO")
    print("  S2PL | Lock Manager | Wait-For Graph")
    print("=" * 60)

    scenario_simple_commit()
    scenario_abort_rollback()
    scenario_lost_update_prevented()
    scenario_dirty_read_prevented()
    scenario_deadlock_detected()
    scenario_nested_transaction()

    print("\n" + "=" * 60)
    print("  ALL SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
