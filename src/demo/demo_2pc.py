import os
import time
import shutil
import logging
import threading

from src.node import Node
from src.storage import shared_memory
from src.config import DATA_DIR


logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")


def cleanup_nodes(*node_ids):
    for nid in node_ids:
        path = os.path.join(DATA_DIR, f"node_{nid}")
        if os.path.exists(path):
            shutil.rmtree(path)


def scenario_all_commit():
    print("\n" + "=" * 60)
    print("SCENARIO 1: 2PC — all commit")
    print("=" * 60)
    cleanup_nodes(0, 1, 2)

    nodes = []
    for nid in range(3):
        n = Node(nid)
        n.start()
        nodes.append(n)
    time.sleep(0.5)

    coordinator = nodes[0]
    participants = [1, 2]

    for nid in range(3):
        shared_memory.write_record(nid, "accounts.txt", "saldo", "1000", 0)

    for nid in participants:
        txn_id = f"dist_txn_1"
        nodes[nid].txn_manager.begin()
        nodes[nid].txn_manager._transactions[nodes[nid].txn_manager._transactions.keys().__iter__().__next__()].txn_id = txn_id
        nodes[nid].txn_manager._transactions[txn_id] = nodes[nid].txn_manager._transactions.pop(
            list(nodes[nid].txn_manager._transactions.keys())[0]
        )

    txn_id = "dist_txn_1"
    print(f"  Coordinator (node 0) starts 2PC for {txn_id}")
    print(f"  Participants: nodes {participants}")

    result = coordinator.two_phase_coord.coordinate_commit(txn_id, participants)

    print(f"  Result: {'GLOBAL_COMMIT' if result else 'GLOBAL_ABORT'}")
    assert result is True, "Expected GLOBAL_COMMIT"

    print("  OK - PASSED")

    for n in nodes:
        n.stop()
    time.sleep(0.3)
    cleanup_nodes(0, 1, 2)


def scenario_one_abort():
    print("\n" + "=" * 60)
    print("SCENARIO 2: 2PC — one participant votes abort")
    print("=" * 60)
    cleanup_nodes(0, 1, 2)

    nodes = []
    for nid in range(3):
        n = Node(nid)
        n.start()
        nodes.append(n)
    time.sleep(0.5)

    coordinator = nodes[0]
    participants = [1, 2]
    txn_id = "dist_txn_2"

    nodes[1].txn_manager._seq += 1
    from src.storage.transaction import Transaction
    nodes[1].txn_manager._transactions[txn_id] = Transaction(txn_id)

    original_check = nodes[2].two_phase_part._check_can_commit

    def force_abort(tid):
        return False

    nodes[2].two_phase_part._check_can_commit = force_abort

    print(f"  Coordinator (node 0) starts 2PC for {txn_id}")
    print(f"  Node 2 will vote ABORT (forced)")

    result = coordinator.two_phase_coord.coordinate_commit(txn_id, participants)

    print(f"  Result: {'GLOBAL_COMMIT' if result else 'GLOBAL_ABORT'}")
    assert result is False, "Expected GLOBAL_ABORT"

    nodes[2].two_phase_part._check_can_commit = original_check

    print("  OK - PASSED")

    for n in nodes:
        n.stop()
    time.sleep(0.3)
    cleanup_nodes(0, 1, 2)


def scenario_timeout_abort():
    print("\n" + "=" * 60)
    print("SCENARIO 3: 2PC — participant timeout (abort)")
    print("=" * 60)
    cleanup_nodes(0, 1, 2)

    nodes = []
    for nid in range(3):
        n = Node(nid)
        n.start()
        nodes.append(n)
    time.sleep(0.5)

    coordinator = nodes[0]
    coordinator.two_phase_coord.vote_timeout = 2.0
    coordinator.two_phase_coord.ack_timeout = 1.0
    coordinator.two_phase_coord.ack_retries = 0

    participants = [1, 2]
    txn_id = "dist_txn_3"

    nodes[1].txn_manager._seq += 1
    from src.storage.transaction import Transaction
    nodes[1].txn_manager._transactions[txn_id] = Transaction(txn_id)

    print(f"  Coordinator (node 0) starts 2PC for {txn_id}")
    print(f"  Node 2 is stopped before voting (simulating crash)")
    nodes[2].stop()
    time.sleep(0.3)

    result = coordinator.two_phase_coord.coordinate_commit(txn_id, participants)

    print(f"  Result: {'GLOBAL_COMMIT' if result else 'GLOBAL_ABORT'}")
    assert result is False, "Expected GLOBAL_ABORT (timeout)"

    print("  OK - PASSED")

    for n in [nodes[0], nodes[1]]:
        n.stop()
    time.sleep(0.3)
    cleanup_nodes(0, 1, 2)


def main():
    print("=" * 60)
    print("  TWO-PHASE COMMIT (2PC) DEMO")
    print("  Coordinator | Participants | Vote | Decision")
    print("=" * 60)

    scenario_all_commit()
    scenario_one_abort()
    scenario_timeout_abort()

    print("\n" + "=" * 60)
    print("  ALL 2PC SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
