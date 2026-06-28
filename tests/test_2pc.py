import os
import time
import shutil
import threading

import pytest

from src.node import Node
from src.recovery.wal import WriteAheadLog
from src.commit.two_phase import TwoPhaseCoordinator, TwoPhaseParticipant
from src.storage.transaction import TransactionManager, Transaction
from src.storage import shared_memory
from src.config import DATA_DIR


@pytest.fixture(autouse=True)
def clean_nodes():
    yield
    for nid in range(4):
        path = os.path.join(DATA_DIR, f"node_{nid}")
        if os.path.exists(path):
            shutil.rmtree(path)


def _start_nodes(count=3):
    nodes = []
    for nid in range(count):
        n = Node(nid)
        n.start()
        nodes.append(n)
    time.sleep(0.5)
    return nodes


def _stop_nodes(nodes):
    for n in nodes:
        try:
            n.stop()
        except Exception:
            pass
    time.sleep(0.3)


def _register_txn(node, txn_id):
    node.txn_manager._seq += 1
    txn = Transaction(txn_id)
    node.txn_manager._transactions[txn_id] = txn


class TestTwoPhaseCommit:
    def test_all_commit(self):
        nodes = _start_nodes(3)
        try:
            txn_id = "test_2pc_1"
            for nid in [1, 2]:
                _register_txn(nodes[nid], txn_id)

            result = nodes[0].two_phase_coord.coordinate_commit(txn_id, [1, 2])
            assert result is True
        finally:
            _stop_nodes(nodes)

    def test_one_abort(self):
        nodes = _start_nodes(3)
        try:
            txn_id = "test_2pc_2"
            _register_txn(nodes[1], txn_id)

            nodes[2].two_phase_part._check_can_commit = lambda tid: False

            result = nodes[0].two_phase_coord.coordinate_commit(txn_id, [1, 2])
            assert result is False
        finally:
            _stop_nodes(nodes)

    def test_timeout_abort(self):
        nodes = _start_nodes(3)
        try:
            txn_id = "test_2pc_3"
            _register_txn(nodes[1], txn_id)

            nodes[0].two_phase_coord.vote_timeout = 1.5
            nodes[0].two_phase_coord.ack_timeout = 0.5
            nodes[0].two_phase_coord.ack_retries = 0

            nodes[2].stop()
            time.sleep(0.3)

            result = nodes[0].two_phase_coord.coordinate_commit(txn_id, [1, 2])
            assert result is False
        finally:
            _stop_nodes(nodes)

    def test_coordinator_log_persist(self):
        nodes = _start_nodes(3)
        try:
            txn_id = "test_2pc_4"
            for nid in [1, 2]:
                _register_txn(nodes[nid], txn_id)

            result = nodes[0].two_phase_coord.coordinate_commit(txn_id, [1, 2])
            assert result is True

            entries = nodes[0].wal.read_all()
            types = [e.entry_type for e in entries if e.txn_id == txn_id]
            assert "PREPARE" in types
            assert "GLOBAL_COMMIT" in types
        finally:
            _stop_nodes(nodes)


class TestTwoPhaseRecovery:
    def test_coordinator_recover_undecided(self):
        wal = WriteAheadLog(97)
        try:
            wal.log_prepare("txn_pending")

            class FakeNode:
                node_id = 97

            coord = TwoPhaseCoordinator(FakeNode(), wal=wal)
            coord.recover_pending()

            entries = wal.read_all()
            types = [e.entry_type for e in entries if e.txn_id == "txn_pending"]
            assert "GLOBAL_ABORT" in types
        finally:
            path = os.path.join(DATA_DIR, "node_97")
            if os.path.exists(path):
                shutil.rmtree(path)

    def test_participant_recover_voted_abort(self):
        wal = WriteAheadLog(96)
        try:
            wal.log_vote_abort("txn_voted_abort")

            class FakeNode:
                node_id = 96

            part = TwoPhaseParticipant(FakeNode(), wal=wal)
            part.recover_pending()

            entries = wal.read_all()
            types = [e.entry_type for e in entries if e.txn_id == "txn_voted_abort"]
            assert "ABORT" in types
        finally:
            path = os.path.join(DATA_DIR, "node_96")
            if os.path.exists(path):
                shutil.rmtree(path)
