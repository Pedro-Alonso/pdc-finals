import argparse
import logging
import time

from src.config import NODES
from src.network.clock import LamportClock
from src.network.message import (
    Message, MSG_PING, MSG_PONG,
    MSG_TXN_BEGIN, MSG_TXN_READ, MSG_TXN_READ_RESPONSE,
    MSG_TXN_WRITE, MSG_TXN_WRITE_ACK, MSG_TXN_COMMIT, MSG_TXN_ABORT,
    MSG_LOCK_REQUEST, MSG_LOCK_GRANT, MSG_LOCK_DENY, MSG_LOCK_RELEASE,
    MSG_2PC_PREPARE, MSG_2PC_VOTE_COMMIT, MSG_2PC_VOTE_ABORT,
    MSG_2PC_GLOBAL_COMMIT, MSG_2PC_GLOBAL_ABORT, MSG_2PC_ACK,
)
from src.network.transport import TransportServer, TransportClient
from src.storage.shared_memory import init_storage


class Node:
    def __init__(self, node_id, election_algorithm=None, enable_failure_detector=False):
        self.node_id = node_id
        self.clock = LamportClock()
        self.handlers = {}
        self.leader_id = None
        self.election_algorithm = election_algorithm
        self._election = None
        self._running = False
        self._enable_failure_detector = enable_failure_detector
        self.failure_detector = None
        self.consensus = None
        self.txn_manager = None
        self.wal = None
        self.recovery_manager = None
        self.two_phase_coord = None
        self.two_phase_part = None

        node_info = NODES[node_id]
        self.server = TransportServer(
            host=node_info["host"],
            port=node_info["port"],
            on_message=self._dispatch,
        )
        self.client = TransportClient()

        self.logger = logging.getLogger(f"node.{node_id}.{id(self)}")
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(f"[NODE-{node_id}] [%(lamport_ts)s] %(message)s"))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        init_storage(node_id)

    def _log(self, level, msg):
        self.logger.log(level, msg, extra={"lamport_ts": self.clock.get_time()})

    def register_handler(self, msg_type, handler):
        self.handlers[msg_type] = handler

    def _dispatch(self, message):
        if not self._running:
            return
        self.clock.update(message.timestamp)
        self._log(logging.INFO, f"Received {message.type} from node {message.sender_id}")

        handler = self.handlers.get(message.type)
        if handler:
            handler(message)
        else:
            self._log(logging.WARNING, f"No handler for message type: {message.type}")

    def send(self, target_id, msg_type, payload=None):
        ts = self.clock.increment()
        msg = Message(
            msg_type=msg_type,
            sender_id=self.node_id,
            receiver_id=target_id,
            timestamp=ts,
            payload=payload,
        )
        self._log(logging.INFO, f"Sending {msg_type} to node {target_id}")
        return self.client.send(target_id, msg)

    def broadcast(self, msg_type, payload=None):
        ts = self.clock.increment()
        msg = Message(
            msg_type=msg_type,
            sender_id=self.node_id,
            timestamp=ts,
            payload=payload,
        )
        self._log(logging.INFO, f"Broadcasting {msg_type}")
        self.client.broadcast(msg, exclude=[self.node_id])

    def is_leader(self):
        return self.leader_id == self.node_id

    def on_leader_elected(self, leader_id):
        self._log(logging.INFO, f"Leader set to node {leader_id}")

    def start_election(self):
        if self._election:
            self._election.start_election()

    def _init_election(self):
        if self.election_algorithm == "chang_roberts":
            from src.election.chang_roberts import ChangRobertsElection
            self._election = ChangRobertsElection(self)
        elif self.election_algorithm == "bully":
            from src.election.bully import BullyElection
            self._election = BullyElection(self)

    def _init_failure_detector(self):
        from src.consensus.failure_detector import FailureDetector
        self.failure_detector = FailureDetector(
            self,
            on_failure=self._on_node_failure,
            on_recovery=self._on_node_recovery,
        )

    def _init_consensus(self):
        from src.consensus.consensus import ConsensusProtocol
        self.consensus = ConsensusProtocol(self, failure_detector=self.failure_detector)

    def _init_recovery(self):
        from src.recovery.wal import WriteAheadLog
        from src.recovery.recovery import RecoveryManager
        from src.storage import shared_memory

        self.wal = WriteAheadLog(self.node_id)
        self.recovery_manager = RecoveryManager()
        self.recovery_manager.recover(self.wal, shared_memory, self.node_id)

    def _init_transactions(self):
        from src.storage.transaction import TransactionManager
        self.txn_manager = TransactionManager(self.node_id, wal=self.wal)
        self.register_handler(MSG_TXN_READ, self._handle_txn_read)
        self.register_handler(MSG_TXN_WRITE, self._handle_txn_write)
        self.register_handler(MSG_TXN_COMMIT, self._handle_txn_commit)
        self.register_handler(MSG_TXN_ABORT, self._handle_txn_abort)
        self.register_handler(MSG_TXN_READ_RESPONSE, self._handle_txn_read_response)
        self.register_handler(MSG_TXN_WRITE_ACK, self._handle_txn_write_ack)

    def _init_2pc(self):
        from src.commit.two_phase import TwoPhaseCoordinator, TwoPhaseParticipant
        self.two_phase_coord = TwoPhaseCoordinator(self, wal=self.wal)
        self.two_phase_part = TwoPhaseParticipant(self, wal=self.wal, txn_manager=self.txn_manager)
        self.register_handler(MSG_2PC_PREPARE, self.two_phase_part.handle_prepare)
        self.register_handler(MSG_2PC_VOTE_COMMIT, self.two_phase_coord.handle_vote_commit)
        self.register_handler(MSG_2PC_VOTE_ABORT, self.two_phase_coord.handle_vote_abort)
        self.register_handler(MSG_2PC_GLOBAL_COMMIT, self.two_phase_part.handle_global_commit)
        self.register_handler(MSG_2PC_GLOBAL_ABORT, self.two_phase_part.handle_global_abort)
        self.register_handler(MSG_2PC_ACK, self.two_phase_coord.handle_ack)

    def _handle_txn_read(self, message):
        p = message.payload
        txn_id = p["txn_id"]
        resource = p["resource"]
        key = p["key"]
        value = self.txn_manager.read(txn_id, resource, key, timeout=5.0)
        self.send(message.sender_id, MSG_TXN_READ_RESPONSE, {
            "txn_id": txn_id, "key": key, "value": value,
        })

    def _handle_txn_write(self, message):
        p = message.payload
        txn_id = p["txn_id"]
        resource = p["resource"]
        key = p["key"]
        value = p["value"]
        ok = self.txn_manager.write(txn_id, resource, key, value, timeout=5.0)
        self.send(message.sender_id, MSG_TXN_WRITE_ACK, {
            "txn_id": txn_id, "key": key, "success": ok,
        })

    def _handle_txn_commit(self, message):
        txn_id = message.payload["txn_id"]
        self.txn_manager.commit(txn_id)

    def _handle_txn_abort(self, message):
        txn_id = message.payload["txn_id"]
        self.txn_manager.abort(txn_id)

    def _handle_txn_read_response(self, message):
        pass

    def _handle_txn_write_ack(self, message):
        pass

    def _on_node_failure(self, node_id):
        self._log(logging.WARNING, f"Detected failure of node {node_id}")
        if node_id == self.leader_id and self._election:
            self._log(logging.INFO, "Leader failed — starting re-election")
            self._election.election_complete.clear()
            self.start_election()

    def _on_node_recovery(self, node_id):
        self._log(logging.INFO, f"Detected recovery of node {node_id}")

    def start(self):
        self._running = True
        self.register_handler(MSG_PING, self._handle_ping)
        self._init_election()
        self._init_recovery()
        self._init_transactions()
        self._init_2pc()
        if self._enable_failure_detector:
            self._init_failure_detector()
            self._init_consensus()
        self.server.start()
        if self.failure_detector:
            self.failure_detector.start()
        if self.txn_manager:
            self.txn_manager.start()
        self._log(logging.INFO, f"Node {self.node_id} started")

    def _handle_ping(self, message):
        self.send(message.sender_id, MSG_PONG)

    def stop(self):
        self._running = False
        if self.txn_manager:
            self.txn_manager.stop()
        if self.failure_detector:
            self.failure_detector.stop()
        self.server.stop()
        self.client.close_all()
        self._log(logging.INFO, f"Node {self.node_id} stopped")


def main():
    parser = argparse.ArgumentParser(description="Distributed Shared Memory Node")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    args = parser.parse_args()

    node = Node(args.id)
    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
