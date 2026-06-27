import argparse
import logging
import time

from src.config import NODES
from src.network.clock import LamportClock
from src.network.message import Message, MSG_PING, MSG_PONG
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
        if self._enable_failure_detector:
            self._init_failure_detector()
            self._init_consensus()
        self.server.start()
        if self.failure_detector:
            self.failure_detector.start()
        self._log(logging.INFO, f"Node {self.node_id} started")

    def _handle_ping(self, message):
        self.send(message.sender_id, MSG_PONG)

    def stop(self):
        self._running = False
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
