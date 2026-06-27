import logging
import threading

from src.config import RING_ORDER, get_successor
from src.network.message import MSG_ELECTION_RING, MSG_ELECTED_RING


class ChangRobertsElection:
    def __init__(self, node):
        self.node = node
        self.participant = False
        self._lock = threading.Lock()
        self.election_complete = threading.Event()

        self.node.register_handler(MSG_ELECTION_RING, self.handle_election_ring)
        self.node.register_handler(MSG_ELECTED_RING, self.handle_elected_ring)

    def _send_to_successor(self, msg_type, payload):
        tried = set()
        current = get_successor(self.node.node_id)
        while current != self.node.node_id:
            if current in tried:
                break
            if self.node.send(current, msg_type, payload):
                return True
            tried.add(current)
            current = get_successor(current)
        return False

    def start_election(self):
        with self._lock:
            self.participant = True
            self.election_complete.clear()
        self.node._log(logging.INFO, f"Starting Chang-Roberts election, candidate={self.node.node_id}")
        self._send_to_successor(MSG_ELECTION_RING, {"candidate_id": self.node.node_id})

    def handle_election_ring(self, message):
        candidate_id = message.payload["candidate_id"]
        with self._lock:
            if candidate_id > self.node.node_id:
                self.participant = True
                self._send_to_successor(MSG_ELECTION_RING, {"candidate_id": candidate_id})
            elif candidate_id < self.node.node_id:
                if not self.participant:
                    self.participant = True
                    self._send_to_successor(MSG_ELECTION_RING, {"candidate_id": self.node.node_id})
            else:
                self.participant = False
                self.node.leader_id = self.node.node_id
                self.node._log(logging.INFO, f"I am the leader (Chang-Roberts)!")
                self._send_to_successor(MSG_ELECTED_RING, {"leader_id": self.node.node_id})
                self.election_complete.set()
                self.node.on_leader_elected(self.node.node_id)

    def handle_elected_ring(self, message):
        leader_id = message.payload["leader_id"]
        with self._lock:
            self.participant = False
        self.node.leader_id = leader_id
        self.node._log(logging.INFO, f"Leader elected: node {leader_id} (Chang-Roberts)")
        self.election_complete.set()
        self.node.on_leader_elected(leader_id)
        if leader_id != self.node.node_id:
            self._send_to_successor(MSG_ELECTED_RING, {"leader_id": leader_id})
