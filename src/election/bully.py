import logging
import threading

from src.config import get_all_other_nodes, NODES
from src.network.message import MSG_ELECTION, MSG_ELECTION_OK, MSG_COORDINATOR


ELECTION_TIMEOUT = 3.0


class BullyElection:
    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()
        self._got_ok = False
        self._in_election = False
        self._timer = None
        self._epoch = 0
        self.election_complete = threading.Event()

        self.node.register_handler(MSG_ELECTION, self.handle_election)
        self.node.register_handler(MSG_ELECTION_OK, self.handle_election_ok)
        self.node.register_handler(MSG_COORDINATOR, self.handle_coordinator)

    def _higher_nodes(self):
        return [nid for nid in NODES if nid > self.node.node_id]

    def start_election(self):
        with self._lock:
            if self._in_election:
                return
            self._in_election = True
            self._got_ok = False
            self._epoch += 1
            epoch = self._epoch
            self.election_complete.clear()

        self.node._log(logging.INFO, "Starting Bully election")
        higher = self._higher_nodes()

        if not higher:
            self._declare_victory(epoch)
            return

        for nid in higher:
            self.node.send(nid, MSG_ELECTION, {"epoch": epoch})

        self._timer = threading.Timer(ELECTION_TIMEOUT, self._on_timeout, args=[epoch])
        self._timer.daemon = True
        self._timer.start()

    def _on_timeout(self, epoch):
        with self._lock:
            if epoch != self._epoch:
                return
            if self._got_ok:
                return
            if not self._in_election:
                return
        self._declare_victory(epoch)

    def _declare_victory(self, epoch):
        with self._lock:
            if epoch != self._epoch:
                return
            self._in_election = False
        self.node.leader_id = self.node.node_id
        self.node._log(logging.INFO, "I am the leader (Bully)!")
        self.node.broadcast(MSG_COORDINATOR, {"leader_id": self.node.node_id, "epoch": epoch})
        self.election_complete.set()
        self.node.on_leader_elected(self.node.node_id)

    def handle_election(self, message):
        if message.sender_id < self.node.node_id:
            self.node.send(message.sender_id, MSG_ELECTION_OK)
            self.start_election()

    def handle_election_ok(self, message):
        with self._lock:
            self._got_ok = True
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def handle_coordinator(self, message):
        leader_id = message.payload["leader_id"]
        msg_epoch = message.payload.get("epoch", 0)
        with self._lock:
            if msg_epoch < self._epoch:
                return
            self._epoch = msg_epoch
            self._in_election = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
        self.node.leader_id = leader_id
        self.node._log(logging.INFO, f"Leader elected: node {leader_id} (Bully)")
        self.election_complete.set()
        self.node.on_leader_elected(leader_id)
