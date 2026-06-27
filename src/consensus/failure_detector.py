import logging
import threading
import time

from src.config import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, get_all_other_nodes
from src.network.message import MSG_HEARTBEAT, MSG_HEARTBEAT_ACK


class FailureDetector:
    def __init__(self, node, on_failure=None, on_recovery=None):
        self.node = node
        self._on_failure = on_failure
        self._on_recovery = on_recovery
        self._lock = threading.Lock()
        self._running = False
        self._thread = None

        self.last_seen = {}
        self.suspected = set()
        self.alive = set()

        for nid in get_all_other_nodes(node.node_id):
            self.last_seen[nid] = time.time()
            self.alive.add(nid)

        self.node.register_handler(MSG_HEARTBEAT, self._handle_heartbeat)
        self.node.register_handler(MSG_HEARTBEAT_ACK, self._handle_heartbeat_ack)

    def _handle_heartbeat(self, message):
        self._mark_alive(message.sender_id)
        self.node.send(message.sender_id, MSG_HEARTBEAT_ACK)

    def _handle_heartbeat_ack(self, message):
        self._mark_alive(message.sender_id)

    def _mark_alive(self, node_id):
        with self._lock:
            self.last_seen[node_id] = time.time()
            if node_id in self.suspected:
                self.suspected.discard(node_id)
                self.alive.add(node_id)
                self.node._log(logging.INFO, f"Node {node_id} recovered")
                if self._on_recovery:
                    threading.Thread(
                        target=self._on_recovery, args=(node_id,), daemon=True
                    ).start()

    def _detector_loop(self):
        while self._running:
            self._send_heartbeats()
            time.sleep(HEARTBEAT_INTERVAL)
            self._check_timeouts()

    def _send_heartbeats(self):
        with self._lock:
            targets = [nid for nid in get_all_other_nodes(self.node.node_id)
                       if nid not in self.suspected]
        for nid in targets:
            self.node.send(nid, MSG_HEARTBEAT)

    def _check_timeouts(self):
        now = time.time()
        with self._lock:
            for nid in get_all_other_nodes(self.node.node_id):
                elapsed = now - self.last_seen.get(nid, 0)
                if elapsed > HEARTBEAT_TIMEOUT and nid not in self.suspected:
                    self.suspected.add(nid)
                    self.alive.discard(nid)
                    self.node._log(logging.WARNING, f"Node {nid} suspected dead (no heartbeat for {elapsed:.1f}s)")
                    if self._on_failure:
                        threading.Thread(
                            target=self._on_failure, args=(nid,), daemon=True
                        ).start()

    def is_alive(self, node_id):
        with self._lock:
            return node_id in self.alive

    def get_alive_nodes(self):
        with self._lock:
            return set(self.alive)

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._detector_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)
