import logging
import threading

from src.network.message import (
    MSG_MAEKAWA_REQUEST, MSG_MAEKAWA_LOCKED, MSG_MAEKAWA_FAILED,
    MSG_MAEKAWA_INQUIRE, MSG_MAEKAWA_RELINQUISH, MSG_MAEKAWA_RELEASE,
)


QUORUMS = {
    0: {0, 1, 3},
    1: {0, 1, 2},
    2: {1, 2, 3},
    3: {0, 2, 3},
}


def get_quorum(node_id):
    return QUORUMS.get(node_id, set())


class MaekawaExclusion:
    def __init__(self, node):
        self.node = node
        self.quorum = get_quorum(node.node_id)

        self._voted_for = None
        self._voted_ts = None
        self._voted_resource = None
        self._inquire_sent = False
        self._request_queue = []

        self._votes_held = set()
        self._votes_needed = len(self.quorum)
        self._request_ts = 0
        self._request_resource = None
        self._cs_event = threading.Event()
        self._in_cs = False
        self._lock = threading.Lock()
        self.logger = logging.getLogger(f"maekawa.{node.node_id}")

        self.node.register_handler(MSG_MAEKAWA_REQUEST, self._handle_request)
        self.node.register_handler(MSG_MAEKAWA_LOCKED, self._handle_locked)
        self.node.register_handler(MSG_MAEKAWA_FAILED, self._handle_failed)
        self.node.register_handler(MSG_MAEKAWA_INQUIRE, self._handle_inquire)
        self.node.register_handler(MSG_MAEKAWA_RELINQUISH, self._handle_relinquish)
        self.node.register_handler(MSG_MAEKAWA_RELEASE, self._handle_release)

    def request_cs(self, resource_id, timeout=10):
        with self._lock:
            self._request_ts = self.node.clock.increment()
            self._request_resource = resource_id
            self._votes_held = set()
            self._in_cs = False
            self._cs_event.clear()

        self.logger.info(
            f"Node {self.node.node_id} requesting CS for {resource_id}, "
            f"quorum={self.quorum}"
        )

        for nid in self.quorum:
            self.node.send(nid, MSG_MAEKAWA_REQUEST, {
                "resource_id": resource_id,
                "request_ts": self._request_ts,
            })

        acquired = self._cs_event.wait(timeout=timeout)
        if acquired:
            with self._lock:
                self._in_cs = True
            self.logger.info(f"Node {self.node.node_id} ENTERED CS for {resource_id}")
        return acquired

    def release_cs(self):
        with self._lock:
            resource_id = self._request_resource
            self._in_cs = False
            self._request_resource = None
            self._votes_held = set()

        self.logger.info(f"Node {self.node.node_id} RELEASED CS for {resource_id}")

        for nid in self.quorum:
            self.node.send(nid, MSG_MAEKAWA_RELEASE, {"resource_id": resource_id})

    def _grant_vote(self, requester, req_ts, resource_id):
        self._voted_for = requester
        self._voted_ts = req_ts
        self._voted_resource = resource_id
        self._inquire_sent = False
        self.node.send(requester, MSG_MAEKAWA_LOCKED, {
            "resource_id": resource_id,
            "voter": self.node.node_id,
        })

    def _handle_request(self, message):
        sender = message.sender_id
        req_ts = message.payload["request_ts"]
        resource_id = message.payload["resource_id"]

        with self._lock:
            if self._voted_for is None:
                self._grant_vote(sender, req_ts, resource_id)
            else:
                self._request_queue.append((req_ts, sender, resource_id))
                self._request_queue.sort(key=lambda x: (x[0], x[1]))

                if (not self._inquire_sent and
                        (req_ts < self._voted_ts or
                         (req_ts == self._voted_ts and sender < self._voted_for))):
                    self._inquire_sent = True
                    self.node.send(self._voted_for, MSG_MAEKAWA_INQUIRE, {
                        "resource_id": resource_id,
                        "voter": self.node.node_id,
                    })
                else:
                    self.node.send(sender, MSG_MAEKAWA_FAILED, {
                        "resource_id": resource_id,
                    })

    def _handle_locked(self, message):
        voter = message.payload.get("voter", message.sender_id)
        with self._lock:
            self._votes_held.add(voter)
            if len(self._votes_held) >= self._votes_needed:
                self._cs_event.set()

    def _handle_failed(self, message):
        pass

    def _handle_inquire(self, message):
        voter = message.payload.get("voter", message.sender_id)
        with self._lock:
            if self._in_cs:
                return
            if voter not in self._votes_held:
                return
            self._votes_held.discard(voter)
            self._cs_event.clear()

        self.node.send(voter, MSG_MAEKAWA_RELINQUISH, {
            "resource_id": message.payload["resource_id"],
        })

    def _handle_relinquish(self, message):
        with self._lock:
            old_voter = self._voted_for
            old_ts = self._voted_ts
            old_resource = self._voted_resource

            if old_voter is not None:
                self._request_queue.append((old_ts, old_voter, old_resource))
                self._request_queue.sort(key=lambda x: (x[0], x[1]))

            if self._request_queue:
                req_ts, requester, resource_id = self._request_queue.pop(0)
                self._grant_vote(requester, req_ts, resource_id)
            else:
                self._voted_for = None
                self._voted_ts = None
                self._voted_resource = None
                self._inquire_sent = False

    def _handle_release(self, message):
        with self._lock:
            self._voted_for = None
            self._voted_ts = None
            self._voted_resource = None
            self._inquire_sent = False

            if self._request_queue:
                req_ts, requester, resource_id = self._request_queue.pop(0)
                self._grant_vote(requester, req_ts, resource_id)
