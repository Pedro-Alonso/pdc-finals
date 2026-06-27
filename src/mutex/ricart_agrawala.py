import logging
import threading

from src.config import get_all_other_nodes
from src.network.message import MSG_MUTEX_REQUEST, MSG_MUTEX_REPLY


RELEASED = "RELEASED"
WANTED = "WANTED"
HELD = "HELD"


class RicartAgrawala:
    def __init__(self, node):
        self.node = node
        self._state = RELEASED
        self._request_ts = 0
        self._request_resource = None
        self._replies_received = 0
        self._replies_needed = 0
        self._deferred = []
        self._lock = threading.Lock()
        self._cs_event = threading.Event()
        self.logger = logging.getLogger(f"ricart_agrawala.{node.node_id}")

        self.node.register_handler(MSG_MUTEX_REQUEST, self._handle_request)
        self.node.register_handler(MSG_MUTEX_REPLY, self._handle_reply)

    def request_cs(self, resource_id, timeout=10):
        others = get_all_other_nodes(self.node.node_id)
        with self._lock:
            self._state = WANTED
            self._request_ts = self.node.clock.increment()
            self._request_resource = resource_id
            self._replies_received = 0
            self._replies_needed = len(others)
            self._cs_event.clear()

        self.logger.info(f"Node {self.node.node_id} requesting CS for {resource_id} at ts={self._request_ts}")

        for nid in others:
            self.node.send(nid, MSG_MUTEX_REQUEST, {
                "resource_id": resource_id,
                "request_ts": self._request_ts,
            })

        acquired = self._cs_event.wait(timeout=timeout)
        if acquired:
            with self._lock:
                self._state = HELD
            self.logger.info(f"Node {self.node.node_id} ENTERED CS for {resource_id}")
        return acquired

    def release_cs(self):
        with self._lock:
            resource_id = self._request_resource
            self._state = RELEASED
            self._request_resource = None
            deferred = list(self._deferred)
            self._deferred.clear()

        self.logger.info(f"Node {self.node.node_id} RELEASED CS for {resource_id}")

        for nid, res_id in deferred:
            self.node.send(nid, MSG_MUTEX_REPLY, {"resource_id": res_id})

    def _handle_request(self, message):
        sender = message.sender_id
        req_ts = message.payload["request_ts"]
        resource_id = message.payload["resource_id"]

        with self._lock:
            if self._state == RELEASED:
                send_reply = True
            elif self._state == HELD:
                send_reply = False
                self._deferred.append((sender, resource_id))
            else:
                if resource_id != self._request_resource:
                    send_reply = True
                elif (req_ts < self._request_ts or
                      (req_ts == self._request_ts and sender < self.node.node_id)):
                    send_reply = True
                else:
                    send_reply = False
                    self._deferred.append((sender, resource_id))

        if send_reply:
            self.node.send(sender, MSG_MUTEX_REPLY, {"resource_id": resource_id})

    def _handle_reply(self, message):
        with self._lock:
            self._replies_received += 1
            if self._replies_received >= self._replies_needed:
                self._cs_event.set()
