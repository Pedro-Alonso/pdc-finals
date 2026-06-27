import logging
import threading

from src.network.clock import VectorClock
from src.network.message import (
    MSG_FIFO_MSG, MSG_CAUSAL_MSG, MSG_TOTAL_MSG, MSG_TOTAL_SEQ_ASSIGN,
)


class FIFOOrdering:
    def __init__(self, node):
        self.node = node
        self._seq = 0
        self._expected_seq = {}
        self._buffer = {}
        self._lock = threading.Lock()
        self._on_deliver = None
        self.logger = logging.getLogger(f"fifo.{node.node_id}")

        self.node.register_handler(MSG_FIFO_MSG, self._handle_fifo)

    def set_deliver_callback(self, callback):
        self._on_deliver = callback

    def send(self, target_id, data):
        with self._lock:
            self._seq += 1
            seq = self._seq
        payload = {"seq": seq, "data": data}
        self.node.send(target_id, MSG_FIFO_MSG, payload)

    def broadcast(self, data):
        with self._lock:
            self._seq += 1
            seq = self._seq
        payload = {"seq": seq, "data": data}
        self.node.broadcast(MSG_FIFO_MSG, payload)

    def _handle_fifo(self, message):
        sender = message.sender_id
        seq = message.payload["seq"]
        data = message.payload["data"]

        with self._lock:
            if sender not in self._expected_seq:
                self._expected_seq[sender] = 1
                self._buffer[sender] = []

            if seq == self._expected_seq[sender]:
                self._deliver(sender, seq, data)
                self._expected_seq[sender] += 1
                self._flush_buffer(sender)
            else:
                self._buffer[sender].append((seq, data))
                self._buffer[sender].sort(key=lambda x: x[0])

    def _flush_buffer(self, sender):
        while self._buffer[sender]:
            seq, data = self._buffer[sender][0]
            if seq == self._expected_seq[sender]:
                self._buffer[sender].pop(0)
                self._deliver(sender, seq, data)
                self._expected_seq[sender] += 1
            else:
                break

    def _deliver(self, sender, seq, data):
        self.logger.info(f"Deliver FIFO msg from node {sender}, seq={seq}: {data}")
        if self._on_deliver:
            self._on_deliver(sender, seq, data)


class CausalOrdering:
    def __init__(self, node, node_ids):
        self.node = node
        self.vc = VectorClock(node.node_id, node_ids)
        self._buffer = []
        self._lock = threading.Lock()
        self._on_deliver = None
        self.logger = logging.getLogger(f"causal.{node.node_id}")

        self.node.register_handler(MSG_CAUSAL_MSG, self._handle_causal)

    def set_deliver_callback(self, callback):
        self._on_deliver = callback

    def send(self, target_id, data):
        vector = self.vc.increment()
        payload = {"vector_clock": vector, "data": data}
        self.node.send(target_id, MSG_CAUSAL_MSG, payload)

    def broadcast(self, data):
        vector = self.vc.increment()
        payload = {"vector_clock": vector, "data": data}
        self.node.broadcast(MSG_CAUSAL_MSG, payload)

    def _handle_causal(self, message):
        sender = message.sender_id
        msg_vc = {int(k): v for k, v in message.payload["vector_clock"].items()}
        data = message.payload["data"]

        with self._lock:
            if self._can_deliver(sender, msg_vc):
                self._deliver(sender, msg_vc, data)
                self._flush_buffer()
            else:
                self._buffer.append((sender, msg_vc, data))

    def _can_deliver(self, sender, msg_vc):
        local_vc = self.vc.get_vector()
        for nid, ts in msg_vc.items():
            nid = int(nid)
            if nid == sender:
                if ts != local_vc.get(nid, 0) + 1:
                    return False
            else:
                if ts > local_vc.get(nid, 0):
                    return False
        return True

    def _flush_buffer(self):
        changed = True
        while changed:
            changed = False
            remaining = []
            for sender, msg_vc, data in self._buffer:
                if self._can_deliver(sender, msg_vc):
                    self._deliver(sender, msg_vc, data)
                    changed = True
                else:
                    remaining.append((sender, msg_vc, data))
            self._buffer = remaining

    def _deliver(self, sender, msg_vc, data):
        self.vc.merge(msg_vc)
        self.logger.info(f"Deliver CAUSAL msg from node {sender}: {data}")
        if self._on_deliver:
            self._on_deliver(sender, data)


class TotalOrdering:
    def __init__(self, node, sequencer_id):
        self.node = node
        self.sequencer_id = sequencer_id
        self._global_seq = 0
        self._expected_seq = 1
        self._buffer = {}
        self._lock = threading.Lock()
        self._on_deliver = None
        self.logger = logging.getLogger(f"total.{node.node_id}")

        self.node.register_handler(MSG_TOTAL_MSG, self._handle_total_msg)
        self.node.register_handler(MSG_TOTAL_SEQ_ASSIGN, self._handle_seq_assign)

    def set_deliver_callback(self, callback):
        self._on_deliver = callback

    def send(self, data):
        payload = {"data": data, "origin": self.node.node_id}
        self.node.send(self.sequencer_id, MSG_TOTAL_MSG, payload)

    def _handle_total_msg(self, message):
        if self.node.node_id != self.sequencer_id:
            return

        with self._lock:
            self._global_seq += 1
            seq = self._global_seq

        payload = {
            "global_seq": seq,
            "data": message.payload["data"],
            "origin": message.payload["origin"],
        }
        self.node.broadcast(MSG_TOTAL_SEQ_ASSIGN, payload)
        self._handle_seq_assign_internal(payload)

    def _handle_seq_assign(self, message):
        self._handle_seq_assign_internal(message.payload)

    def _handle_seq_assign_internal(self, payload):
        seq = payload["global_seq"]
        data = payload["data"]
        origin = payload["origin"]

        with self._lock:
            if seq == self._expected_seq:
                self._deliver(origin, seq, data)
                self._expected_seq += 1
                self._flush_buffer()
            else:
                self._buffer[seq] = (origin, data)

    def _flush_buffer(self):
        while self._expected_seq in self._buffer:
            origin, data = self._buffer.pop(self._expected_seq)
            self._deliver(origin, self._expected_seq, data)
            self._expected_seq += 1

    def _deliver(self, origin, seq, data):
        self.logger.info(f"Deliver TOTAL msg seq={seq} from node {origin}: {data}")
        if self._on_deliver:
            self._on_deliver(origin, seq, data)
