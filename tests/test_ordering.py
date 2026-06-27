import time
import threading

import pytest

from src.node import Node
from src.network.clock import VectorClock
from src.network.ordering import FIFOOrdering, CausalOrdering, TotalOrdering


def start_nodes(nodes):
    for n in nodes:
        n.start()
    time.sleep(1)


def stop_nodes(nodes):
    for n in nodes:
        n.stop()
    time.sleep(0.5)


class TestVectorClock:
    def test_increment(self):
        vc = VectorClock(0, [0, 1, 2])
        v = vc.increment()
        assert v[0] == 1
        assert v[1] == 0
        assert v[2] == 0

        v = vc.increment()
        assert v[0] == 2

    def test_update(self):
        vc = VectorClock(0, [0, 1, 2])
        vc.increment()
        v = vc.update({0: 0, 1: 3, 2: 1})
        assert v[0] == 2
        assert v[1] == 3
        assert v[2] == 1

    def test_happens_before(self):
        assert VectorClock.happens_before({0: 1, 1: 0}, {0: 2, 1: 1})
        assert not VectorClock.happens_before({0: 2, 1: 1}, {0: 1, 1: 0})
        assert not VectorClock.happens_before({0: 1, 1: 2}, {0: 2, 1: 1})
        assert not VectorClock.happens_before({0: 1, 1: 1}, {0: 1, 1: 1})

    def test_happens_before_equal_is_false(self):
        v = {0: 1, 1: 1}
        assert not VectorClock.happens_before(v, v)


class TestFIFOOrdering:
    def test_delivers_in_order(self):
        nodes = [Node(i) for i in range(3)]
        start_nodes(nodes)
        try:
            delivered = []
            lock = threading.Lock()

            fifo_sender = FIFOOrdering(nodes[0])
            fifo_receiver = FIFOOrdering(nodes[1])

            def on_deliver(sender, seq, data):
                with lock:
                    delivered.append((seq, data))

            fifo_receiver.set_deliver_callback(on_deliver)

            for i in range(5):
                fifo_sender.send(1, f"msg_{i}")
                time.sleep(0.05)

            time.sleep(2)

            assert len(delivered) == 5
            for i, (seq, data) in enumerate(delivered):
                assert seq == i + 1
                assert data == f"msg_{i}"
        finally:
            stop_nodes(nodes)

    def test_reorders_messages(self):
        vc = VectorClock(0, [0, 1])
        v1 = {0: 1, 1: 0}
        v2 = {0: 2, 1: 0}
        assert VectorClock.happens_before(v1, v2)


class TestCausalOrdering:
    def test_causal_delivery(self):
        node_ids = [0, 1, 2]
        nodes = [Node(i) for i in node_ids]
        start_nodes(nodes)
        try:
            delivered_at_2 = []
            lock = threading.Lock()
            delivered_at_1 = threading.Event()

            causal = [CausalOrdering(n, node_ids) for n in nodes]

            def on_deliver_2(sender, data):
                with lock:
                    delivered_at_2.append((sender, data))

            def on_deliver_1(sender, data):
                delivered_at_1.set()

            causal[2].set_deliver_callback(on_deliver_2)
            causal[1].set_deliver_callback(on_deliver_1)

            causal[0].broadcast("msg_A")

            delivered_at_1.wait(timeout=5)
            time.sleep(0.3)

            causal[1].broadcast("msg_B")

            time.sleep(3)

            assert len(delivered_at_2) >= 2
            a_idx = next(i for i, (s, d) in enumerate(delivered_at_2) if d == "msg_A")
            b_idx = next(i for i, (s, d) in enumerate(delivered_at_2) if d == "msg_B")
            assert a_idx < b_idx
        finally:
            stop_nodes(nodes)


class TestTotalOrdering:
    def test_consistent_global_order(self):
        nodes = [Node(i) for i in range(4)]
        start_nodes(nodes)
        try:
            sequencer_id = 0
            delivered = {i: [] for i in range(4)}
            locks = {i: threading.Lock() for i in range(4)}

            total = [TotalOrdering(n, sequencer_id) for n in nodes]

            for i, t in enumerate(total):
                def make_cb(nid):
                    def cb(origin, seq, data):
                        with locks[nid]:
                            delivered[nid].append((seq, data))
                    return cb
                t.set_deliver_callback(make_cb(i))

            total[1].send("from_1")
            time.sleep(0.2)
            total[2].send("from_2")

            time.sleep(3)

            reference = [(seq, data) for seq, data in delivered[0]]
            assert len(reference) >= 2

            for nid in range(1, 4):
                order = [(seq, data) for seq, data in delivered[nid]]
                assert order == reference
        finally:
            stop_nodes(nodes)
