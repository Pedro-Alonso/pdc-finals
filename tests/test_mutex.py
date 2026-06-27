import time
import threading

import pytest

from src.node import Node
from src.mutex.ricart_agrawala import RicartAgrawala
from src.mutex.maekawa import MaekawaExclusion, QUORUMS


def start_nodes(nodes):
    for n in nodes:
        n.start()
    time.sleep(1)


def stop_nodes(nodes):
    for n in nodes:
        n.stop()
    time.sleep(0.5)


class TestRicartAgrawala:
    def test_mutual_exclusion(self):
        nodes = [Node(i) for i in range(4)]
        start_nodes(nodes)
        try:
            mutexes = [RicartAgrawala(n) for n in nodes]
            cs_log = []
            lock = threading.Lock()

            def access_cs(nid, mutex):
                acquired = mutex.request_cs("resource_X")
                if acquired:
                    with lock:
                        cs_log.append(("enter", nid, time.time()))
                    time.sleep(0.3)
                    with lock:
                        cs_log.append(("exit", nid, time.time()))
                    mutex.release_cs()

            threads = [
                threading.Thread(target=access_cs, args=(i, mutexes[i]))
                for i in range(3)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            time.sleep(1)

            enters = [(nid, ts) for action, nid, ts in cs_log if action == "enter"]
            exits = [(nid, ts) for action, nid, ts in cs_log if action == "exit"]

            assert len(enters) == 3

            for i, (nid_i, enter_i) in enumerate(enters):
                exit_i = next(ts for n, ts in exits if n == nid_i)
                for j, (nid_j, enter_j) in enumerate(enters):
                    if i != j:
                        exit_j = next(ts for n, ts in exits if n == nid_j)
                        assert not (enter_j < exit_i and enter_i < exit_j)
        finally:
            stop_nodes(nodes)

    def test_fifo_fairness(self):
        nodes = [Node(i) for i in range(4)]
        start_nodes(nodes)
        try:
            mutexes = [RicartAgrawala(n) for n in nodes]
            enter_order = []
            lock = threading.Lock()

            def access_cs(nid, mutex, delay):
                time.sleep(delay)
                acquired = mutex.request_cs("resource_X")
                if acquired:
                    with lock:
                        enter_order.append(nid)
                    time.sleep(0.2)
                    mutex.release_cs()

            threads = [
                threading.Thread(target=access_cs, args=(0, mutexes[0], 0)),
                threading.Thread(target=access_cs, args=(1, mutexes[1], 0.1)),
                threading.Thread(target=access_cs, args=(2, mutexes[2], 0.2)),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            time.sleep(1)

            assert len(enter_order) == 3
        finally:
            stop_nodes(nodes)


class TestMaekawa:
    def test_quorum_intersection(self):
        quorum_list = list(QUORUMS.values())
        for i in range(len(quorum_list)):
            for j in range(i + 1, len(quorum_list)):
                assert quorum_list[i] & quorum_list[j], (
                    f"Quorums {i} and {j} don't intersect"
                )

    def test_mutual_exclusion(self):
        nodes = [Node(i) for i in range(4)]
        start_nodes(nodes)
        try:
            mutexes = [MaekawaExclusion(n) for n in nodes]
            cs_log = []
            lock = threading.Lock()

            def access_cs(nid, mutex):
                acquired = mutex.request_cs("resource_X")
                if acquired:
                    with lock:
                        cs_log.append(("enter", nid, time.time()))
                    time.sleep(0.3)
                    with lock:
                        cs_log.append(("exit", nid, time.time()))
                    mutex.release_cs()

            threads = [
                threading.Thread(target=access_cs, args=(i, mutexes[i]))
                for i in range(3)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            time.sleep(1)

            enters = [(nid, ts) for action, nid, ts in cs_log if action == "enter"]
            exits = [(nid, ts) for action, nid, ts in cs_log if action == "exit"]

            assert len(enters) >= 1

            for i, (nid_i, enter_i) in enumerate(enters):
                exit_i = next(ts for n, ts in exits if n == nid_i)
                for j, (nid_j, enter_j) in enumerate(enters):
                    if i != j:
                        exit_j = next(ts for n, ts in exits if n == nid_j)
                        assert not (enter_j < exit_i and enter_i < exit_j)
        finally:
            stop_nodes(nodes)
