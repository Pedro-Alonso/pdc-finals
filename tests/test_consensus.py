import time
import threading

import pytest

from src.node import Node


def start_nodes(nodes):
    for n in nodes:
        n.start()
    time.sleep(1.5)


def stop_nodes(nodes):
    for n in nodes:
        try:
            n.stop()
        except Exception:
            pass
    time.sleep(0.5)


def elect_leader(nodes, timeout=10):
    nodes[0].start_election()
    for n in nodes:
        if n._election:
            n._election.election_complete.wait(timeout=timeout)
    time.sleep(0.5)


def make_nodes(count=4):
    return [
        Node(i, election_algorithm="bully", enable_failure_detector=True)
        for i in range(count)
    ]


class TestFailureDetector:
    def test_detects_dead_node(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            time.sleep(2)
            for n in nodes:
                assert n.failure_detector.is_alive(
                    (n.node_id + 1) % 4
                )

            nodes[1].stop()
            time.sleep(5)

            for n in [nodes[0], nodes[2], nodes[3]]:
                assert not n.failure_detector.is_alive(1)
                assert 1 in n.failure_detector.suspected
        finally:
            stop_nodes([n for n in nodes if n.node_id != 1])

    def test_recovery(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            nodes[1].stop()
            time.sleep(5)

            assert not nodes[0].failure_detector.is_alive(1)

            nodes[1] = Node(1, election_algorithm="bully", enable_failure_detector=True)
            nodes[1].start()
            time.sleep(4)

            assert nodes[0].failure_detector.is_alive(1)
            assert 1 not in nodes[0].failure_detector.suspected
        finally:
            stop_nodes(nodes)


class TestConsensus:
    def test_all_accept(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            elect_leader(nodes)
            leader = next(n for n in nodes if n.is_leader())

            result = leader.consensus.propose("VALUE_A")
            time.sleep(0.5)

            assert result == "VALUE_A"
            for n in nodes:
                assert n.consensus.get_decision(1) == "VALUE_A"
        finally:
            stop_nodes(nodes)

    def test_majority_accept(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            elect_leader(nodes)

            leader = next(n for n in nodes if n.is_leader())
            victim = next(n for n in nodes if not n.is_leader())
            victim.stop()

            deadline = time.time() + 15
            while leader.failure_detector.is_alive(victim.node_id):
                assert time.time() < deadline, "Failure detector did not detect dead node"
                time.sleep(0.5)

            result = leader.consensus.propose("VALUE_B")
            time.sleep(0.5)

            assert result == "VALUE_B"
        finally:
            stop_nodes([n for n in nodes if n._running])

    def test_leader_required(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            elect_leader(nodes)
            follower = next(n for n in nodes if not n.is_leader())

            result = follower.consensus.propose("VALUE_C", timeout=2)
            assert result is None
        finally:
            stop_nodes(nodes)

    def test_leader_failure_triggers_reelection(self):
        nodes = make_nodes(4)
        start_nodes(nodes)
        try:
            elect_leader(nodes)
            leader_id = nodes[0].leader_id
            assert leader_id == 3

            alive = [n for n in nodes if n.node_id != 3]
            for n in alive:
                if n._election:
                    n._election.election_complete.clear()

            nodes[3].stop()

            for n in alive:
                if n._election:
                    n._election.election_complete.wait(timeout=15)
            time.sleep(1)

            for n in alive:
                assert n.leader_id != 3
                assert n.leader_id == 2
        finally:
            stop_nodes([n for n in nodes if n.node_id != 3])
