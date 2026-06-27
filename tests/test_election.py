import time
import threading

import pytest

from src.node import Node


def start_nodes(nodes):
    for n in nodes:
        n.start()
    time.sleep(1)


def stop_nodes(nodes):
    for n in nodes:
        n.stop()
    time.sleep(0.5)


def wait_election(nodes, timeout=10):
    for n in nodes:
        if n._election:
            n._election.election_complete.wait(timeout=timeout)


class TestChangRoberts:
    def test_elects_highest_id(self):
        nodes = [Node(i, election_algorithm="chang_roberts") for i in range(4)]
        start_nodes(nodes)
        try:
            nodes[0].start_election()
            wait_election(nodes)
            time.sleep(0.5)
            for n in nodes:
                assert n.leader_id == 3
        finally:
            stop_nodes(nodes)

    def test_any_node_can_start(self):
        nodes = [Node(i, election_algorithm="chang_roberts") for i in range(4)]
        start_nodes(nodes)
        try:
            nodes[2].start_election()
            wait_election(nodes)
            time.sleep(0.5)
            for n in nodes:
                assert n.leader_id == 3
        finally:
            stop_nodes(nodes)

    def test_handles_dead_successor(self):
        nodes = [Node(i, election_algorithm="chang_roberts") for i in range(4)]
        start_nodes(nodes)
        try:
            nodes[1].stop()
            time.sleep(0.5)

            nodes[0].start_election()
            alive = [n for n in nodes if n.node_id != 1]
            wait_election(alive)
            time.sleep(0.5)

            for n in alive:
                assert n.leader_id == 3
        finally:
            stop_nodes([n for n in nodes if n.node_id != 1])


class TestBully:
    def test_elects_highest_id(self):
        nodes = [Node(i, election_algorithm="bully") for i in range(4)]
        start_nodes(nodes)
        try:
            nodes[0].start_election()
            wait_election(nodes)
            time.sleep(0.5)
            for n in nodes:
                assert n.leader_id == 3
        finally:
            stop_nodes(nodes)

    def test_timeout_self_elect(self):
        nodes = [Node(i, election_algorithm="bully") for i in range(4)]
        start_nodes(nodes[:3])
        try:
            nodes[2].start_election()
            wait_election(nodes[:3], timeout=5)
            time.sleep(0.5)
            for n in nodes[:3]:
                assert n.leader_id == 2
        finally:
            stop_nodes(nodes[:3])

    def test_leader_failure_triggers_reelection(self):
        nodes = [Node(i, election_algorithm="bully") for i in range(4)]
        start_nodes(nodes)
        try:
            nodes[0].start_election()
            wait_election(nodes)
            time.sleep(0.5)
            assert nodes[0].leader_id == 3

            nodes[3].stop()
            time.sleep(0.5)

            for n in nodes[:3]:
                if n._election:
                    n._election.election_complete.clear()

            nodes[1].start_election()
            wait_election(nodes[:3], timeout=5)
            time.sleep(0.5)

            for n in nodes[:3]:
                assert n.leader_id == 2
        finally:
            stop_nodes([n for n in nodes if n.node_id != 3])
