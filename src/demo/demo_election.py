import time
import threading

from src.config import NODES
from src.node import Node


def wait_for_election(nodes, timeout=10):
    for n in nodes:
        if n._election:
            n._election.election_complete.wait(timeout=timeout)


def print_leaders(nodes):
    for n in nodes:
        role = "LEADER" if n.is_leader() else "follower"
        print(f"  Node {n.node_id}: leader={n.leader_id} ({role})")


def scenario_chang_roberts_all_alive():
    print("\n" + "=" * 60)
    print("SCENARIO 1: Chang-Roberts, all nodes alive")
    print("=" * 60)

    nodes = []
    for nid in range(4):
        n = Node(nid, election_algorithm="chang_roberts")
        nodes.append(n)

    for n in nodes:
        n.start()
    time.sleep(1)

    print("\nNode 0 starts election...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)

    print("\nResults:")
    print_leaders(nodes)

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def scenario_bully_all_alive():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Bully, all nodes alive")
    print("=" * 60)

    nodes = []
    for nid in range(4):
        n = Node(nid, election_algorithm="bully")
        nodes.append(n)

    for n in nodes:
        n.start()
    time.sleep(1)

    print("\nNode 0 starts election...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)

    print("\nResults:")
    print_leaders(nodes)

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def scenario_leader_failure():
    print("\n" + "=" * 60)
    print("SCENARIO 3: Leader failure and re-election (Bully)")
    print("=" * 60)

    nodes = []
    for nid in range(4):
        n = Node(nid, election_algorithm="bully")
        nodes.append(n)

    for n in nodes:
        n.start()
    time.sleep(1)

    print("\nNode 0 starts election...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)

    print("\nResults after first election:")
    print_leaders(nodes)

    print(f"\nKilling leader node 3...")
    nodes[3].stop()
    time.sleep(1)

    for n in nodes[:3]:
        if n._election:
            n._election.election_complete.clear()

    print("Node 2 detects failure, starts re-election...")
    nodes[2].start_election()
    wait_for_election(nodes[:3])
    time.sleep(1)

    print("\nResults after re-election:")
    print_leaders(nodes[:3])

    for n in nodes[:3]:
        n.stop()
    time.sleep(0.5)


def scenario_new_node_joins():
    print("\n" + "=" * 60)
    print("SCENARIO 4: New high-priority node joins (Bully)")
    print("=" * 60)

    nodes = []
    for nid in range(3):
        n = Node(nid, election_algorithm="bully")
        nodes.append(n)

    for n in nodes:
        n.start()
    time.sleep(1)

    print("\nNode 0 starts election among nodes 0-2...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)

    print("\nResults (nodes 0-2):")
    print_leaders(nodes)

    print("\nNode 3 joins and starts election...")
    node3 = Node(3, election_algorithm="bully")
    nodes.append(node3)
    node3.start()
    time.sleep(1)

    for n in nodes:
        if n._election:
            n._election.election_complete.clear()

    node3.start_election()
    wait_for_election(nodes)
    time.sleep(1)

    print("\nResults after node 3 joins:")
    print_leaders(nodes)

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def main():
    print("=" * 60)
    print("  LEADER ELECTION DEMO")
    print("  Chang-Roberts (ring) + Bully (priority)")
    print("=" * 60)

    scenario_chang_roberts_all_alive()
    scenario_bully_all_alive()
    scenario_leader_failure()
    scenario_new_node_joins()

    print("\n" + "=" * 60)
    print("  ALL SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
