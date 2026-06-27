import time
import threading

from src.node import Node


def wait_for_election(nodes, timeout=10):
    for n in nodes:
        if n._election:
            n._election.election_complete.wait(timeout=timeout)


def print_leaders(nodes):
    for n in nodes:
        role = "LEADER" if n.is_leader() else "follower"
        print(f"  Node {n.node_id}: leader={n.leader_id} ({role})")


def create_nodes(count, algorithm="bully", enable_fd=True):
    return [
        Node(i, election_algorithm=algorithm, enable_failure_detector=enable_fd)
        for i in range(count)
    ]


def start_all(nodes):
    for n in nodes:
        n.start()
    time.sleep(1.5)


def stop_all(nodes):
    for n in nodes:
        try:
            n.stop()
        except Exception:
            pass
    time.sleep(0.5)


def scenario_consensus_all_alive():
    print("\n" + "=" * 60)
    print("SCENARIO 1: Consensus with all nodes alive")
    print("=" * 60)

    nodes = create_nodes(4)
    start_all(nodes)

    print("\nElecting leader...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)
    print_leaders(nodes)

    leader = next(n for n in nodes if n.is_leader())
    print(f"\nLeader (node {leader.node_id}) proposes 'OPERACAO_X'...")
    result = leader.consensus.propose("OPERACAO_X")
    time.sleep(0.5)
    print(f"Consensus result: {result}")

    for n in nodes:
        if n.consensus:
            decision = n.consensus.get_decision(1)
            print(f"  Node {n.node_id} decision: {decision}")

    stop_all(nodes)


def scenario_consensus_one_dead():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Consensus with 1 node dead")
    print("=" * 60)

    nodes = create_nodes(4)
    start_all(nodes)

    print("\nElecting leader...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)
    print_leaders(nodes)

    victim = next(n for n in nodes if not n.is_leader())
    print(f"\nKilling node {victim.node_id}...")
    victim.stop()
    time.sleep(4)

    leader = next(n for n in nodes if n.is_leader())
    print(f"\nLeader (node {leader.node_id}) proposes 'OPERACAO_Y'...")
    result = leader.consensus.propose("OPERACAO_Y")
    time.sleep(0.5)
    print(f"Consensus result: {result}")

    alive = [n for n in nodes if n.node_id != victim.node_id]
    for n in alive:
        if n.consensus:
            decision = n.consensus.get_decision(1)
            print(f"  Node {n.node_id} decision: {decision}")

    stop_all(alive)


def scenario_leader_failure():
    print("\n" + "=" * 60)
    print("SCENARIO 3: Leader failure triggers re-election + new consensus")
    print("=" * 60)

    nodes = create_nodes(4)
    start_all(nodes)

    print("\nElecting leader...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)
    print_leaders(nodes)

    leader = next(n for n in nodes if n.is_leader())
    print(f"\nKilling leader (node {leader.node_id})...")
    leader.stop()

    print("Waiting for failure detection and re-election...")
    time.sleep(5)

    alive = [n for n in nodes if n.node_id != leader.node_id]
    print("\nAfter re-election:")
    print_leaders(alive)

    new_leader = next((n for n in alive if n.is_leader()), None)
    if new_leader:
        print(f"\nNew leader (node {new_leader.node_id}) proposes 'OPERACAO_Z'...")
        result = new_leader.consensus.propose("OPERACAO_Z")
        time.sleep(0.5)
        print(f"Consensus result: {result}")

    stop_all(alive)


def scenario_heartbeat_demo():
    print("\n" + "=" * 60)
    print("SCENARIO 4: Heartbeat detection — failure and recovery")
    print("=" * 60)

    nodes = create_nodes(4)
    start_all(nodes)

    print("\nAll nodes alive. Heartbeats flowing...")
    time.sleep(3)

    for n in nodes:
        if n.failure_detector:
            alive = n.failure_detector.get_alive_nodes()
            suspected = n.failure_detector.suspected
            print(f"  Node {n.node_id}: alive={sorted(alive)}, suspected={sorted(suspected)}")

    print(f"\nKilling node 1...")
    nodes[1].stop()

    print("Waiting for failure detection...")
    time.sleep(5)

    for n in [nodes[0], nodes[2], nodes[3]]:
        if n.failure_detector:
            alive = n.failure_detector.get_alive_nodes()
            suspected = n.failure_detector.suspected
            print(f"  Node {n.node_id}: alive={sorted(alive)}, suspected={sorted(suspected)}")

    print(f"\nRestarting node 1...")
    nodes[1] = Node(1, election_algorithm="bully", enable_failure_detector=True)
    nodes[1].start()

    print("Waiting for recovery detection...")
    time.sleep(4)

    for n in [nodes[0], nodes[2], nodes[3]]:
        if n.failure_detector:
            alive = n.failure_detector.get_alive_nodes()
            suspected = n.failure_detector.suspected
            print(f"  Node {n.node_id}: alive={sorted(alive)}, suspected={sorted(suspected)}")

    stop_all(nodes)


def main():
    print("=" * 60)
    print("  CONSENSUS & FAULT TOLERANCE DEMO")
    print("  Leader-based consensus + Heartbeat failure detection")
    print("=" * 60)

    scenario_consensus_all_alive()
    scenario_consensus_one_dead()
    scenario_leader_failure()
    scenario_heartbeat_demo()

    print("\n" + "=" * 60)
    print("  ALL SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
