import time
import threading

from src.node import Node
from src.mutex.ricart_agrawala import RicartAgrawala
from src.mutex.maekawa import MaekawaExclusion


def scenario_ricart_agrawala():
    print("\n" + "=" * 60)
    print("SCENARIO 1: Ricart-Agrawala Mutual Exclusion")
    print("=" * 60)

    nodes = [Node(i) for i in range(4)]
    for n in nodes:
        n.start()
    time.sleep(1)

    mutexes = [RicartAgrawala(n) for n in nodes]

    cs_log = []
    cs_lock = threading.Lock()
    message_count = {"requests": 0, "replies": 0}

    def access_cs(node_id, mutex):
        print(f"  Node {node_id} requesting CS...")
        acquired = mutex.request_cs("resource_X")
        if acquired:
            with cs_lock:
                cs_log.append(("enter", node_id, time.time()))
            print(f"  Node {node_id} IN critical section")
            time.sleep(0.5)
            with cs_lock:
                cs_log.append(("exit", node_id, time.time()))
            mutex.release_cs()
            print(f"  Node {node_id} LEFT critical section")

    print("\nNodes 0, 1, 2 all request CS simultaneously...")
    threads = []
    for i in range(3):
        t = threading.Thread(target=access_cs, args=(i, mutexes[i]))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    time.sleep(1)

    print("\nCS access log:")
    for action, nid, ts in cs_log:
        print(f"  {action}: node {nid}")

    exclusive = True
    enters = [(nid, ts) for action, nid, ts in cs_log if action == "enter"]
    exits = [(nid, ts) for action, nid, ts in cs_log if action == "exit"]
    for i, (nid_i, enter_i) in enumerate(enters):
        exit_i = next(ts for n, ts in exits if n == nid_i)
        for j, (nid_j, enter_j) in enumerate(enters):
            if i != j:
                exit_j = next(ts for n, ts in exits if n == nid_j)
                if enter_j < exit_i and enter_i < exit_j:
                    exclusive = False

    print(f"\nMutual exclusion preserved: {exclusive}")
    print(f"Ricart-Agrawala messages: 2*(N-1) = {2 * 2} per CS entry (3 nodes requesting)")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def scenario_maekawa():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Maekawa Quorum-Based Mutual Exclusion")
    print("=" * 60)

    nodes = [Node(i) for i in range(4)]
    for n in nodes:
        n.start()
    time.sleep(1)

    mutexes = [MaekawaExclusion(n) for n in nodes]

    print("\nQuorum assignments:")
    from src.mutex.maekawa import QUORUMS
    for nid, q in QUORUMS.items():
        print(f"  Node {nid}: quorum = {q}")

    cs_log = []
    cs_lock = threading.Lock()

    def access_cs(node_id, mutex):
        print(f"  Node {node_id} requesting CS...")
        acquired = mutex.request_cs("resource_X")
        if acquired:
            with cs_lock:
                cs_log.append(("enter", node_id, time.time()))
            print(f"  Node {node_id} IN critical section")
            time.sleep(0.5)
            with cs_lock:
                cs_log.append(("exit", node_id, time.time()))
            mutex.release_cs()
            print(f"  Node {node_id} LEFT critical section")

    print("\nNodes 0, 1, 2 all request CS simultaneously...")
    threads = []
    for i in range(3):
        t = threading.Thread(target=access_cs, args=(i, mutexes[i]))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    time.sleep(1)

    print("\nCS access log:")
    for action, nid, ts in cs_log:
        print(f"  {action}: node {nid}")

    exclusive = True
    enters = [(nid, ts) for action, nid, ts in cs_log if action == "enter"]
    exits = [(nid, ts) for action, nid, ts in cs_log if action == "exit"]
    for i, (nid_i, enter_i) in enumerate(enters):
        exit_i = next(ts for n, ts in exits if n == nid_i)
        for j, (nid_j, enter_j) in enumerate(enters):
            if i != j:
                exit_j = next(ts for n, ts in exits if n == nid_j)
                if enter_j < exit_i and enter_i < exit_j:
                    exclusive = False

    print(f"\nMutual exclusion preserved: {exclusive}")
    print(f"Maekawa messages: 3*sqrt(N) per CS entry (quorum size ~2 for 4 nodes)")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def main():
    print("=" * 60)
    print("  MUTUAL EXCLUSION DEMO")
    print("  Ricart-Agrawala + Maekawa")
    print("=" * 60)

    scenario_ricart_agrawala()
    scenario_maekawa()

    print("\n" + "=" * 60)
    print("  ALL MUTEX SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
