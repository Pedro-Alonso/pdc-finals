import time
import threading

from src.config import NODES
from src.node import Node
from src.network.ordering import FIFOOrdering, CausalOrdering, TotalOrdering


def scenario_fifo():
    print("\n" + "=" * 60)
    print("SCENARIO 1: FIFO Ordering")
    print("=" * 60)

    nodes = [Node(i) for i in range(3)]
    for n in nodes:
        n.start()
    time.sleep(1)

    delivered = []
    lock = threading.Lock()

    fifo_sender = FIFOOrdering(nodes[0])
    fifo_receiver = FIFOOrdering(nodes[1])

    def on_deliver(sender, seq, data):
        with lock:
            delivered.append((sender, seq, data))
            print(f"  Node 1 delivered: seq={seq} data={data}")

    fifo_receiver.set_deliver_callback(on_deliver)

    print("\nNode 0 sends 5 messages to Node 1...")
    for i in range(1, 6):
        fifo_sender.send(1, f"msg_{i}")
        time.sleep(0.1)

    time.sleep(2)

    print(f"\nDelivered {len(delivered)} messages in order:")
    for sender, seq, data in delivered:
        print(f"  from={sender} seq={seq} data={data}")

    in_order = all(delivered[i][1] <= delivered[i + 1][1] for i in range(len(delivered) - 1))
    print(f"\nFIFO order preserved: {in_order}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def scenario_causal():
    print("\n" + "=" * 60)
    print("SCENARIO 2: Causal Ordering")
    print("=" * 60)

    node_ids = [0, 1, 2]
    nodes = [Node(i) for i in node_ids]
    for n in nodes:
        n.start()
    time.sleep(1)

    delivered_at_2 = []
    lock = threading.Lock()

    causal = [CausalOrdering(n, node_ids) for n in nodes]

    def on_deliver_2(sender, data):
        with lock:
            delivered_at_2.append((sender, data))
            print(f"  Node 2 delivered: from={sender} data={data}")

    causal[2].set_deliver_callback(on_deliver_2)

    delivered_at_1 = threading.Event()

    def on_deliver_1(sender, data):
        print(f"  Node 1 delivered: from={sender} data={data}")
        delivered_at_1.set()

    causal[1].set_deliver_callback(on_deliver_1)

    print("\nNode 0 broadcasts msg_A...")
    causal[0].broadcast("msg_A")

    delivered_at_1.wait(timeout=5)
    time.sleep(0.5)

    print("Node 1 (after receiving A) broadcasts msg_B...")
    causal[1].broadcast("msg_B")

    time.sleep(2)

    print(f"\nNode 2 delivery order:")
    for sender, data in delivered_at_2:
        print(f"  from={sender} data={data}")

    if len(delivered_at_2) >= 2:
        a_idx = next((i for i, (s, d) in enumerate(delivered_at_2) if d == "msg_A"), -1)
        b_idx = next((i for i, (s, d) in enumerate(delivered_at_2) if d == "msg_B"), -1)
        if a_idx >= 0 and b_idx >= 0:
            print(f"\nCausal order preserved (A before B): {a_idx < b_idx}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def scenario_total():
    print("\n" + "=" * 60)
    print("SCENARIO 3: Total Ordering (via sequencer)")
    print("=" * 60)

    nodes = [Node(i) for i in range(4)]
    for n in nodes:
        n.start()
    time.sleep(1)

    sequencer_id = 0
    delivered = {i: [] for i in range(4)}
    locks = {i: threading.Lock() for i in range(4)}

    total = []
    for n in nodes:
        t = TotalOrdering(n, sequencer_id)
        total.append(t)

    for i, t in enumerate(total):
        def make_callback(node_id):
            def on_deliver(origin, seq, data):
                with locks[node_id]:
                    delivered[node_id].append((seq, origin, data))
            return on_deliver
        t.set_deliver_callback(make_callback(i))

    print("\nNode 1 and Node 2 send messages concurrently...")
    t1 = threading.Thread(target=lambda: total[1].send("from_node_1"))
    t2 = threading.Thread(target=lambda: total[2].send("from_node_2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    time.sleep(2)

    print("\nDelivery order at each node:")
    all_same = True
    reference = None
    for nid in range(4):
        order = [(seq, data) for seq, origin, data in delivered[nid]]
        print(f"  Node {nid}: {order}")
        if reference is None:
            reference = order
        elif order != reference:
            all_same = False

    print(f"\nTotal order consistent across all nodes: {all_same}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def main():
    print("=" * 60)
    print("  MESSAGE ORDERING DEMO")
    print("  FIFO / Causal / Total")
    print("=" * 60)

    scenario_fifo()
    scenario_causal()
    scenario_total()

    print("\n" + "=" * 60)
    print("  ALL ORDERING SCENARIOS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
