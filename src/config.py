NODES = {
    0: {"host": "127.0.0.1", "port": 5000},
    1: {"host": "127.0.0.1", "port": 5001},
    2: {"host": "127.0.0.1", "port": 5002},
    3: {"host": "127.0.0.1", "port": 5003},
}

RING_ORDER = [0, 1, 2, 3]

DATA_DIR = "data"

HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.0


def get_successor(node_id):
    idx = RING_ORDER.index(node_id)
    return RING_ORDER[(idx + 1) % len(RING_ORDER)]


def get_all_other_nodes(node_id):
    return [nid for nid in NODES if nid != node_id]
