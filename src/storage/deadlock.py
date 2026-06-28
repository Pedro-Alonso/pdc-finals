import threading
import logging
import time


class DeadlockDetector:
    def __init__(self, lock_manager, on_victim=None, interval=2.0):
        self.lock_manager = lock_manager
        self._on_victim = on_victim
        self._interval = interval
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        self.logger = logging.getLogger("deadlock_detector")

    def detect_cycle(self):
        graph = self.lock_manager.get_wait_edges()
        if not graph:
            return None

        visited = set()
        rec_stack = set()
        parent = {}

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    parent[neighbor] = node
                    cycle = dfs(neighbor)
                    if cycle is not None:
                        return cycle
                elif neighbor in rec_stack:
                    cycle = [neighbor]
                    cur = node
                    while cur != neighbor:
                        cycle.append(cur)
                        cur = parent.get(cur)
                        if cur is None:
                            break
                    cycle.append(neighbor)
                    return cycle
            rec_stack.discard(node)
            return None

        for node in list(graph.keys()):
            if node not in visited:
                parent.clear()
                cycle = dfs(node)
                if cycle is not None:
                    return cycle
        return None

    def select_victim(self, cycle):
        candidates = [txn for txn in cycle if txn != cycle[0] or cycle.count(txn) > 1]
        unique = list(dict.fromkeys(candidates))
        if not unique:
            unique = list(dict.fromkeys(cycle))
        return max(unique, key=self._txn_sort_key)

    def _txn_sort_key(self, txn_id):
        parts = txn_id.split("_")
        nums = [int(p) for p in parts if p.isdigit()]
        return nums[-1] if nums else 0

    def _detector_loop(self):
        while self._running:
            time.sleep(self._interval)
            if not self._running:
                break
            cycle = self.detect_cycle()
            if cycle:
                victim = self.select_victim(cycle)
                self.logger.warning(
                    f"Deadlock detected: cycle={cycle}, victim={victim}"
                )
                if self._on_victim:
                    self._on_victim(victim)

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._detector_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._interval + 1)
