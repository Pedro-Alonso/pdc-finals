import threading


class LamportClock:
    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time):
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def get_time(self):
        with self._lock:
            return self._time


class VectorClock:
    def __init__(self, node_id, node_ids):
        self.node_id = node_id
        self._clock = {nid: 0 for nid in node_ids}
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._clock[self.node_id] += 1
            return dict(self._clock)

    def update(self, received_vector):
        with self._lock:
            for nid, ts in received_vector.items():
                nid = int(nid)
                if nid in self._clock:
                    self._clock[nid] = max(self._clock[nid], ts)
            self._clock[self.node_id] += 1
            return dict(self._clock)

    def merge(self, received_vector):
        with self._lock:
            for nid, ts in received_vector.items():
                nid = int(nid)
                if nid in self._clock:
                    self._clock[nid] = max(self._clock[nid], ts)
            return dict(self._clock)

    def get_vector(self):
        with self._lock:
            return dict(self._clock)

    @staticmethod
    def happens_before(v1, v2):
        v1 = {int(k): v for k, v in v1.items()}
        v2 = {int(k): v for k, v in v2.items()}
        all_keys = set(v1) | set(v2)
        at_least_one_less = False
        for k in all_keys:
            val1 = v1.get(k, 0)
            val2 = v2.get(k, 0)
            if val1 > val2:
                return False
            if val1 < val2:
                at_least_one_less = True
        return at_least_one_less
