import os
import threading

from src.config import DATA_DIR

_file_locks = {}
_global_lock = threading.Lock()


def _get_file_lock(path):
    with _global_lock:
        if path not in _file_locks:
            _file_locks[path] = threading.Lock()
        return _file_locks[path]


def _data_path(node_id, filename):
    return os.path.join(DATA_DIR, f"node_{node_id}", filename)


def init_storage(node_id):
    dir_path = os.path.join(DATA_DIR, f"node_{node_id}")
    os.makedirs(dir_path, exist_ok=True)


def read_all(node_id, filename):
    path = _data_path(node_id, filename)
    lock = _get_file_lock(path)
    with lock:
        if not os.path.exists(path):
            return []
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|", 2)
                if len(parts) == 3:
                    records.append({
                        "key": parts[0],
                        "value": parts[1],
                        "timestamp": int(parts[2]),
                    })
        return records


def read_record(node_id, filename, key):
    records = read_all(node_id, filename)
    for r in records:
        if r["key"] == key:
            return r
    return None


def write_record(node_id, filename, key, value, timestamp=0):
    path = _data_path(node_id, filename)
    lock = _get_file_lock(path)
    with lock:
        init_storage(node_id)
        records = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(line)
        found = False
        new_records = []
        for rec in records:
            parts = rec.split("|", 2)
            if len(parts) == 3 and parts[0] == key:
                new_records.append(f"{key}|{value}|{timestamp}")
                found = True
            else:
                new_records.append(rec)
        if not found:
            new_records.append(f"{key}|{value}|{timestamp}")

        with open(path, "w", encoding="utf-8") as f:
            for rec in new_records:
                f.write(rec + "\n")


def delete_record(node_id, filename, key):
    path = _data_path(node_id, filename)
    lock = _get_file_lock(path)
    with lock:
        if not os.path.exists(path):
            return False
        records = []
        found = False
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|", 2)
                if len(parts) == 3 and parts[0] == key:
                    found = True
                else:
                    records.append(line)
        if not found:
            return False
        with open(path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(rec + "\n")
        return True
