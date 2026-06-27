import os
import shutil
import threading
import time

import pytest

from src.network.clock import LamportClock
from src.network.message import Message, MSG_PING, MSG_PONG
from src.network.transport import TransportServer, TransportClient
from src.storage.shared_memory import (
    init_storage, read_all, read_record, write_record, delete_record,
)
from src.config import DATA_DIR


class TestLamportClock:
    def test_increment(self):
        clock = LamportClock()
        assert clock.get_time() == 0
        assert clock.increment() == 1
        assert clock.increment() == 2
        assert clock.get_time() == 2

    def test_update_with_higher(self):
        clock = LamportClock()
        clock.increment()
        result = clock.update(10)
        assert result == 11
        assert clock.get_time() == 11

    def test_update_with_lower(self):
        clock = LamportClock()
        for _ in range(5):
            clock.increment()
        result = clock.update(2)
        assert result == 6
        assert clock.get_time() == 6


class TestMessage:
    def test_serialization_roundtrip(self):
        msg = Message(
            msg_type=MSG_PING,
            sender_id=1,
            receiver_id=2,
            timestamp=42,
            payload={"data": "hello"},
        )
        json_str = msg.to_json()
        restored = Message.from_json(json_str)

        assert restored.type == MSG_PING
        assert restored.sender_id == 1
        assert restored.receiver_id == 2
        assert restored.timestamp == 42
        assert restored.payload == {"data": "hello"}

    def test_defaults(self):
        msg = Message(msg_type=MSG_PONG, sender_id=0)
        assert msg.receiver_id == -1
        assert msg.timestamp == 0
        assert msg.payload == {}

    def test_repr(self):
        msg = Message(msg_type=MSG_PING, sender_id=3, timestamp=7)
        assert "PING" in repr(msg)
        assert "3" in repr(msg)


class TestSharedMemory:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        init_storage(99)
        yield
        shutil.rmtree(os.path.join(DATA_DIR, "node_99"), ignore_errors=True)

    def test_write_and_read(self):
        write_record(99, "test.txt", "key1", "value1", timestamp=1)
        record = read_record(99, "test.txt", "key1")
        assert record is not None
        assert record["key"] == "key1"
        assert record["value"] == "value1"
        assert record["timestamp"] == 1

    def test_read_all(self):
        write_record(99, "test.txt", "a", "1", timestamp=1)
        write_record(99, "test.txt", "b", "2", timestamp=2)
        records = read_all(99, "test.txt")
        assert len(records) == 2
        keys = {r["key"] for r in records}
        assert keys == {"a", "b"}

    def test_update_record(self):
        write_record(99, "test.txt", "key1", "old", timestamp=1)
        write_record(99, "test.txt", "key1", "new", timestamp=2)
        record = read_record(99, "test.txt", "key1")
        assert record["value"] == "new"
        assert record["timestamp"] == 2
        assert len(read_all(99, "test.txt")) == 1

    def test_delete_record(self):
        write_record(99, "test.txt", "key1", "value1", timestamp=1)
        assert delete_record(99, "test.txt", "key1") is True
        assert read_record(99, "test.txt", "key1") is None

    def test_delete_nonexistent(self):
        assert delete_record(99, "test.txt", "nokey") is False

    def test_read_empty(self):
        assert read_all(99, "nofile.txt") == []
        assert read_record(99, "nofile.txt", "key") is None


class TestTransport:
    def test_ping_pong(self):
        received = []
        event = threading.Event()

        def on_message(msg):
            received.append(msg)
            event.set()

        server = TransportServer("127.0.0.1", 15000, on_message)
        server.start()

        try:
            client = TransportClient()
            # Temporarily add a test node to the config
            from src.config import NODES
            NODES[99] = {"host": "127.0.0.1", "port": 15000}

            msg = Message(msg_type=MSG_PING, sender_id=0, receiver_id=99, timestamp=1)
            success = client.send(99, msg)
            assert success is True

            event.wait(timeout=3)
            assert len(received) == 1
            assert received[0].type == MSG_PING
            assert received[0].sender_id == 0

            client.close_all()
        finally:
            del NODES[99]
            server.stop()
