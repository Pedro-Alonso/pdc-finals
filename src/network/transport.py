import socket
import struct
import threading
import logging

from src.network.message import Message
from src.config import NODES

logger = logging.getLogger("transport")
logging.basicConfig(level=logging.WARNING)


class TransportServer:
    def __init__(self, host, port, on_message):
        self.host = host
        self.port = port
        self.on_message = on_message
        self._server_socket = None
        self._running = False
        self._threads = []
        self._connections = []
        self._conn_lock = threading.Lock()

    def start(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(10)
        self._server_socket.settimeout(1.0)
        self._running = True

        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        self._threads.append(t)
        logger.info(f"Server listening on {self.host}:{self.port}")

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server_socket.accept()
                with self._conn_lock:
                    self._connections.append(conn)
                t = threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True)
                t.start()
                self._threads.append(t)
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn, addr):
        conn.settimeout(2.0)
        buf = b""
        while self._running:
            try:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while len(buf) >= 4:
                    length = struct.unpack("!I", buf[:4])[0]
                    if len(buf) < 4 + length:
                        break
                    payload = buf[4:4 + length].decode("utf-8")
                    buf = buf[4 + length:]
                    try:
                        msg = Message.from_json(payload)
                        self.on_message(msg)
                    except Exception:
                        logger.exception("Failed to parse message")
            except socket.timeout:
                continue
            except OSError:
                break
        conn.close()

    def stop(self):
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        with self._conn_lock:
            for conn in self._connections:
                try:
                    conn.close()
                except OSError:
                    pass
            self._connections.clear()


class TransportClient:
    def __init__(self):
        self._connections = {}
        self._lock = threading.Lock()
        self._send_locks = {}

    def _get_send_lock(self, node_id):
        with self._lock:
            if node_id not in self._send_locks:
                self._send_locks[node_id] = threading.Lock()
            return self._send_locks[node_id]

    def _get_connection(self, node_id):
        conn = self._connections.get(node_id)
        if conn:
            try:
                conn.getpeername()
                return conn
            except OSError:
                self._connections.pop(node_id, None)

        node_info = NODES[node_id]
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(2.0)
            conn.connect((node_info["host"], node_info["port"]))
            self._connections[node_id] = conn
            return conn
        except OSError:
            logger.warning(f"Cannot connect to node {node_id} at {node_info['host']}:{node_info['port']}")
            return None

    def send(self, node_id, message):
        send_lock = self._get_send_lock(node_id)
        with send_lock:
            conn = self._get_connection(node_id)
            if not conn:
                return False
            data = message.to_json().encode("utf-8")
            frame = struct.pack("!I", len(data)) + data
            try:
                conn.sendall(frame)
                return True
            except OSError:
                logger.warning(f"Failed to send to node {node_id}")
                self._connections.pop(node_id, None)
                try:
                    conn.close()
                except OSError:
                    pass
                return False

    def broadcast(self, message, exclude=None):
        exclude = exclude or []
        for node_id in NODES:
            if node_id not in exclude:
                self.send(node_id, message)

    def close_all(self):
        with self._lock:
            for conn in self._connections.values():
                try:
                    conn.close()
                except OSError:
                    pass
            self._connections.clear()
