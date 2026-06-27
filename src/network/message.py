import json


MSG_PING = "PING"
MSG_PONG = "PONG"

MSG_ELECTION_RING = "ELECTION_RING"
MSG_ELECTED_RING = "ELECTED_RING"
MSG_ELECTION = "ELECTION"
MSG_ELECTION_OK = "ELECTION_OK"
MSG_COORDINATOR = "COORDINATOR"

MSG_FIFO_MSG = "FIFO_MSG"
MSG_CAUSAL_MSG = "CAUSAL_MSG"
MSG_TOTAL_MSG = "TOTAL_MSG"
MSG_TOTAL_SEQ_ASSIGN = "TOTAL_SEQ_ASSIGN"

MSG_MUTEX_REQUEST = "MUTEX_REQUEST"
MSG_MUTEX_REPLY = "MUTEX_REPLY"

MSG_MAEKAWA_REQUEST = "MAEKAWA_REQUEST"
MSG_MAEKAWA_LOCKED = "MAEKAWA_LOCKED"
MSG_MAEKAWA_FAILED = "MAEKAWA_FAILED"
MSG_MAEKAWA_INQUIRE = "MAEKAWA_INQUIRE"
MSG_MAEKAWA_RELINQUISH = "MAEKAWA_RELINQUISH"
MSG_MAEKAWA_RELEASE = "MAEKAWA_RELEASE"

MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"

MSG_PROPOSE = "PROPOSE"
MSG_VOTE_ACCEPT = "VOTE_ACCEPT"
MSG_VOTE_REJECT = "VOTE_REJECT"
MSG_DECIDE = "DECIDE"


class Message:
    def __init__(self, msg_type, sender_id, payload=None, timestamp=0, receiver_id=-1):
        self.type = msg_type
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.timestamp = timestamp
        self.payload = payload or {}

    def to_json(self):
        return json.dumps({
            "type": self.type,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        })

    @classmethod
    def from_json(cls, data):
        d = json.loads(data)
        return cls(
            msg_type=d["type"],
            sender_id=d["sender_id"],
            receiver_id=d.get("receiver_id", -1),
            timestamp=d.get("timestamp", 0),
            payload=d.get("payload", {}),
        )

    def __repr__(self):
        return f"Message(type={self.type}, sender={self.sender_id}, ts={self.timestamp})"
