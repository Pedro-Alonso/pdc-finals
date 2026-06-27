import json


MSG_PING = "PING"
MSG_PONG = "PONG"


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
