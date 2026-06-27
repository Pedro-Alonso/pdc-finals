import logging
import threading

from src.network.message import MSG_PROPOSE, MSG_VOTE_ACCEPT, MSG_VOTE_REJECT, MSG_DECIDE


CONSENSUS_TIMEOUT = 5.0


class ConsensusRound:
    def __init__(self, round_id, proposed_value):
        self.round_id = round_id
        self.proposed_value = proposed_value
        self.votes = {}
        self.decided = False
        self.decision = None


class ConsensusProtocol:
    def __init__(self, node, failure_detector=None):
        self.node = node
        self.failure_detector = failure_detector
        self._lock = threading.Lock()
        self._round_counter = 0
        self._rounds = {}
        self._decided_values = {}
        self._decision_event = threading.Event()

        self.node.register_handler(MSG_PROPOSE, self._handle_propose)
        self.node.register_handler(MSG_VOTE_ACCEPT, self._handle_vote)
        self.node.register_handler(MSG_VOTE_REJECT, self._handle_vote)
        self.node.register_handler(MSG_DECIDE, self._handle_decide)

    def propose(self, value, timeout=CONSENSUS_TIMEOUT):
        if not self.node.is_leader():
            self.node._log(logging.WARNING, "Only the leader can propose")
            return None

        with self._lock:
            self._round_counter += 1
            round_id = self._round_counter
            rnd = ConsensusRound(round_id, value)
            self._rounds[round_id] = rnd
            self._decision_event.clear()

        alive = set()
        if self.failure_detector:
            alive = self.failure_detector.get_alive_nodes()
        else:
            from src.config import get_all_other_nodes
            alive = set(get_all_other_nodes(self.node.node_id))

        self.node._log(logging.INFO, f"Proposing value '{value}' in round {round_id} to {len(alive)} nodes")

        for nid in alive:
            self.node.send(nid, MSG_PROPOSE, {
                "round_id": round_id,
                "value": value,
            })

        self._decision_event.wait(timeout=timeout)

        with self._lock:
            rnd = self._rounds[round_id]
            if rnd.decided:
                return rnd.decision

            accepts = sum(1 for v in rnd.votes.values() if v)
            total_alive = len(alive) + 1
            if accepts + 1 > total_alive / 2:
                rnd.decided = True
                rnd.decision = value
                self._decided_values[round_id] = value
                self.node._log(logging.INFO, f"Consensus reached for round {round_id}: '{value}' ({accepts + 1}/{total_alive})")
                for nid in alive:
                    self.node.send(nid, MSG_DECIDE, {
                        "round_id": round_id,
                        "value": value,
                    })
                return value

        self.node._log(logging.WARNING, f"Consensus failed for round {round_id}")
        return None

    def _handle_propose(self, message):
        round_id = message.payload["round_id"]
        value = message.payload["value"]
        self.node._log(logging.INFO, f"Received proposal for round {round_id}: '{value}'")
        self.node.send(message.sender_id, MSG_VOTE_ACCEPT, {
            "round_id": round_id,
        })

    def _handle_vote(self, message):
        round_id = message.payload["round_id"]
        accepted = message.type == MSG_VOTE_ACCEPT

        with self._lock:
            rnd = self._rounds.get(round_id)
            if not rnd or rnd.decided:
                return

            rnd.votes[message.sender_id] = accepted

            alive = set()
            if self.failure_detector:
                alive = self.failure_detector.get_alive_nodes()
            else:
                from src.config import get_all_other_nodes
                alive = set(get_all_other_nodes(self.node.node_id))

            accepts = sum(1 for v in rnd.votes.values() if v)
            total_alive = len(alive) + 1

            if accepts + 1 > total_alive / 2:
                rnd.decided = True
                rnd.decision = rnd.proposed_value
                self._decided_values[round_id] = rnd.proposed_value
                self.node._log(logging.INFO, f"Consensus reached for round {round_id}: '{rnd.proposed_value}' ({accepts + 1}/{total_alive})")
                self._decision_event.set()
                for nid in alive:
                    self.node.send(nid, MSG_DECIDE, {
                        "round_id": round_id,
                        "value": rnd.proposed_value,
                    })

    def _handle_decide(self, message):
        round_id = message.payload["round_id"]
        value = message.payload["value"]

        with self._lock:
            self._decided_values[round_id] = value
            rnd = self._rounds.get(round_id)
            if rnd:
                rnd.decided = True
                rnd.decision = value

        self.node._log(logging.INFO, f"Decision received for round {round_id}: '{value}'")
        self._decision_event.set()

    def get_decision(self, round_id):
        with self._lock:
            return self._decided_values.get(round_id)
