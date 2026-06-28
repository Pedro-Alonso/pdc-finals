import logging
import threading
import time


class TwoPhaseCoordinator:
    def __init__(self, node, wal=None, vote_timeout=5.0, ack_timeout=5.0, ack_retries=2):
        self.node = node
        self.wal = wal
        self.vote_timeout = vote_timeout
        self.ack_timeout = ack_timeout
        self.ack_retries = ack_retries
        self._votes = {}
        self._acks = {}
        self._vote_events = {}
        self._ack_events = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger(f"2pc_coord.{node.node_id}")

    def coordinate_commit(self, txn_id, participants):
        from src.network.message import (
            MSG_2PC_PREPARE, MSG_2PC_GLOBAL_COMMIT, MSG_2PC_GLOBAL_ABORT,
        )

        with self._lock:
            self._votes[txn_id] = {}
            self._acks[txn_id] = set()
            self._vote_events[txn_id] = threading.Event()
            self._ack_events[txn_id] = threading.Event()

        if self.wal:
            self.wal.log_prepare(txn_id)

        self.logger.info(f"Phase 1: sending PREPARE for {txn_id} to {participants}")
        for pid in participants:
            self.node.send(pid, MSG_2PC_PREPARE, {"txn_id": txn_id})

        deadline = time.time() + self.vote_timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self._vote_events[txn_id].wait(timeout=remaining)
            with self._lock:
                votes = dict(self._votes.get(txn_id, {}))
            if len(votes) >= len(participants) or any(v == "abort" for v in votes.values()):
                break
            self._vote_events[txn_id].clear()

        with self._lock:
            votes = dict(self._votes.get(txn_id, {}))

        all_commit = (
            len(votes) == len(participants)
            and all(v == "commit" for v in votes.values())
        )

        if all_commit:
            if self.wal:
                self.wal.log_global_commit(txn_id)
            decision = MSG_2PC_GLOBAL_COMMIT
            self.logger.info(f"Phase 2: GLOBAL_COMMIT for {txn_id}")
        else:
            if self.wal:
                self.wal.log_global_abort(txn_id)
            decision = MSG_2PC_GLOBAL_ABORT
            missing = set(participants) - set(votes.keys())
            abort_voters = [p for p, v in votes.items() if v == "abort"]
            self.logger.info(
                f"Phase 2: GLOBAL_ABORT for {txn_id} "
                f"(missing={missing}, abort_voters={abort_voters})"
            )

        for pid in participants:
            self.node.send(pid, decision, {"txn_id": txn_id})

        for attempt in range(self.ack_retries + 1):
            deadline = time.time() + self.ack_timeout
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self._ack_events[txn_id].wait(timeout=remaining)
                with self._lock:
                    acks = set(self._acks.get(txn_id, set()))
                if len(acks) >= len(participants):
                    break
                self._ack_events[txn_id].clear()

            with self._lock:
                acks = set(self._acks.get(txn_id, set()))
            if len(acks) >= len(participants):
                break
            missing_acks = set(participants) - acks
            if attempt < self.ack_retries:
                self.logger.warning(f"Retrying decision to {missing_acks}")
                for pid in missing_acks:
                    self.node.send(pid, decision, {"txn_id": txn_id})

        with self._lock:
            self._votes.pop(txn_id, None)
            self._acks.pop(txn_id, None)
            self._vote_events.pop(txn_id, None)
            self._ack_events.pop(txn_id, None)

        return decision == MSG_2PC_GLOBAL_COMMIT

    def handle_vote_commit(self, message):
        txn_id = message.payload["txn_id"]
        sender = message.sender_id
        with self._lock:
            if txn_id in self._votes:
                self._votes[txn_id][sender] = "commit"
                self._vote_events[txn_id].set()
        self.logger.info(f"Received VOTE_COMMIT from node {sender} for {txn_id}")

    def handle_vote_abort(self, message):
        txn_id = message.payload["txn_id"]
        sender = message.sender_id
        with self._lock:
            if txn_id in self._votes:
                self._votes[txn_id][sender] = "abort"
                self._vote_events[txn_id].set()
        self.logger.info(f"Received VOTE_ABORT from node {sender} for {txn_id}")

    def handle_ack(self, message):
        txn_id = message.payload["txn_id"]
        sender = message.sender_id
        with self._lock:
            if txn_id in self._acks:
                self._acks[txn_id].add(sender)
                self._ack_events[txn_id].set()
        self.logger.info(f"Received ACK from node {sender} for {txn_id}")

    def recover_pending(self):
        if not self.wal:
            return
        from src.network.message import MSG_2PC_GLOBAL_COMMIT, MSG_2PC_GLOBAL_ABORT

        entries = self.wal.read_all()
        prepared = set()
        decided = {}
        for entry in entries:
            if entry.entry_type == "PREPARE":
                prepared.add(entry.txn_id)
            elif entry.entry_type == "GLOBAL_COMMIT":
                decided[entry.txn_id] = MSG_2PC_GLOBAL_COMMIT
            elif entry.entry_type == "GLOBAL_ABORT":
                decided[entry.txn_id] = MSG_2PC_GLOBAL_ABORT

        for txn_id in prepared:
            if txn_id not in decided:
                self.wal.log_global_abort(txn_id)
                self.logger.info(f"Recovery: aborting undecided {txn_id}")


class TwoPhaseParticipant:
    def __init__(self, node, wal=None, txn_manager=None):
        self.node = node
        self.wal = wal
        self.txn_manager = txn_manager
        self._pending_decisions = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger(f"2pc_part.{node.node_id}")

    def handle_prepare(self, message):
        from src.network.message import MSG_2PC_VOTE_COMMIT, MSG_2PC_VOTE_ABORT

        txn_id = message.payload["txn_id"]
        coordinator_id = message.sender_id

        can_commit = self._check_can_commit(txn_id)

        if can_commit:
            if self.wal:
                self.wal.log_vote_commit(txn_id)
            with self._lock:
                self._pending_decisions[txn_id] = "voted_commit"
            self.node.send(coordinator_id, MSG_2PC_VOTE_COMMIT, {"txn_id": txn_id})
            self.logger.info(f"Voted COMMIT for {txn_id}")
        else:
            if self.wal:
                self.wal.log_vote_abort(txn_id)
            with self._lock:
                self._pending_decisions[txn_id] = "voted_abort"
            self.node.send(coordinator_id, MSG_2PC_VOTE_ABORT, {"txn_id": txn_id})
            self.logger.info(f"Voted ABORT for {txn_id}")

    def _check_can_commit(self, txn_id):
        if self.txn_manager:
            txn = self.txn_manager.get_transaction(txn_id)
            if txn and txn.status.value == "active":
                return True
        return True

    def handle_global_commit(self, message):
        from src.network.message import MSG_2PC_ACK

        txn_id = message.payload["txn_id"]
        coordinator_id = message.sender_id

        if self.txn_manager:
            txn = self.txn_manager.get_transaction(txn_id)
            if txn and txn.status.value == "active":
                self.txn_manager.commit(txn_id)

        with self._lock:
            self._pending_decisions.pop(txn_id, None)

        self.node.send(coordinator_id, MSG_2PC_ACK, {"txn_id": txn_id})
        self.logger.info(f"Applied GLOBAL_COMMIT for {txn_id}")

    def handle_global_abort(self, message):
        from src.network.message import MSG_2PC_ACK

        txn_id = message.payload["txn_id"]
        coordinator_id = message.sender_id

        if self.txn_manager:
            txn = self.txn_manager.get_transaction(txn_id)
            if txn and txn.status.value == "active":
                self.txn_manager.abort(txn_id)

        with self._lock:
            self._pending_decisions.pop(txn_id, None)

        self.node.send(coordinator_id, MSG_2PC_ACK, {"txn_id": txn_id})
        self.logger.info(f"Applied GLOBAL_ABORT for {txn_id}")

    def recover_pending(self):
        if not self.wal:
            return
        entries = self.wal.read_all()
        voted = {}
        decided = set()
        for entry in entries:
            if entry.entry_type == "VOTE_COMMIT":
                voted[entry.txn_id] = "commit"
            elif entry.entry_type == "VOTE_ABORT":
                voted[entry.txn_id] = "abort"
            elif entry.entry_type in ("GLOBAL_COMMIT", "GLOBAL_ABORT", "COMMIT", "ABORT"):
                decided.add(entry.txn_id)

        for txn_id, vote in voted.items():
            if txn_id not in decided:
                if vote == "abort":
                    if self.wal:
                        self.wal.log_abort(txn_id)
                    self.logger.info(f"Recovery: aborted {txn_id} (had voted abort)")
                else:
                    with self._lock:
                        self._pending_decisions[txn_id] = "awaiting_decision"
                    self.logger.info(f"Recovery: {txn_id} blocked (voted commit, awaiting coordinator)")
