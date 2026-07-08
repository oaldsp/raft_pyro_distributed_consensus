import argparse
import random
import threading
import time
from dataclasses import asdict, dataclass

import Pyro5.api
from Pyro5.errors import CommunicationError, NamingError, PyroError

from config import (
    ELECTION_TIMEOUT_RANGE,
    HEARTBEAT_INTERVAL,
    HOST,
    LEADER_NAME,
    NAMESERVER_PORT,
    NODE_CONFIGS,
    PEER_URIS,
    RPC_TIMEOUT,
)


@dataclass
class LogEntry:
    term: int
    command: str


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.peers = {nid: uri for nid, uri in PEER_URIS.items() if nid != node_id}
        self.majority = (len(PEER_URIS) // 2) + 1

        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = 0
        self.last_applied = 0
        self.state_machine = []

        self.role = "follower"
        self.leader_id = None
        self.votes_received = set()
        self.last_heartbeat = time.monotonic()
        self.election_deadline = self._new_election_deadline()

        self.running = True
        self.lock = threading.RLock()
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

    def start(self):
        self.election_thread.start()
        self.heartbeat_thread.start()
        print(f"[{self.node_id}] iniciado como follower, termo {self.current_term}", flush=True)

    def stop(self):
        with self.lock:
            self.running = False

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        with self.lock:
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self._become_follower(term)

            candidate_is_current = self._is_candidate_log_up_to_date(last_log_index, last_log_term)
            can_vote = self.voted_for is None or self.voted_for == candidate_id

            if can_vote and candidate_is_current:
                self.voted_for = candidate_id
                self.election_deadline = self._new_election_deadline()
                print(f"[{self.node_id}] votou em {candidate_id} no termo {term}", flush=True)
                return {"term": self.current_term, "vote_granted": True}

            return {"term": self.current_term, "vote_granted": False}

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        with self.lock:
            if term < self.current_term:
                return {"term": self.current_term, "success": False, "match_index": len(self.log)}

            if term > self.current_term or self.role != "follower":
                self._become_follower(term)

            self.leader_id = leader_id
            self.last_heartbeat = time.monotonic()
            self.election_deadline = self._new_election_deadline()

            if prev_log_index > 0:
                if len(self.log) < prev_log_index:
                    return {"term": self.current_term, "success": False, "match_index": len(self.log)}
                if self.log[prev_log_index - 1].term != prev_log_term:
                    self.log = self.log[: prev_log_index - 1]
                    return {"term": self.current_term, "success": False, "match_index": len(self.log)}

            for offset, raw_entry in enumerate(entries):
                index = prev_log_index + offset + 1
                entry = LogEntry(**raw_entry)
                if len(self.log) >= index:
                    if self.log[index - 1].term != entry.term:
                        self.log = self.log[: index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self._apply_committed_entries()

            return {"term": self.current_term, "success": True, "match_index": len(self.log)}

    def submit_command(self, command):
        with self.lock:
            if self.role != "leader":
                return {
                    "success": False,
                    "error": "not_leader",
                    "leader_id": self.leader_id,
                    "term": self.current_term,
                }

            self.log.append(LogEntry(term=self.current_term, command=command))
            entry_index = len(self.log)
            print(f"[{self.node_id}] recebeu comando '{command}' no indice {entry_index}", flush=True)

        ack_count = 1
        for peer_id in self.peers:
            if self._replicate_to_peer(peer_id):
                ack_count += 1

        with self.lock:
            if self.role == "leader" and ack_count >= self.majority:
                self.commit_index = max(self.commit_index, entry_index)
                self._apply_committed_entries()
                print(
                    f"[{self.node_id}] comando '{command}' committed com {ack_count}/{len(PEER_URIS)} confirmacoes",
                    flush=True,
                )

        self._broadcast_heartbeat()

        with self.lock:
            return {
                "success": ack_count >= self.majority,
                "term": self.current_term,
                "index": entry_index,
                "acks": ack_count,
                "committed": self.commit_index >= entry_index,
            }

    def get_status(self):
        with self.lock:
            return {
                "node_id": self.node_id,
                "role": self.role,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "voted_for": self.voted_for,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log": [asdict(entry) for entry in self.log],
                "state_machine": list(self.state_machine),
            }

    def _election_loop(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                if not self.running:
                    return
                timed_out = self.role != "leader" and time.monotonic() >= self.election_deadline

            if timed_out:
                self._start_election()

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                if not self.running:
                    return
                is_leader = self.role == "leader"

            if is_leader:
                self._broadcast_heartbeat()

    def _start_election(self):
        with self.lock:
            self.role = "candidate"
            self.current_term += 1
            term = self.current_term
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.leader_id = None
            self.election_deadline = self._new_election_deadline()
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
            print(f"[{self.node_id}] iniciou eleicao no termo {term}", flush=True)

        for peer_id, uri in self.peers.items():
            try:
                with Pyro5.api.Proxy(uri) as peer:
                    peer._pyroTimeout = RPC_TIMEOUT
                    response = peer.request_vote(term, self.node_id, last_log_index, last_log_term)
            except (CommunicationError, PyroError, OSError) as exc:
                print(f"[{self.node_id}] sem voto de {peer_id}: {exc}", flush=True)
                continue

            with self.lock:
                if self.role != "candidate" or self.current_term != term:
                    return
                if response["term"] > self.current_term:
                    self._become_follower(response["term"])
                    return
                if response["vote_granted"]:
                    self.votes_received.add(peer_id)
                    if len(self.votes_received) >= self.majority:
                        self._become_leader()
                        return

    def _become_leader(self):
        self.role = "leader"
        self.leader_id = self.node_id
        self.votes_received = set()
        print(f"[{self.node_id}] eleito lider no termo {self.current_term}", flush=True)
        self._register_as_leader()
        threading.Thread(target=self._broadcast_heartbeat, daemon=True).start()

    def _become_follower(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        if self.role != "follower":
            print(f"[{self.node_id}] voltou para follower no termo {self.current_term}", flush=True)
        self.role = "follower"
        self.leader_id = None
        self.votes_received = set()
        self.election_deadline = self._new_election_deadline()

    def _register_as_leader(self):
        try:
            ns = Pyro5.api.locate_ns(host=HOST, port=NAMESERVER_PORT)
            cfg = NODE_CONFIGS[self.node_id]
            uri = f"PYRO:{cfg['object_id']}@{HOST}:{cfg['port']}"
            ns.register(LEADER_NAME, uri, safe=False)
            print(f"[{self.node_id}] registrado no nameserver como {LEADER_NAME}: {uri}", flush=True)
        except (NamingError, PyroError, OSError) as exc:
            print(f"[{self.node_id}] falha ao registrar lider: {exc}", flush=True)

    def _broadcast_heartbeat(self):
        for peer_id in list(self.peers):
            self._replicate_to_peer(peer_id)

    def _replicate_to_peer(self, peer_id):
        with self.lock:
            term = self.current_term
            if self.role != "leader":
                return False
            prev_log_index = 0
            prev_log_term = 0
            entries = [asdict(entry) for entry in self.log]
            leader_commit = self.commit_index
            uri = self.peers[peer_id]

        try:
            with Pyro5.api.Proxy(uri) as peer:
                peer._pyroTimeout = RPC_TIMEOUT
                response = peer.append_entries(
                    term,
                    self.node_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
        except (CommunicationError, PyroError, OSError) as exc:
            print(f"[{self.node_id}] falha ao replicar para {peer_id}: {exc}", flush=True)
            return False

        with self.lock:
            if response["term"] > self.current_term:
                self._become_follower(response["term"])
                return False
            return response["success"]

    def _is_candidate_log_up_to_date(self, last_log_index, last_log_term):
        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log)
        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index

    def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            command = self.log[self.last_applied - 1].command
            self.state_machine.append(command)
            print(f"[{self.node_id}] aplicou indice {self.last_applied}: {command}", flush=True)

    def _new_election_deadline(self):
        return time.monotonic() + random.uniform(*ELECTION_TIMEOUT_RANGE)


def main():
    parser = argparse.ArgumentParser(description="Processo Raft usando Pyro5")
    parser.add_argument("node_id", choices=NODE_CONFIGS.keys())
    args = parser.parse_args()

    cfg = NODE_CONFIGS[args.node_id]
    node = RaftNode(args.node_id)

    daemon = Pyro5.api.Daemon(host=HOST, port=cfg["port"])
    uri = daemon.register(node, objectId=cfg["object_id"])

    try:
        ns = Pyro5.api.locate_ns(host=HOST, port=NAMESERVER_PORT)
        ns.register(args.node_id, uri, safe=False)
    except (NamingError, PyroError, OSError) as exc:
        print(f"[{args.node_id}] aviso: nao registrou no nameserver: {exc}", flush=True)

    print(f"[{args.node_id}] URI {uri}", flush=True)
    node.start()

    try:
        daemon.requestLoop()
    finally:
        node.stop()
        daemon.close()


if __name__ == "__main__":
    main()
