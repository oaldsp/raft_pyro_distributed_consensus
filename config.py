HOST = "localhost"
NAMESERVER_PORT = 9090

NODE_CONFIGS = {
    "node1": {"port": 5001, "object_id": "raft.node1"},
    "node2": {"port": 5002, "object_id": "raft.node2"},
    "node3": {"port": 5003, "object_id": "raft.node3"},
    "node4": {"port": 5004, "object_id": "raft.node4"},
}

LEADER_NAME = "Líder"
HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)
RPC_TIMEOUT = 1.0


def node_uri(node_id):
    cfg = NODE_CONFIGS[node_id]
    return f"PYRO:{cfg['object_id']}@{HOST}:{cfg['port']}"


PEER_URIS = {node_id: node_uri(node_id) for node_id in NODE_CONFIGS}
