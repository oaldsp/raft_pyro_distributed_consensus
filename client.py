import argparse
import time

import Pyro5.api
from Pyro5.errors import CommunicationError, NamingError, PyroError

from config import HOST, LEADER_NAME, NAMESERVER_PORT, RPC_TIMEOUT


def lookup_leader():
    ns = Pyro5.api.locate_ns(host=HOST, port=NAMESERVER_PORT)
    return ns.lookup(LEADER_NAME)


def send_command(command):
    leader_uri = lookup_leader()
    with Pyro5.api.Proxy(leader_uri) as leader:
        leader._pyroTimeout = RPC_TIMEOUT * 3
        return leader.submit_command(command)


def main():
    parser = argparse.ArgumentParser(description="Cliente Raft")
    parser.add_argument("commands", nargs="*", help="comandos enviados ao lider")
    parser.add_argument("--retry", type=int, default=5, help="tentativas quando ainda nao ha lider")
    args = parser.parse_args()

    commands = args.commands
    if not commands:
        print("Digite comandos para replicar. Use Ctrl+D para sair.")
        commands = (line.strip() for line in iter(input, "") if line.strip())

    for command in commands:
        for attempt in range(1, args.retry + 1):
            try:
                result = send_command(command)
                print(f"comando={command!r} resultado={result}")
                break
            except (NamingError, CommunicationError, PyroError, OSError) as exc:
                if attempt == args.retry:
                    print(f"falha ao enviar {command!r}: {exc}")
                else:
                    time.sleep(1)


if __name__ == "__main__":
    main()
