import argparse
import signal
import subprocess
import sys
import time

from config import HOST, NAMESERVER_PORT, NODE_CONFIGS


def start_process(command):
    return subprocess.Popen(command)


def main():
    parser = argparse.ArgumentParser(description="Inicializa nameserver e 4 processos Raft")
    parser.add_argument("--no-client", action="store_true", help="nao abre o cliente interativo ao final")
    args = parser.parse_args()

    processes = []
    shutting_down = False

    def shutdown(*_):
        nonlocal shutting_down
        shutting_down = True
        for process in processes:
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    ns_cmd = [
        sys.executable,
        "-m",
        "Pyro5.nameserver",
        "-n",
        HOST,
        "-p",
        str(NAMESERVER_PORT),
    ]
    processes.append(start_process(ns_cmd))
    time.sleep(1)

    for node_id in NODE_CONFIGS:
        processes.append(start_process([sys.executable, "raft_node.py", node_id]))
        time.sleep(0.2)

    print("Cluster iniciado. Aguarde a eleicao do lider.")
    print("URIs hard coded:")
    for node_id, cfg in NODE_CONFIGS.items():
        print(f"  {node_id}: PYRO:{cfg['object_id']}@{HOST}:{cfg['port']}")

    if args.no_client:
        print("Pressione Ctrl+C para encerrar.")
        while not shutting_down:
            time.sleep(1)
    else:
        time.sleep(3)
        processes.append(start_process([sys.executable, "client.py"]))
        while not shutting_down and processes[-1].poll() is None:
            time.sleep(0.2)

    shutdown()
    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    main()
