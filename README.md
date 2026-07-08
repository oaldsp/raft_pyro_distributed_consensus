# raft_pyro_distributed_consensus

Implementação do algoritmo de consenso Raft em Python usando PyRO/Pyro5 para comunicação remota entre 4 processos.

O projeto inicializa:

- 1 servidor de nomes do Pyro;
- 4 processos Raft, inicialmente seguidores;
- 1 cliente que consulta o líder no servidor de nomes e envia comandos.

Cada nó usa porta e `objectId` fixos, formando URIs hard coded:

- `node1`: `PYRO:raft.node1@localhost:5001`
- `node2`: `PYRO:raft.node2@localhost:5002`
- `node3`: `PYRO:raft.node3@localhost:5003`
- `node4`: `PYRO:raft.node4@localhost:5004`

Quando um nó vence a eleição, ele registra sua URI no servidor de nomes com o nome `Líder`, sobrescrevendo a entrada anterior.

## Requisitos

Use Python 3.

```bash
python3 -m pip install -r requirements.txt
```

## Executar com Docker

Suba o cluster com Docker Compose:

```bash
docker compose up --build
```

Depois que algum nó aparecer como `eleito lider`, envie comandos em outro terminal:

```bash
docker compose exec raft python3 client.py "set x=1" "set y=2"
```

Para abrir o cliente interativo dentro do contêiner:

```bash
docker compose exec raft python3 client.py
```

Para parar tudo:

```bash
docker compose down
```

As portas `9090`, `5001`, `5002`, `5003` e `5004` são expostas para inspeção, mas o cliente deve ser executado dentro do contêiner porque os URIs dos objetos foram fixados como `localhost`.

## Executar tudo junto

```bash
python3 cluster.py
```

O script sobe o servidor de nomes, os 4 nós e depois abre o cliente interativo. Digite comandos no cliente para enviá-los ao líder:

```text
set x=1
set y=2
```

Para subir apenas o cluster, sem cliente:

```bash
python3 cluster.py --no-client
```

Em outro terminal, envie comandos:

```bash
python3 client.py "set x=1" "set y=2"
```

## Executar manualmente

Terminal 1:

```bash
python3 -m Pyro5.nameserver -n localhost -p 9090
```

Terminais 2 a 5:

```bash
python3 raft_node.py node1
python3 raft_node.py node2
python3 raft_node.py node3
python3 raft_node.py node4
```

Terminal 6:

```bash
python3 client.py "comando A"
```
