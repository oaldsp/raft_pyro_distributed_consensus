import asyncio
import logging
import random
import time
import grpc
from grpc import aio
from storage import RaftStorage

# Importa os módulos gerados pelo compilador do Protocol Buffers
import raft_pb2
import raft_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')

NODE_CONFIGS = {
    "node1": "node1:5001",
    "node2": "node2:5002",
    "node3": "node3:5003",
    "node4": "node4:5004",
}
HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0

class RaftNode(raft_pb2_grpc.ConsensusServiceServicer, raft_pb2_grpc.AppServiceServicer):
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.logger = logging.getLogger(self.node_id)
        
        # Configuração de rede
        self.host_port = NODE_CONFIGS[node_id]
        self.peers = {nid: addr for nid, addr in NODE_CONFIGS.items() if nid != node_id}
        
        # O quórum em um cluster de 4 nós é 3 (maioria absoluta: 4 // 2 + 1)
        self.majority = 3 
        
        # Canais gRPC para os peers (Lazy loading)
        self.peer_stubs = {}

        # Lock assíncrono para proteger o estado interno do Raft
        self.lock = asyncio.Lock()
        
        # Estado Persistente (A ser injetado/gerenciado na próxima etapa via SQLite)
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        # Injeção da Persistência
        self.storage = RaftStorage(node_id)
        
        # Recuperação de Falhas (Cenário 3 e 4)
        saved_term, saved_vote = self.storage.load_state()
        self.current_term = saved_term
        self.voted_for = saved_vote
        
        # Estado Volátil
        self.commit_index = 0
        self.last_applied = 0
        self.role = "follower"
        self.leader_id = None
        
        # Controle de Eleição
        self.votes_received = set()
        self.election_deadline = 0.0
        
        # Tasks do AsyncIO
        self.running = False
        self.tasks = []

    def _get_peer_stub(self, peer_id):
        """Cria ou reaproveita um canal gRPC assíncrono para um peer."""
        if peer_id not in self.peer_stubs:
            channel = aio.insecure_channel(self.peers[peer_id])
            self.peer_stubs[peer_id] = raft_pb2_grpc.ConsensusServiceStub(channel)
        return self.peer_stubs[peer_id]

    def _reset_election_deadline(self):
        """Redefine o timer de eleição com um jitter aleatório para evitar split votes."""
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self.election_deadline = time.monotonic() + timeout

    async def _become_follower(self, term: int, leader_id: str = None):
        """Transição para o estado de Seguidor."""
        self.role = "follower"
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id
        self.votes_received.clear()
        self._reset_election_deadline()
        
        # Delega o I/O do SQLite para uma thread em background
        await asyncio.to_thread(self.storage.save_state, self.current_term, self.voted_for)
        self.logger.info(f"Tornou-se FOLLOWER no termo {term}")

    async def _become_candidate(self):
        """Transição para o estado de Candidato e início de eleição."""
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self.votes_received = {self.node_id}
        self._reset_election_deadline()
        
        # Salva o novo termo e o próprio voto antes de pedir votos aos outros
        await asyncio.to_thread(self.storage.save_state, self.current_term, self.voted_for)
        
        self.logger.info(f"Iniciando eleição para o termo {self.current_term}")
        
        last_log_index, last_log_term = await asyncio.to_thread(self.storage.get_last_log_info)
        
        request = raft_pb2.VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
        for peer_id in self.peers:
            asyncio.create_task(self._send_request_vote(peer_id, request))

    async def _become_leader(self):
        """Transição para o estado de Líder."""
        self.role = "leader"
        self.leader_id = self.node_id
        
        # Arrays de rastreamento de quórum requeridos pelo Raft
        last_log_index, _ = await asyncio.to_thread(self.storage.get_last_log_info)
        self.next_index = {p: last_log_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.match_index[self.node_id] = last_log_index
        
        self.logger.info(f"ELEITO LÍDER no termo {self.current_term}!")
        await self._broadcast_heartbeat()

    # =========================================================================
    # LOOPS DE BACKGROUND (ASYNCIO)
    # =========================================================================

    async def _election_timer_loop(self):
        """Monitora o tempo para disparar eleições caso não receba heartbeats."""
        while self.running:
            await asyncio.sleep(0.1)  # Tick de verificação
            async with self.lock:
                if self.role != "leader":
                    if time.monotonic() >= self.election_deadline:
                        await self._become_candidate()

    async def _heartbeat_loop(self):
        """Líder envia heartbeats periodicamente para manter a autoridade."""
        while self.running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            async with self.lock:
                if self.role == "leader":
                    await self._broadcast_heartbeat()

    # =========================================================================
    # COMUNICAÇÃO RPC (SAÍDA)
    # =========================================================================

    async def _send_request_vote(self, peer_id: str, request: raft_pb2.VoteRequest):
        """Envia um pedido de voto para um peer específico."""
        stub = self._get_peer_stub(peer_id)
        try:
            # Timeout curto para não prender a eleição em nós mortos
            response = await stub.RequestVote(request, timeout=1.0)
            
            async with self.lock:
                if self.role != "candidate" or self.current_term != request.term:
                    return # Estado mudou enquanto aguardávamos a resposta
                
                if response.term > self.current_term:
                    await self._become_follower(response.term)
                    return
                
                if response.vote_granted:
                    self.votes_received.add(peer_id)
                    if len(self.votes_received) >= self.majority:
                        await self._become_leader()
                        
        except grpc.aio.AioRpcError:
            self.logger.debug(f"Falha ao solicitar voto de {peer_id}")

    async def _broadcast_heartbeat(self):
        """Dispara a replicação individual para cada nó seguidor."""
        for peer_id in self.peers:
            asyncio.create_task(self._replicate_to_peer(peer_id))
    
    async def _replicate_to_peer(self, peer_id: str):
        """Envia AppendEntries focado na necessidade específica da réplica."""
        async with self.lock:
            if self.role != "leader":
                return
            
            next_idx = self.next_index[peer_id]
            prev_log_index = next_idx - 1
            prev_log_term = await asyncio.to_thread(self.storage.get_log_term, prev_log_index)
            
            # Busca todos os logs pendentes para este nó
            entries_data = await asyncio.to_thread(self.storage.get_entries_from, next_idx)
            entries = [raft_pb2.LogEntry(term=e[0], command=e[1]) for e in entries_data]
            
            request = raft_pb2.AppendRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            )
            
        stub = self._get_peer_stub(peer_id)
        try:
            response = await stub.AppendEntries(request, timeout=1.5)
        except grpc.aio.AioRpcError:
            return  # Nó inacessível, tentará novamente no próximo heartbeat

        # Avalia a resposta da réplica
        async with self.lock:
            if response.term > self.current_term:
                await self._become_follower(response.term)
                return
            
            if self.role != "leader" or self.current_term != request.term:
                return
            
            if response.success:
                # Atualiza os ponteiros de sincronização com sucesso
                new_match = prev_log_index + len(entries)
                if new_match > self.match_index[peer_id]:
                    self.match_index[peer_id] = new_match
                    self.next_index[peer_id] = new_match + 1
                    await self._update_commit_index()
            else:
                # Réplica indicou inconsistência. Recua o ponteiro para reenviar na próxima rodada.
                # Usa o match_index retornado pela réplica para otimizar o recuo.
                self.next_index[peer_id] = max(1, response.match_index + 1)
                asyncio.create_task(self._replicate_to_peer(peer_id))
    
    async def _update_commit_index(self):
        """Calcula o quórum e efetiva os logs se a maioria confirmar."""
        # Ordena os índices de match_index de forma decrescente
        sorted_match = sorted(self.match_index.values(), reverse=True)
        
        # Em um cluster de 4 nós, a maioria é 3. 
        # O 3º maior valor (índice 2) representa o log replicado em pelo menos 3 nós.
        quorum_match = sorted_match[self.majority - 1]
        
        if quorum_match > self.commit_index:
            # Regra de Segurança do Raft: só efetiva logs do termo atual
            quorum_term = await asyncio.to_thread(self.storage.get_log_term, quorum_match)
            if quorum_term == self.current_term:
                self.commit_index = quorum_match
                await asyncio.to_thread(self.storage.commit_logs_up_to, self.commit_index)
                self.logger.info(f"Quórum alcançado. Índice {self.commit_index} EFETIVADO (Committed).")

    # async def _send_append_entries(self, peer_id: str, request: raft_pb2.AppendRequest):
    #     stub = self._get_peer_stub(peer_id)
    #     try:
    #         response = await stub.AppendEntries(request, timeout=1.0)
    #         async with self.lock:
    #             if response.term > self.current_term:
    #                 await self._become_follower(response.term)
    #     except grpc.aio.AioRpcError:
    #         pass # Timeout/Indisponibilidade do peer no heartbeat

    # =========================================================================
    # ENDPOINTS RPC (ENTRADA) - Consensus Service
    # =========================================================================

    async def RequestVote(self, request: raft_pb2.VoteRequest, context: grpc.aio.ServicerContext):
        """Processa requisições de voto de outros candidatos."""
        async with self.lock:
            if request.term < self.current_term:
                return raft_pb2.VoteResponse(term=self.current_term, vote_granted=False)
            
            if request.term > self.current_term:
                await self._become_follower(request.term)
            
            my_last_log_index, my_last_log_term = await asyncio.to_thread(self.storage.get_last_log_info)
            
            log_is_ok = (request.last_log_term > my_last_log_term) or \
                        (request.last_log_term == my_last_log_term and request.last_log_index >= my_last_log_index)
            
            can_vote = self.voted_for is None or self.voted_for == request.candidate_id
            
            if can_vote and log_is_ok:
                self.voted_for = request.candidate_id
                self._reset_election_deadline()
                # Persiste a concessão do voto
                await asyncio.to_thread(self.storage.save_state, self.current_term, self.voted_for)
                
                self.logger.info(f"Votou em {request.candidate_id} no termo {self.current_term}")
                return raft_pb2.VoteResponse(term=self.current_term, vote_granted=True)
            
            return raft_pb2.VoteResponse(term=self.current_term, vote_granted=False)

    async def AppendEntries(self, request: raft_pb2.AppendRequest, context: grpc.aio.ServicerContext):
        """Recepção de logs e heartbeats pelas réplicas."""
        async with self.lock:
            # 1. Rejeita imediatamente se o líder for obsoleto
            if request.term < self.current_term:
                return raft_pb2.AppendResponse(term=self.current_term, success=False, match_index=0)
            
            self._reset_election_deadline()
            if request.term > self.current_term or self.role != "follower":
                await self._become_follower(request.term, leader_id=request.leader_id)
            else:
                self.leader_id = request.leader_id

            # 2. Verificação de Consistência (Incompatibilidade local vs Líder)
            if request.prev_log_index > 0:
                local_prev_term = await asyncio.to_thread(self.storage.get_log_term, request.prev_log_index)
                if local_prev_term != request.prev_log_term:
                    last_valid_info = await asyncio.to_thread(self.storage.get_last_log_info)
                    # Informa falha e o ponto atual para auxiliar o recuo do líder
                    return raft_pb2.AppendResponse(
                        term=self.current_term, 
                        success=False, 
                        match_index=last_valid_info[0] 
                    )
            
            # 3. Truncamento de Conflitos e Inserção
            if request.entries:
                # Remove divergências a partir do ponto de conflito
                await asyncio.to_thread(self.storage.truncate_log_from, request.prev_log_index + 1)
                
                # Aplica as novas entradas fornecidas pelo líder
                for i, entry in enumerate(request.entries):
                    log_idx = request.prev_log_index + 1 + i
                    await asyncio.to_thread(self.storage.append_entry, log_idx, entry.term, entry.command)

            # 4. Atualização do Commit (Efetivação orientada pelo Líder)
            my_last_index, _ = await asyncio.to_thread(self.storage.get_last_log_info)
            if request.leader_commit > self.commit_index:
                # A réplica não pode commitar além do que ela própria já possui
                self.commit_index = min(request.leader_commit, my_last_index)
                await asyncio.to_thread(self.storage.commit_logs_up_to, self.commit_index)
            
            return raft_pb2.AppendResponse(
                term=self.current_term, 
                success=True, 
                match_index=my_last_index
            )

    # =========================================================================
    # ENDPOINTS RPC (ENTRADA) - App Service (Cliente Node.js)
    # =========================================================================

    async def PublishData(self, request: raft_pb2.PublishRequest, context: grpc.aio.ServicerContext):
        """Recebe dados do cliente, salva localmente e aguarda o quórum."""
        async with self.lock:
            if self.role != "leader":
                return raft_pb2.PublishResponse(success=False, leader_id=self.leader_id or "", index=-1)
            
            # Adiciona ao log local como "uncommitted"
            last_index, _ = await asyncio.to_thread(self.storage.get_last_log_info)
            new_index = last_index + 1
            await asyncio.to_thread(self.storage.append_entry, new_index, self.current_term, request.command)
            
            self.match_index[self.node_id] = new_index
            self.logger.info(f"Comando recebido: '{request.command}'. Índice: {new_index}. Aguardando quórum...")
            
        # Dispara replicação imediata (fora do lock para não bloquear o servidor)
        asyncio.create_task(self._broadcast_heartbeat())
        
        # Aguarda a efetivação por quórum
        while self.running:
            async with self.lock:
                if self.commit_index >= new_index:
                    return raft_pb2.PublishResponse(success=True, leader_id=self.node_id, index=new_index)
                if self.role != "leader":
                    # Falha ocorreu antes do commit, cliente deve tentar com o novo líder
                    return raft_pb2.PublishResponse(success=False, leader_id=self.leader_id or "", index=-1)
            
            await asyncio.sleep(0.05)

    async def ConsumeData(self, request: raft_pb2.ConsumeRequest, context: grpc.aio.ServicerContext):
        async with self.lock:
            if self.role != "leader":
                return raft_pb2.ConsumeResponse(success=False, leader_id=self.leader_id or "", committed_data=[])
            
            # Requisito: Apenas dados efetivados (committed) poderão ser retornados.
            committed_data = await asyncio.to_thread(self.storage.get_committed_data)
            
            return raft_pb2.ConsumeResponse(
                success=True, 
                leader_id=self.leader_id or "", 
                committed_data=committed_data
            )

    # =========================================================================
    # INICIALIZAÇÃO
    # =========================================================================

    async def start(self):
        self.running = True
        self._reset_election_deadline()
        self.tasks.append(asyncio.create_task(self._election_timer_loop()))
        self.tasks.append(asyncio.create_task(self._heartbeat_loop()))
        
        server = aio.server()
        raft_pb2_grpc.add_ConsensusServiceServicer_to_server(self, server)
        raft_pb2_grpc.add_AppServiceServicer_to_server(self, server)
        
        port = self.host_port.split(':')[1]
        server.add_insecure_port(f'[::]:{port}')
        
        await server.start()
        self.logger.info(f"Servidor gRPC iniciado em {self.host_port}")
        
        await server.wait_for_termination()

    async def stop(self):
        self.running = False
        for task in self.tasks:
            task.cancel()

# Ponto de entrada do script
if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2 or sys.argv[1] not in NODE_CONFIGS:
        print(f"Uso: python raft_server.py [node1|node2|node3|node4]")
        sys.exit(1)
        
    node_id = sys.argv[1]
    node = RaftNode(node_id)
    
    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        asyncio.run(node.stop())
        print("\nServidor encerrado.")