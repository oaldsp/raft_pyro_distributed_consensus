const grpc = require('@grpc/grpc-js'); // Biblioteca gRPC para Node.js
const protoLoader = require('@grpc/proto-loader'); //Leitor de Protocol Buffers
const path = require('path');

// Carregamento do arquivo .proto
const PROTO_PATH = path.join(__dirname, 'raft.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true, // Mantem nome do jeito que está no Proto
    longs: String, // Converte longs para string
    enums: String, // Converte enums para string
    defaults: true, //  Define valores default para campos não preenchidos
    oneofs: true // Trata campos oneof como objetos
});
const raftProto = grpc.loadPackageDefinition(packageDefinition).raft;

// Mapeamento de nós (similar ao config.py do backend)
const NODE_CONFIGS = {
    "node1": "node1:5001",
    "node2": "node2:5002",
    "node3": "node3:5003",
    "node4": "node4:5004"
};

class RaftClient {
    constructor() {
        // Inicia tentando falar com o node1 arbitrariamente
        this.currentLeaderId = "node1"; 
        this.client = this.createGrpcClient(this.currentLeaderId);
    }

    createGrpcClient(nodeId) {
        const address = NODE_CONFIGS[nodeId];
        if (!address) throw new Error(`Nó desconhecido: ${nodeId}`);
        console.log(`[Cliente] Conectando ao nó: ${nodeId} (${address})`);
        return new raftProto.AppService(address, grpc.credentials.createInsecure());
    }

    handleRedirect(leaderId) {
        if (!leaderId || leaderId === "") {
            console.error("[Cliente] O cluster atualmente não possui um líder eleito. Aguardando...");
            return false;
        }
        console.log(`[Cliente] Redirecionando requisição para o líder conhecido: ${leaderId}`);
        this.currentLeaderId = leaderId;
        this.client.close();
        this.client = this.createGrpcClient(leaderId);
        return true;
    }

    publishData(command) {
        return new Promise((resolve, reject) => {
            const attempt = () => {
                this.client.PublishData({ command: command }, (error, response) => {
                    if (error) {
                        console.error(`[Cliente] Erro de comunicação RPC: ${error.message}`);
                        return reject(error);
                    }

                    if (response.success) {
                        console.log(`[Cliente] Sucesso! Comando '${command}' efetivado no índice ${response.index}.`);
                        resolve(response);
                    } else {
                        // Redireciona e tenta novamente caso não seja o líder
                        if (this.handleRedirect(response.leader_id)) {
                            setTimeout(attempt, 500); // Backoff simples
                        } else {
                            reject(new Error("Sem líder disponível no momento."));
                        }
                    }
                });
            };
            attempt();
        });
    }

    consumeData() {
        return new Promise((resolve, reject) => {
            const attempt = () => {
                this.client.ConsumeData({}, (error, response) => {
                    if (error) {
                        console.error(`[Cliente] Erro de comunicação RPC: ${error.message}`);
                        return reject(error);
                    }

                    // Retorna APENAS committed_data.
                    if (response.success) {
                        console.log(`[Cliente] Dados consumidos (Committed):`, response.committed_data);
                        resolve(response.committed_data);
                    } else if (response.leader_id) {
                        if (this.handleRedirect(response.leader_id)) {
                            setTimeout(attempt, 500);
                        } else {
                            reject(new Error("Falha ao consumir dados."));
                        }
                    }
                });
            };
            attempt();
        });
    }
}

// ---------------------------------------------------------
// Demonstração de Uso
// ---------------------------------------------------------
async function runDemo() {
    const client = new RaftClient();
    
    try {
        console.log("\n--- Cenário: Publicação de Dados ---");
        await client.publishData("set x=100");
        await client.publishData("set y=250");
        
        console.log("\n--- Cenário: Consumo de Dados ---");
        await client.consumeData();
        
    } catch (e) {
        console.error("Falha na demonstração:", e.message);
    }
}

runDemo();