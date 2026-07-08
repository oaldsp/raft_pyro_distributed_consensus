import sqlite3
import json
from typing import List, Tuple, Optional

class RaftStorage:
    def __init__(self, node_id: str):
        self.db_path = f"{node_id}.db"
        self._init_db()

    def _init_db(self):
        """Cria as tabelas de estado estável e logs de forma idempotente."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Tabela de estado (Sempre terá apenas 1 linha com id=1)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_term INTEGER NOT NULL,
                    voted_for TEXT
                )
            ''')
            # Tabela de logs (Diferencia dados uncommitted e committed)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS log (
                    log_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    command TEXT NOT NULL,
                    is_committed BOOLEAN NOT NULL DEFAULT 0
                )
            ''')
            
            # Inicializa a linha de estado se o banco for novo
            cursor.execute('INSERT OR IGNORE INTO state (id, current_term, voted_for) VALUES (1, 0, NULL)')
            conn.commit()

    # =========================================================================
    # ESTADO ESTÁVEL (Term e VotedFor)
    # =========================================================================

    def load_state(self) -> Tuple[int, Optional[str]]:
        """Recupera o estado salvo após uma falha/reinicialização."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT current_term, voted_for FROM state WHERE id = 1')
            row = cursor.fetchone()
            return row if row else (0, None)

    def save_state(self, current_term: int, voted_for: Optional[str]):
        """Persiste o termo e o voto. Deve ser chamado antes de responder a RPCs."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE state 
                SET current_term = ?, voted_for = ? 
                WHERE id = 1
            ''', (current_term, voted_for))
            conn.commit()

    # =========================================================================
    # GERENCIAMENTO DE LOGS (Uncommitted e Committed)
    # =========================================================================

    def get_last_log_info(self) -> Tuple[int, int]:
        """Retorna (ultimo_indice, ultimo_termo) para requisições de voto e heartbeats."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT log_index, term FROM log ORDER BY log_index DESC LIMIT 1')
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (0, 0)

    def append_entry(self, log_index: int, term: int, command: str):
        """Insere um novo dado (inicialmente uncommitted)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO log (log_index, term, command, is_committed)
                VALUES (?, ?, ?, 0)
            ''', (log_index, term, command))
            conn.commit()

    def truncate_log_from(self, start_index: int):
        """Remove entradas conflitantes caso o líder envie um log divergente."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM log WHERE log_index >= ?', (start_index,))
            conn.commit()

    def commit_logs_up_to(self, commit_index: int):
        """Marca logs como efetivados (committed) até o índice especificado."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE log SET is_committed = 1 WHERE log_index <= ?', (commit_index,))
            conn.commit()

    def get_log_term(self, index: int) -> int:
        """Obtém o termo de um log específico (usado na validação do AppendEntries)."""
        if index == 0: return 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT term FROM log WHERE log_index = ?', (index,))
            row = cursor.fetchone()
            return row[0] if row else 0

    def get_committed_data(self) -> List[str]:
        """Retorna apenas dados efetivados para o cliente (Requisito de Leitura)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT command FROM log WHERE is_committed = 1 ORDER BY log_index ASC')
            return [row[0] for row in cursor.fetchall()]
    
    def get_entries_from(self, start_index: int) -> List[Tuple[int, str]]:
        """Retorna uma lista de tuplas (termo, comando) a partir de um índice (inclusivo)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT term, command 
                FROM log 
                WHERE log_index >= ? 
                ORDER BY log_index ASC
            ''', (start_index,))
            return cursor.fetchall()