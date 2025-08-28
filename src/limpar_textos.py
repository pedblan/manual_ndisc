import sqlite3
from pathlib import Path
import re
from typing import Optional

# Caminho do banco
PATH_PRONUNCIAMENTOS_V2 = Path(__file__).resolve().parents[1] / "data" / "Discursos.sqlite"

# ------------------------------------------------------------
# Limpeza de anexos
# ------------------------------------------------------------
REGEX_EXCLUIR_ANEXOS = re.compile(
    r'(?:'
    r'\*\*\*\*'  # ****
    r'|["“”]?\s*SEGUE,\s+NA\s+ÍNTEGRA,\s+PRONUNCIAMENTO["“”]?'
    r'|["“”]?\s*SEGUE\s+NA\s+ÍNTEGRA\s+PRONUNCIAMENTO["“”]?'
    r'|["“”]?\s*DOCUMENTO\s+ENCAMINHADO\s+PEL[OA]["“”]?'
    r').*',
    flags=re.IGNORECASE | re.DOTALL
)

def limpar_texto_anexos(texto: Optional[str]) -> str:
    """Remove anexos (conteúdo após marcadores conhecidos) de um texto."""
    if not isinstance(texto, str):
        return ""
    return REGEX_EXCLUIR_ANEXOS.sub("", texto).strip()

# ------------------------------------------------------------
# Utilitários SQLite
# ------------------------------------------------------------
def _count_rows(conn: sqlite3.Connection, table: str, where: Optional[str]) -> int:
    sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
    cur = conn.execute(sql)
    return int(cur.fetchone()[0])

def _yield_batches(conn: sqlite3.Connection, table: str, id_col: str, text_col: str,
                   where: Optional[str], batch_size: int):
    """
    Gera lotes de (id, texto) usando um cursor com fetchmany().
    Evita usar OFFSET para não degradar em tabelas grandes.
    Requer que id_col seja chave única/primária.
    """
    # Podemos iterar ordenando pelo id para cursor estável
    base_sql = f"SELECT {id_col}, {text_col} FROM {table}"
    if where:
        base_sql += f" WHERE {where}"
    base_sql += f" ORDER BY {id_col} ASC"

    cur = conn.execute(base_sql)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows

# ------------------------------------------------------------
# Pipeline principal
# ------------------------------------------------------------
def limpar_coluna_sqlite(
    db_path: Path,
    table: str,
    id_col: str,
    text_col: str,
    where: Optional[str] = None,
    batch_size: int = 5_000,
    commit_every: int = 20_000,
    pragmas: bool = True,
) -> dict:
    """
    Limpa anexos na coluna `text_col` de `table` dentro do SQLite.
    Atualiza apenas quando o texto for alterado.

    Parâmetros
    ----------
    db_path : Path
        Caminho do arquivo .sqlite.
    table : str
        Nome da tabela.
    id_col : str
        Coluna identificadora única (PRIMARY KEY ou UNIQUE).
    text_col : str
        Coluna de texto a ser limpa.
    where : str | None
        Filtro opcional (ex.: "Data BETWEEN '2007-01-01' AND '2024-12-31'").
    batch_size : int
        Tamanho do lote de leitura.
    commit_every : int
        Faz commit a cada N atualizações aplicadas.
    pragmas : bool
        Se True, ajusta PRAGMAs para melhor performance.

    Retorna
    -------
    dict com contagens: {'verificadas', 'atualizadas', 'inalteradas'}
    """
    conn = sqlite3.connect(str(db_path))
    try:
        if pragmas:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA mmap_size=134217728;")  # 128 MiB

        total = _count_rows(conn, table, where)
        verificadas = atualizadas = inalteradas = 0
        pendentes = 0

        # Recomenda-se um índice (se ainda não houver) para acelerar o ORDER BY/UPDATE:
        # conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{id_col} ON {table}({id_col});")

        for rows in _yield_batches(conn, table, id_col, text_col, where, batch_size):
            updates = []
            for _id, txt in rows:
                verificadas += 1
                novo = limpar_texto_anexos(txt)
                if (txt or "") != novo:
                    updates.append((novo, _id))
                else:
                    inalteradas += 1

            if updates:
                conn.executemany(
                    f"UPDATE {table} SET {text_col} = ? WHERE {id_col} = ?;",
                    updates
                )
                atualizadas += len(updates)
                pendentes += len(updates)

            if pendentes >= commit_every:
                conn.commit()
                pendentes = 0

        if pendentes:
            conn.commit()

        return {
            "linhas_totais_filtradas": total,
            "verificadas": verificadas,
            "atualizadas": atualizadas,
            "inalteradas": inalteradas,
        }
    finally:
        conn.close()

# ------------------------------------------------------------
# Exemplo de uso
# ------------------------------------------------------------
if __name__ == "__main__":
    stats = limpar_coluna_sqlite(
        db_path=PATH_PRONUNCIAMENTOS_V2,
        table="Discursos",            # ajuste para o nome real
        id_col="CodigoPronunciamento",# ajuste para a PK/única
        text_col="TextoIntegral",     # ajuste para a coluna de texto
        where=None,                   # ou, por exemplo: "Data BETWEEN '2007-01-01' AND '2024-12-31'"
        batch_size=5000,
        commit_every=20000,
    )
    print(stats)
