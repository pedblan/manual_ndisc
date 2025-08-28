#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amostragem estratificada (1%) por partido, com piso de 1 quando N<100,
a partir de data/Discursos.sqlite → data/Amostra_1.sqlite.
Adiciona NomeParlamentar (join em data/Senadores.sqlite pela chave CodigoParlamentar).

Critério de inclusão: TextoIntegral com mais de 200 palavras (aprox. por contagem de espaços em SQL).

Uso:
    python amostrar_discursos.py --seed 42
"""

from __future__ import annotations

import argparse
import math
import random
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

SRC_DISCURSOS = Path("data/Discursos.sqlite")
SRC_SENADORES = Path("data/Senadores.sqlite")
DEST_DB = Path("Amostra_1.sqlite")

TBL_DISCURSOS = "Discursos"
TBL_SAIDA = "DiscursosAmostra"
BATCH_SIZE = 800  # tamanho do IN (...) por batch


SQL_WORDS_FILTER = f"""
-- Aproximação de contagem de palavras: (nº de espaços + 1) em texto "normalizado"
WITH base AS (
  SELECT
    CodigoPronunciamento,
    SiglaPartidoParlamentarNaData,
    CodigoParlamentar,
    TRIM(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(COALESCE(TextoIntegral, ''), CHAR(10), ' '),  -- \n
          CHAR(13), ' '),                                         -- \r
        CHAR(9), ' '),                                            -- \t
      '  ', ' ')                                                  -- *redução parcial; ainda pode sobrar duplo espaço
    ) AS T
  FROM {TBL_DISCURSOS}
)
SELECT
  CodigoPronunciamento,
  COALESCE(SiglaPartidoParlamentarNaData, 'SEM_PARTIDO') AS SiglaPartidoParlamentarNaData,
  CodigoParlamentar
FROM base
WHERE
  -- conta de palavras ≈ espaços + 1
  (LENGTH(T) - LENGTH(REPLACE(T, ' ', ''))) + 1 > 200;
"""


def tamanho_amostra_por_partido(n: int) -> int:
    if n >= 100:
        return max(1, math.ceil(0.01 * n))
    return 1 if n > 0 else 0


def coletar_elegiveis(conn_disc: sqlite3.Connection) -> Dict[str, List[Tuple[int, int | None]]]:
    """
    Retorna {partido: [(CodigoPronunciamento, CodigoParlamentar), ...]} apenas dos discursos >200 palavras.
    """
    partidos: Dict[str, List[Tuple[int, int | None]]] = {}
    cur = conn_disc.execute(SQL_WORDS_FILTER)
    total = 0
    for cod, partido, cod_parl in cur:
        partidos.setdefault(partido, []).append((cod, cod_parl))
        total += 1
    print(f"[INFO] Discursos elegíveis (>200 palavras): {total:,}")
    print(f"[INFO] Partidos com pelo menos 1 elegível: {len(partidos)}")
    return partidos


def amostrar_por_partido(
    elegiveis: Dict[str, List[Tuple[int, int | None]]],
    *,
    seed: int | None = None,
) -> List[int]:
    if seed is not None:
        random.seed(seed)
    amostra_ids: List[int] = []
    for partido, linhas in elegiveis.items():
        n = len(linhas)
        k = tamanho_amostra_por_partido(n)
        if k == 0:
            continue
        escolhidos = random.sample(linhas, min(k, n))
        amostra_ids.extend([cod for cod, _ in escolhidos])
    print(f"[INFO] Total de discursos na amostra: {len(amostra_ids):,}")
    return amostra_ids


def preparar_destino(conn_dest: sqlite3.Connection):
    conn_dest.execute(f"DROP TABLE IF EXISTS {TBL_SAIDA};")
    conn_dest.commit()


def copiar_amostra_com_join(
    conn_disc: sqlite3.Connection,
    conn_sen: sqlite3.Connection,
    conn_dest: sqlite3.Connection,
    ids: List[int],
):
    """
    Cria {TBL_SAIDA} no destino com todas as colunas de Discursos + NomeParlamentar (LEFT JOIN Senadores).
    Faz em batches para não estourar o IN (...).
    """
    preparar_destino(conn_dest)

    # Cria tabela destino com o mesmo schema de Discursos + NomeParlamentar.
    # Como schemas podem variar, criaremos via SELECT INTO (CTAS) no primeiro batch e depois INSERT nos demais.
    primeiro_batch = True
    for i in range(0, len(ids), BATCH_SIZE):
        lote = ids[i : i + BATCH_SIZE]
        placeholders = ",".join("?" for _ in lote)
        # Usamos ATTACH para ler de duas origens no mesmo execute.
        conn_disc.execute("ATTACH DATABASE ? AS sen_db;", (str(SRC_SENADORES),))
        conn_disc.execute("ATTACH DATABASE ? AS dest_db;", (str(DEST_DB),))

        sql_select_join = f"""
            SELECT d.*, s.NomeParlamentar
            FROM {TBL_DISCURSOS} d
            LEFT JOIN sen_db.Senadores s
              ON s.CodigoParlamentar = d.CodigoParlamentar
            WHERE d.CodigoPronunciamento IN ({placeholders})
        """

        if primeiro_batch:
            # CTAS
            ct_sql = f"""
                CREATE TABLE dest_db.{TBL_SAIDA} AS
                {sql_select_join}
            """
            conn_disc.execute(ct_sql, lote)
            primeiro_batch = False
        else:
            ins_sql = f"""
                INSERT INTO dest_db.{TBL_SAIDA}
                {sql_select_join}
            """
            conn_disc.execute(ins_sql, lote)

        conn_disc.execute("DETACH DATABASE sen_db;")
        conn_disc.execute("DETACH DATABASE dest_db;")
        conn_disc.commit()

    print(f"[OK] Tabela '{TBL_SAIDA}' criada em {DEST_DB} (com NomeParlamentar).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=None, help="Seed para reprodutibilidade")
    args = parser.parse_args()

    if not SRC_DISCURSOS.exists():
        raise FileNotFoundError(f"Não encontrei {SRC_DISCURSOS.resolve()}")
    if not SRC_SENADORES.exists():
        raise FileNotFoundError(f"Não encontrei {SRC_SENADORES.resolve()}")

    DEST_DB.parent.mkdir(parents=True, exist_ok=True)
    if DEST_DB.exists():
        DEST_DB.unlink()

    # Conexões
    conn_disc = sqlite3.connect(str(SRC_DISCURSOS))
    conn_sen = sqlite3.connect(str(SRC_SENADORES))
    conn_dest = sqlite3.connect(str(DEST_DB))

    try:
        elegiveis = coletar_elegiveis(conn_disc)
        ids = amostrar_por_partido(elegiveis, seed=args.seed)

        if not ids:
            print("[AVISO] Nenhum discurso elegível encontrado para amostrar.")
            return

        copiar_amostra_com_join(conn_disc, conn_sen, conn_dest, ids)
    finally:
        conn_disc.close()
        conn_sen.close()
        conn_dest.close()


if __name__ == "__main__":
    main()
