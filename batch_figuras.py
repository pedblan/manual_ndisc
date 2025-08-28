#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch para análise de figuras de linguagem com OpenAI Responses API,
seguindo a estrutura do projeto (login() e schema importados).

Fluxo:
1) Lê data/Amostra_1.sqlite (tabela DiscursosAmostra)
2) Gera JSONL (um POST /v1/responses por linha) no formato:
   - model="gpt-5"
   - input = [developer + user] conforme tua função analisar_figuras()
   - text={"format": schema, "verbosity": "medium"}
   - reasoning={"effort": "medium"}, tools=[], store=True
3) Cria o batch (/v1/batches) e acompanha status
4) Baixa output.jsonl e parseia para Parquet (uma linha por span)

Requisitos:
  pip install openai pandas pyarrow
"""

from __future__ import annotations

import os
import json
import time
import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any, List

import pandas as pd

# Usa tua infra
from src.login_openai import login     # deve retornar um client compatível com OpenAI Python SDK
from src.structured_outputs import schema  # teu schema JSON para Structured Outputs

# Caminhos
SRC_DB = Path("data/Amostra_1.sqlite")
SRC_TABLE = "DiscursosAmostra"
OUT_DIR = Path("data/batch_figuras")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def iter_discursos(limit: int | None = None, seed: int | None = None, min_chars: int = 1) -> Iterable[Dict[str, Any]]:
    """
    Itera pelos discursos da tabela DiscursosAmostra.
    Embaralha com ORDER BY random() quando seed é fornecida (SQLite).
    """
    if not SRC_DB.exists():
        raise FileNotFoundError(f"Não encontrei {SRC_DB.resolve()}")

    conn = sqlite3.connect(str(SRC_DB))
    try:
        order_clause = " ORDER BY random() " if seed is not None else ""
        sql = f"""
        SELECT
            CodigoPronunciamento,
            NomeParlamentar,
            SiglaPartidoParlamentarNaData,
            DataPronunciamento,
            TextoIntegral
        FROM {SRC_TABLE}
        {order_clause}
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        cur = conn.execute(sql)
        cols = [c[0] for c in cur.description]
        for row in cur:
            rec = dict(zip(cols, row))
            texto = rec.get("TextoIntegral") or ""
            if len(texto) >= min_chars:
                yield rec
    finally:
        conn.close()


def build_request_body(model: str, discurso: str) -> Dict[str, Any]:
    """
    Monta o 'body' da requisição para /v1/responses exatamente como na tua função analisar_figuras().
    """
    return {
        "model": model,
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Você é um linguista que analisa figuras de linguagem em discursos no Senado."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Analise a seguinte fala:\n\n{discurso}"
                    }
                ]
            }
        ],
        "text": {
            "format": schema,
            "verbosity": "medium"
        },
        "reasoning": {
            "effort": "medium"
        },
        "tools": [],
        "store": True
    }


def create_jsonl(jsonl_path: Path, model: str, limit: int | None, seed: int | None, max_chars: int | None) -> int:
    """
    Cria o arquivo JSONL com uma linha por discurso no formato de batch.
    custom_id = disc-{CodigoPronunciamento}
    url = "/v1/responses"
    """
    n = 0
    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in iter_discursos(limit=limit, seed=seed, min_chars=1):
            codigo = rec["CodigoPronunciamento"]
            texto = rec.get("TextoIntegral") or ""

            if max_chars is not None and len(texto) > max_chars:
                texto = texto[:max_chars]

            body = build_request_body(model=model, discurso=texto)

            line = {
                "custom_id": f"disc-{codigo}",
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
            }
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
            n += 1
    return n


def create_and_run_batch(client, jsonl_path: Path, completion_window: str = "24h"):
    """
    Sobe o arquivo (purpose=batch) e cria o batch para /v1/responses.
    """
    uploaded = client.files.create(file=open(jsonl_path, "rb"), purpose="batch")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint="/v1/responses",
        completion_window=completion_window,
    )
    print(f"[OK] File id:   {uploaded.id}")
    print(f"[OK] Batch id:  {batch.id}")
    return batch


def wait_and_download(client, batch_id: str, out_dir: Path) -> Path | None:
    """
    Espera o batch finalizar e baixa o output.jsonl (se existir).
    """
    print(f"[INFO] Aguardando batch {batch_id} terminar...")
    while True:
        b = client.batches.retrieve(batch_id)
        print(f"  - status: {b.status}")
        if b.status in ("completed", "failed", "expired", "cancelled", "cancelling", "finalizing"):
            break
        time.sleep(5)

    if getattr(b, "output_file_id", None):
        file_out = client.files.retrieve(b.output_file_id)
        content = client.files.content(file_out.id).read()
        out_path = out_dir / f"{batch_id}_output.jsonl"
        out_path.write_bytes(content)
        print(f"[OK] Output salvo em: {out_path}")
        return out_path

    print("[AVISO] Sem arquivo de saída para baixar.")
    return None


def parse_output_to_parquet(output_jsonl: Path, parquet_path: Path):
    """
    Lê o output JSONL do batch e transforma em um Parquet COM UMA LINHA POR SPAN.
    - Se a resposta vier como JSON estruturado (output_json), explode os 'spans'.
    - Se vier como texto, guarda o texto bruto (coluna 'text').
    """
    linhas: List[Dict[str, Any]] = []

    with output_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            custom_id = obj.get("custom_id")
            resp = obj.get("response", {}) or {}

            try:
                output = resp["output"][0]["content"][0]  # bloco único esperado
                ctype = output.get("type")

                if ctype == "output_json":
                    payload = output["json"]  # deve ter {"spans":[...]}
                    spans = payload.get("spans", [])
                    # explode: 1 linha por span
                    for sp in spans:
                        linhas.append({
                            "custom_id": custom_id,
                            **sp
                        })
                elif ctype == "output_text":
                    linhas.append({
                        "custom_id": custom_id,
                        "text": output.get("text", "")
                    })
                else:
                    # tipo inesperado; guarda bruto
                    linhas.append({
                        "custom_id": custom_id,
                        "raw": output
                    })

            except Exception as e:
                linhas.append({
                    "custom_id": custom_id,
                    "parse_error": str(e),
                    "raw_response": obj
                })

    df = pd.DataFrame(linhas)
    df.to_parquet(parquet_path, index=False)
    print(f"[OK] Parquet salvo em: {parquet_path} | linhas: {len(df)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gpt-5", help="Modelo (ex.: gpt-5)")
    ap.add_argument("--limit", type=int, default=None, help="Limite de discursos a enviar")
    ap.add_argument("--seed", type=int, default=None, help="Seed p/ embaralhar no SELECT")
    ap.add_argument("--max-chars", type=int, default=None, help="Truncar TextoIntegral a N chars (opcional)")
    ap.add_argument("--completion-window", default="24h", help="Janela do batch (ex.: 24h)")
    args = ap.parse_args()

    jsonl_path = OUT_DIR / f"requests_{args.model}.jsonl"
    n = create_jsonl(jsonl_path, model=args.model, limit=args.limit, seed=args.seed, max_chars=args.max_chars)
    if n == 0:
        print("[AVISO] JSONL vazio. Nada a fazer.")
        return
    print(f"[OK] JSONL criado: {jsonl_path} ({n} requisições)")

    client = login()  # teu client já autenticado

    batch = create_and_run_batch(client, jsonl_path, completion_window=args.completion_window)
    out_jsonl = wait_and_download(client, batch.id, OUT_DIR)

    if out_jsonl:
        parquet_path = OUT_DIR / f"{batch.id}_spans.parquet"
        parse_output_to_parquet(out_jsonl, parquet_path)


if __name__ == "__main__":
    main()
