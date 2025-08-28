# -*- coding: utf-8 -*-
# Amostragem estratificada por ano + contagem de tokens e estimativa de custos (gpt-5)

import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np

# tiktoken para contagem de tokens
try:
    import tiktoken
except ImportError as e:
    raise ImportError("Instale tiktoken: pip install tiktoken") from e


# ---------------------------
# PREÇOS (USD por 1M tokens)
# ---------------------------
PRICES_PER_MILLION = {
    "gpt-5": {
        "input": 0.625,          # $ por 1M tokens (input)
        "cached_input": 0.0625,  # $ por 1M tokens (cached input)
        "output": 5.00           # $ por 1M tokens (output)
    }
}


def _get_encoding(model_name: str):
    """
    Obtém o encoding do tiktoken.
    Se o modelo não estiver mapeado, cai no 'cl100k_base'.
    """
    try:
        return tiktoken.encoding_for_model(model_name)
    except Exception:
        # Fallback robusto para modelos GPT modernos
        return tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str, enc) -> int:
    """Conta tokens de uma string usando o encoding fornecido."""
    if not isinstance(text, str):
        text = "" if text is None else str(text)
    # Evita estouro por strings gigantes; ajuste se necessário
    return len(enc.encode(text))


def _ensure_fraction(value: float) -> float:
    """
    Converte percentuais >1 (ex.: 10 para 10%) em fração (0.10).
    Mantém frações já entre (0,1].
    """
    if value <= 0:
        raise ValueError("pct_per_year deve ser > 0.")
    return value / 100.0 if value > 1 else value


def sample_discursos_by_year(
    db_path: str | Path = "data/DiscursosV2.sqlite",
    table: str = "Discursos",
    pct_per_year: float = 0.10,       # 10% por ano
    min_words: int = 200,
    seed: int = 42,
    prompt: str = "",
    model: str = "gpt-5",
    estimate_output_tokens_per_item: int | None = None,
    prompt_cached: bool = True,
    extra_columns: list[str] | None = None,
):
    """
    Amostra discursos por ano e estima custos de processamento no modelo gpt-5.

    Parâmetros:
    - db_path: caminho para o SQLite.
    - table: nome da tabela (espera colunas: Data, TextoIntegral).
    - pct_per_year: fração por ano (0.10 = 10%). Se passado como 10, será convertido para 0.10.
    - min_words: mínimo de palavras do TextoIntegral para filtrar.
    - seed: semente do sorteio.
    - prompt: prompt que será enviado com cada texto (para contar tokens do prompt).
    - model: nome do modelo (usa preços PRICES_PER_MILLION['gpt-5']).
    - estimate_output_tokens_per_item: estimativa de tokens de saída por item (opcional).
    - prompt_cached: se True, precifica tokens do prompt como cached_input.
    - extra_columns: colunas adicionais a carregar (ex.: ['CodigoPronunciamento']).

    Retorna:
    - df_sample: DataFrame com amostra e colunas de tokens e custos.
    - df_summary_year: custo e contagens agregadas por ano.
    - df_summary_total: linha única com totais.
    """
    pct = _ensure_fraction(pct_per_year)

    # Colunas mínimas necessárias
    base_cols = ["Data", "TextoIntegral"]
    if extra_columns:
        cols = base_cols + [c for c in extra_columns if c not in base_cols]
    else:
        cols = base_cols

    conn = sqlite3.connect(str(db_path))
    try:
        # Leitura das colunas necessárias
        query = f"SELECT {', '.join(cols)} FROM {table}"
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    # Parse de datas e Ano
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Data"])
    df["Ano"] = df["Data"].dt.year.astype(int)

    # Filtro por número mínimo de palavras
    # (contagem simples de palavras por espaços; ajuste se quiser usar regex)
    word_counts = df["TextoIntegral"].fillna("").astype(str).str.split().map(len)
    df = df.loc[word_counts > min_words].copy()

    if df.empty:
        raise ValueError("Após o filtro de palavras mínimas, não há discursos para amostrar.")

    # Amostragem por ano
    rng = np.random.default_rng(seed)
    def _sample_group(g):
        n = max(1, int(np.floor(len(g) * pct)))
        # amostragem estável: usa permutation do numpy sobre o índice
        idx = rng.choice(g.index.values, size=n, replace=False)
        return g.loc[idx]

    df_sample = df.groupby("Ano", group_keys=False).apply(_sample_group).reset_index(drop=True)

    # Contagem de tokens
    enc = _get_encoding(model)
    df_sample["n_tokens_texto"] = df_sample["TextoIntegral"].apply(lambda x: count_tokens(x, enc))
    n_tokens_prompt = count_tokens(prompt, enc)

    # Custos (USD por item)
    try:
        price_input = PRICES_PER_MILLION[model]["input"]
        price_cached = PRICES_PER_MILLION[model]["cached_input"]
        price_output = PRICES_PER_MILLION[model]["output"]
    except KeyError:
        raise KeyError(f"Modelo '{model}' não definido em PRICES_PER_MILLION.")

    # Tokens de input: prompt + texto (prompt opcionalmente como cached)
    df_sample["n_tokens_prompt"] = n_tokens_prompt
    df_sample["n_tokens_input_total"] = df_sample["n_tokensPrompt_calc"] = (
        df_sample["n_tokens_prompt"] + df_sample["n_tokens_texto"]
    )

    # Custo de input (separando prompt como cached vs input normal)
    cost_per_token_input = price_input / 1_000_000.0
    cost_per_token_cached = price_cached / 1_000_000.0

    if prompt_cached and n_tokens_prompt > 0:
        # prompt cobrado como cached_input + texto como input
        df_sample["est_custo_input_usd"] = (
            df_sample["n_tokens_texto"] * cost_per_token_input
            + df_sample["n_tokens_prompt"] * cost_per_token_cached
        )
    else:
        # tudo como input normal
        df_sample["est_custo_input_usd"] = (
            df_sample["n_tokens_input_total"] * cost_per_token_input
        )

    # Custo de output (opcional, estimado)
    if estimate_output_tokens_per_item is not None and estimate_output_tokens_per_item >= 0:
        cost_per_token_output = price_output / 1_000_000.0
        df_sample["est_output_tokens"] = int(estimate_output_tokens_per_item)
        df_sample["est_custo_output_usd"] = df_sample["est_output_tokens"] * cost_per_token_output
    else:
        df_sample["est_output_tokens"] = 0
        df_sample["est_custo_output_usd"] = 0.0

    # Total por item
    df_sample["est_custo_total_usd"] = df_sample["est_custo_input_usd"] + df_sample["est_custo_output_usd"]

    # Resumos
    df_summary_year = (
        df_sample.groupby("Ano")
        .agg(
            n_itens=("TextoIntegral", "size"),
            tokens_texto=("n_tokens_texto", "sum"),
            tokens_prompt=("n_tokens_prompt", "sum"),
            tokens_input_total=("n_tokens_input_total", "sum"),
            custo_input_usd=("est_custo_input_usd", "sum"),
            custo_output_usd=("est_custo_output_usd", "sum"),
            custo_total_usd=("est_custo_total_usd", "sum"),
        )
        .reset_index()
        .sort_values("Ano")
    )

    df_summary_total = pd.DataFrame(
        [{
            "n_itens": int(df_summary_year["n_itens"].sum()),
            "tokens_texto": int(df_summary_year["tokens_texto"].sum()),
            "tokens_prompt": int(df_summary_year["tokens_prompt"].sum()),
            "tokens_input_total": int(df_summary_year["tokens_input_total"].sum()),
            "custo_input_usd": float(df_summary_year["custo_input_usd"].sum()),
            "custo_output_usd": float(df_summary_year["custo_output_usd"].sum()),
            "custo_total_usd": float(df_summary_year["custo_total_usd"].sum()),
        }]
    )

    return df_sample, df_summary_year, df_summary_total


#