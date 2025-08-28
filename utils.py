import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def load_spans() -> pd.DataFrame:
    """Load spans dataframe.

    Priority order:
    1. data/spans_long.parquet if available (faster).
    2. Parse local ``resultados_batch.jsonl`` output file.
    """
    parquet_path = Path("data/spans_long.parquet")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)

    jsonl_path = Path("resultados_batch.jsonl")
    if not jsonl_path.exists():
        raise FileNotFoundError("resultados_batch.jsonl não encontrado")

    rows = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            custom_id = obj.get("custom_id", "")
            codigo = None
            try:
                codigo = int(custom_id.split("-")[-1])
            except Exception:
                pass
            # Structured output is returned as JSON string inside text
            body = obj.get("response", {}).get("body", {})
            try:
                text_json = body["output"][1]["content"][0]["text"]
                parsed = json.loads(text_json)
            except Exception:
                parsed = {"spans": []}
            for span in parsed.get("spans", []):
                span["CodigoPronunciamento"] = codigo
                rows.append(span)
    df = pd.DataFrame(rows)
    return df


@st.cache_data(show_spinner=False)
def load_meta() -> pd.DataFrame:
    """Load discurso metadata.
    Expect a Parquet file ``data/discursos_meta.parquet`` with fields
    ``CodigoPronunciamento, Data, NomeParlamentar, SiglaPartidoParlamentarNaData,
    tamanho_discurso_palavras, TextoIntegral``.
    If the file is absent, an empty dataframe is returned.
    """
    meta_path = Path("data/discursos_meta.parquet")
    if meta_path.exists():
        return pd.read_parquet(meta_path)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame, f: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    if f.get("labels"):
        out = out[out["label"].isin(f["labels"])]
    if f.get("oradores"):
        out = out[out["NomeParlamentar"].isin(f["oradores"])]
    if f.get("partidos"):
        out = out[out["SiglaPartidoParlamentarNaData"].isin(f["partidos"])]
    if f.get("conf_min") is not None:
        out = out[out["confidence"] >= float(f["conf_min"])]
    if f.get("data_ini") and f.get("data_fim"):
        out = out[(out["Data"] >= f["data_ini"]) & (out["Data"] <= f["data_fim"])]
    if f.get("q"):
        out = out[out["text"].str.contains(f["q"], case=False, na=False)]
    return out


def to_density(df: pd.DataFrame) -> pd.DataFrame:
    if "peso" not in df:
        df = df.assign(peso=1000 / df["tamanho_discurso_palavras"].clip(lower=1))
    return df


# ---------------------------------------------------------------------------
# Text highlighting
# ---------------------------------------------------------------------------

def highlight_spans(text: str, spans: pd.DataFrame) -> str:
    """Return HTML with spans highlighted by label.

    Parameters
    ----------
    text: str
        Full speech text.
    spans: pd.DataFrame
        DataFrame with columns ``start_char``, ``end_char`` and ``label``.
    """
    import html

    if spans is None or spans.empty:
        return html.escape(text)

    spans = spans.sort_values("start_char")
    parts = []
    last = 0
    for _, row in spans.iterrows():
        start = int(row["start_char"])
        end = int(row["end_char"])
        label = row.get("label", "")
        parts.append(html.escape(text[last:start]))
        snippet = html.escape(text[start:end])
        parts.append(
            f'<mark class="label-{label}">{snippet}<span class="badge">{label}</span></mark>'
        )
        last = end
    parts.append(html.escape(text[last:]))
    return "".join(parts)
