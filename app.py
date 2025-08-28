import datetime as dt
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from utils import load_spans, load_meta, apply_filters, to_density, highlight_spans

st.set_page_config(page_title="Figuras de Linguagem — Senado", layout="wide")

# CSS for highlighted spans -------------------------------------------------
st.markdown(
    """
    <style>
    .badge {background-color:rgba(0,0,0,0.2); padding:0 4px; margin-left:4px; border-radius:4px; font-size:0.8em;}
    mark.label-metafora {background-color:#3B82F6; color:white;}
    mark.label-ironia {background-color:#F59E0B; color:white;}
    mark.label-anafora {background-color:#10B981; color:white;}
    mark.label-antitese {background-color:#EF4444; color:white;}
    mark.label-hiperbole {background-color:#8B5CF6; color:white;}
    mark.label-analogia {background-color:#06B6D4; color:white;}
    mark.label-eufemismo {background-color:#EAB308; color:white;}
    mark.label-metonimia {background-color:#6366F1; color:white;}
    </style>
    """,
    unsafe_allow_html=True,
)

# Load data ---------------------------------------------------------------
spans = load_spans()
meta = load_meta()
if not meta.empty:
    spans = spans.merge(meta, on="CodigoPronunciamento", how="left")
if "Data" in spans:
    spans["Data"] = pd.to_datetime(spans["Data"])
    spans["ano_mes"] = spans["Data"].dt.to_period("M").astype(str)
if "tamanho_discurso_palavras" not in spans:
    spans["tamanho_discurso_palavras"] = 1
spans = to_density(spans)

# Query params -----------------------------------------------------------
params = st.experimental_get_query_params()

# defaults
min_date = spans["Data"].min() if "Data" in spans else dt.date.today()
max_date = spans["Data"].max() if "Data" in spans else dt.date.today()

filters = {
    "labels": params.get("labels", []),
    "oradores": params.get("oradores", []),
    "partidos": params.get("partidos", []),
    "data_ini": params.get("data_ini", [min_date])[0],
    "data_fim": params.get("data_fim", [max_date])[0],
    "conf_min": float(params.get("conf_min", [0])[0]) if params.get("conf_min") else 0.0,
    "normalizado": params.get("normalizado", ["0"])[0] == "1",
    "q": params.get("q", [""])[0],
    "page": params.get("page", ["Panorama"])[0],
}

# Sidebar ---------------------------------------------------------------
with st.sidebar:
    st.title("Figuras de Linguagem")
    page = st.radio("Página", ["Panorama", "Explorar", "Discurso"], index=["Panorama", "Explorar", "Discurso"].index(filters["page"]))

    st.markdown("**Filtros**")
    labels = st.multiselect("Tipo de figura", sorted(spans["label"].dropna().unique()), default=filters["labels"])
    oradores = st.multiselect(
        "Orador",
        sorted(spans.get("NomeParlamentar", pd.Series(dtype=str)).dropna().unique()),
        default=filters["oradores"],
    )
    partidos = st.multiselect(
        "Partido",
        sorted(spans.get("SiglaPartidoParlamentarNaData", pd.Series(dtype=str)).dropna().unique()),
        default=filters["partidos"],
    )
    data_ini, data_fim = st.date_input(
        "Período",
        value=(pd.to_datetime(filters["data_ini"]).date(), pd.to_datetime(filters["data_fim"]).date()),
    )
    conf_min = st.slider("Confiança mínima", 0.0, 1.0, filters["conf_min"], step=0.05)
    normalizado = st.checkbox("Mostrar valores por 1000 palavras", value=filters["normalizado"])
    q = st.text_input("Busca textual", value=filters["q"])

    btn1, btn2 = st.columns(2)
    with btn1:
        if st.button("Aplicar"):
            new_params = {
                "labels": labels,
                "oradores": oradores,
                "partidos": partidos,
                "data_ini": data_ini.isoformat(),
                "data_fim": data_fim.isoformat(),
                "conf_min": conf_min,
                "normalizado": "1" if normalizado else "0",
                "q": q,
                "page": page,
            }
            params = st.query_params
            st.query_params.clear()  # para resetar
            st.query_params.update(new_params)  # para aplicar
    with btn2:
        if st.button("Resetar filtros"):
            st.experimental_set_query_params(page=page)
            st.experimental_rerun()

# apply filters
filter_dict = {
    "labels": labels,
    "oradores": oradores,
    "partidos": partidos,
    "data_ini": pd.to_datetime(data_ini),
    "data_fim": pd.to_datetime(data_fim),
    "conf_min": conf_min,
    "q": q,
}

df_filt = apply_filters(spans, filter_dict)

if normalizado:
    df_plot = df_filt.copy()
else:
    df_plot = df_filt.assign(peso=1)

# Glossary --------------------------------------------------------------
GLOSSARIO = {
    "metafora": "Transferência de significado por comparação implícita.",
    "ironia": "Expressão de sentido contrário ao literal.",
    "hiperbole": "Exagero intencional.",
}
with st.sidebar.expander("Glossário"):
    for k, v in GLOSSARIO.items():
        st.markdown(f"**{k}** — {v}")

# Pages -----------------------------------------------------------------
if page == "Panorama":
    st.subheader("Panorama")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("# discursos", int(df_filt["CodigoPronunciamento"].nunique()))
    col2.metric("# spans", int(len(df_filt)))
    col3.metric("# oradores", int(df_filt.get("NomeParlamentar", pd.Series()).nunique()))
    col4.metric("# partidos", int(df_filt.get("SiglaPartidoParlamentarNaData", pd.Series()).nunique()))

    if not df_plot.empty and "ano_mes" in df_plot:
        serie = (
            df_plot.groupby(["ano_mes", "label"], as_index=False)["peso"].sum()
        )
        fig = px.area(serie, x="ano_mes", y="peso", color="label")
        fig.update_layout(xaxis_title="Mês", yaxis_title="Spans" if not normalizado else "Spans/1000 palavras")
        st.plotly_chart(fig, use_container_width=True)

    if not df_plot.empty and "SiglaPartidoParlamentarNaData" in df_plot:
        heat = (
            df_plot.groupby(["SiglaPartidoParlamentarNaData", "label"], as_index=False)["peso"].sum()
        )
        pivot = heat.pivot(index="label", columns="SiglaPartidoParlamentarNaData", values="peso").fillna(0)
        fig2 = px.imshow(pivot, aspect="auto", color_continuous_scale="Blues")
        st.plotly_chart(fig2, use_container_width=True)

    if not df_plot.empty and "NomeParlamentar" in df_plot:
        top = (
            df_plot.groupby("NomeParlamentar", as_index=False)["peso"].sum().sort_values("peso", ascending=False).head(10)
        )
        fig3 = px.bar(top, x="peso", y="NomeParlamentar", orientation="h")
        st.plotly_chart(fig3, use_container_width=True)

    exemplos = df_filt.head(6)
    for _, row in exemplos.iterrows():
        st.markdown(
            f"**{row.get('NomeParlamentar', '')} ({row.get('SiglaPartidoParlamentarNaData', '')}) — {row.get('Data', '')}**"
        )
        st.markdown(f"<span class='badge'>{row['label']}</span> {row['text']}", unsafe_allow_html=True)
        st.markdown("---")

elif page == "Explorar":
    st.subheader("Explorar")
    cols = [
        c
        for c in [
            "Data",
            "NomeParlamentar",
            "SiglaPartidoParlamentarNaData",
            "label",
            "text",
            "confidence",
            "CodigoPronunciamento",
        ]
        if c in df_filt.columns
    ]
    st.dataframe(df_filt[cols], use_container_width=True)
    csv = df_filt[cols].to_csv(index=False).encode("utf-8")
    st.download_button("Exportar CSV", csv, "spans.csv", "text/csv")

    # Mini charts
    if not df_plot.empty:
        treemap = df_plot.groupby("label", as_index=False)["peso"].sum()
        figt = px.treemap(treemap, path=["label"], values="peso")
        st.plotly_chart(figt, use_container_width=True)

        ranking = (
            df_plot.groupby(["ano_mes", "label"], as_index=False)["peso"].sum()
            .sort_values(["ano_mes", "peso"], ascending=[True, False])
        )
        ranking["rank"] = ranking.groupby("ano_mes")["peso"].rank("dense", ascending=False)
        figb = px.line(ranking, x="ano_mes", y="rank", color="label")
        figb.update_yaxes(autorange="reversed")
        st.plotly_chart(figb, use_container_width=True)

        if "SiglaPartidoParlamentarNaData" in df_plot:
            dens = (
                df_plot.groupby(["SiglaPartidoParlamentarNaData"], as_index=False)["peso"].sum()
            )
            figd = px.scatter(dens, x="peso", y="SiglaPartidoParlamentarNaData")
            st.plotly_chart(figd, use_container_width=True)

elif page == "Discurso":
    st.subheader("Discurso")
    codigo = params.get("codigo", [None])[0]
    if codigo is not None:
        try:
            codigo = int(codigo)
        except Exception:
            codigo = None
    if codigo is None:
        st.info("Selecione um discurso na página Explorar.")
    else:
        meta_row = meta[meta["CodigoPronunciamento"] == codigo]
        spans_disc = spans[spans["CodigoPronunciamento"] == codigo]
        if meta_row.empty:
            st.warning("Discurso não encontrado nos metadados.")
        else:
            rec = meta_row.iloc[0]
            st.markdown(f"### {rec['NomeParlamentar']} ({rec['SiglaPartidoParlamentarNaData']})")
            st.caption(str(rec['Data']))
            st.markdown(highlight_spans(rec.get("TextoIntegral", ""), spans_disc), unsafe_allow_html=True)

            resumo = spans_disc.groupby("label").agg(spans=("label", "count"))
            resumo["densidade"] = resumo["spans"] * (1000 / rec.get("tamanho_discurso_palavras", 1))
            st.table(resumo)
