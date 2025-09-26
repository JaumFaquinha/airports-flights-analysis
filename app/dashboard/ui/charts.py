
# dashboard/ui/charts.py
from __future__ import annotations
import io
import polars as pl
import streamlit as st
from ..services.aggregations import _base_atraso, _agg_variacao_aeroporto

_USE_PLOTLY = True
try:
    import plotly.express as px
except Exception:
    _USE_PLOTLY = False
    import altair as alt  # type: ignore

from ..services.aggregations import (
    _agg_voos_por_dia, _agg_status, _agg_top_rotas
)

def render_kpis(df_filtrado: pl.DataFrame):
    total_voos = df_filtrado.height
    empresas_k = df_filtrado.select(
        pl.col("Empresa Aérea").n_unique() if "Empresa Aérea" in df_filtrado.columns else pl.lit(0)
    ).item()
    rotas_k = (
        df_filtrado.select(
            pl.concat_str([pl.col("ICAO Aeródromo Origem"), pl.lit("-"), pl.col("ICAO Aeródromo Destino")]).n_unique()
            if "ICAO Aeródromo Origem" in df_filtrado.columns and "ICAO Aeródromo Destino" in df_filtrado.columns
            else pl.lit(0)
        ).item()
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Voos (após filtros)", f"{total_voos:,}".replace(",", "."))
    c2.metric("Empresas distintas", f"{empresas_k:,}".replace(",", "."))
    c3.metric("Rotas distintas", f"{rotas_k:,}".replace(",", "."))
    st.markdown("---")

def render_general_charts(df_filtrado: pl.DataFrame):
    colA, colB = st.columns([2, 1])

    with colA:
        st.subheader("Voos por dia")
        ag = _agg_voos_por_dia(df_filtrado)
        if ag.height == 0:
            st.info("Sem dados suficientes para este gráfico.")
        else:
            pdf = ag.to_pandas()
            if _USE_PLOTLY:
                xcol = "Dia" if "Dia" in pdf.columns else "dia_str"
                fig = px.bar(pdf, x=xcol, y="Voos")
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=340)
                st.plotly_chart(fig, use_container_width=True)
            else:
                xcol = "Dia" if "Dia" in pdf.columns else "dia_str"
                chart = alt.Chart(pdf).mark_bar().encode(x=xcol, y="Voos").properties(height=340)
                st.altair_chart(chart, use_container_width=True)

    with colB:
        st.subheader("Distribuição por Status")
        ags = _agg_status(df_filtrado)
        if ags.height == 0:
            st.info("Sem dados de status.")
        else:
            pdf = ags.to_pandas()
            if _USE_PLOTLY:
                fig = px.pie(pdf, names="status", values="Quantidade", hole=0.30)
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=340, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                chart = alt.Chart(pdf).mark_arc().encode(theta="Quantidade", color="status").properties(height=340)
                st.altair_chart(chart, use_container_width=True)

    st.subheader("Top rotas (por volume)")
    ag_top = _agg_top_rotas(df_filtrado, topn=20)
    if ag_top.height == 0:
        st.info("Sem dados de rotas.")
    else:
        pdf = ag_top.to_pandas()
        if _USE_PLOTLY:
            fig = px.bar(pdf, x="Quantidade", y=pdf["origem"] + " ➔ " + pdf["destino"], orientation="h")
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=520)
            st.plotly_chart(fig, use_container_width=True)
        else:
            chart = (
                alt.Chart(pdf)
                .mark_bar()
                .encode(x="Quantidade", y=alt.Y("origem:N", sort="-x"))
                .properties(height=520)
            )
            st.altair_chart(chart, use_container_width=True)

    st.markdown("---")

def _compute_airport_variation_flex(df_delay: pl.DataFrame, df_no_date: pl.DataFrame, delay_limit: int):
    """
    Try to compute airport variation (delta). If only one year exists in the
    current date-filtered dataframe, re-run ignoring date filters but keeping
    other filters. Returns (increases, decreases, year_prev, year_curr, used_fallback).
    """
    inc, dec, y_prev, y_curr = _agg_variacao_aeroporto(df_delay)
    if y_prev and y_curr:
        return inc, dec, y_prev, y_curr, False

    # fallback ignoring date
    df_delay_all = _base_atraso(df_no_date, delay_limit)
    inc2, dec2, yp2, yc2 = _agg_variacao_aeroporto(df_delay_all)
    return inc2, dec2, yp2, yc2, True

def render_delay_insights(
    df_delay: pl.DataFrame,
    agg_aeroporto_mais_atrasos,
    agg_variacao_aeroporto,
    agg_atrasos_por_ano,
    agg_dias_semana_por_ano,
    agg_periodo_por_ano,
    agg_companhias_por_ano,
    *,
    df_filtered_no_date: pl.DataFrame,   # <-- NOVO (DF filtrado sem a restrição de data)
    delay_limit: int,                    # <-- NOVO (mesmo limite usado para gerar df_delay)
    base_delay_fn=None,                  # <-- NOVO (injete aqui sua função _base_atraso; se None, assume _base_atraso global)
):
    # permite injeção ou fallback para um nome global _base_atraso
    if base_delay_fn is None:
        try:
            base_delay_fn = _base_atraso  # type: ignore[name-defined]
        except NameError:
            raise RuntimeError(
                "Defina base_delay_fn (ex.: _base_atraso) ao chamar render_delay_insights "
                "ou garanta que _base_atraso esteja no escopo."
            )

    col1, col2 = st.columns([1.3, 1])

    # 1) Aeroportos com mais atrasos
    ag_aero = agg_aeroporto_mais_atrasos(df_delay)
    if ag_aero.height > 0:
        pdf = ag_aero.head(25).to_pandas()
        if _USE_PLOTLY:
            fig = px.bar(
                pdf,
                x="Quantidade Atrasos",
                y="Aeroporto",
                orientation="h",
                title="Aeroportos com mais atrasos (Top 25)",
            )
            fig.update_layout(height=520, margin=dict(l=0, r=0, t=40, b=0))
            col1.plotly_chart(fig, use_container_width=True)
        else:
            col1.dataframe(pdf)
    else:
        col1.info("Sem dados de atrasos para os filtros atuais.")

    # 2) Variação por aeroporto (com fallback automático 1C)
    aumentos, quedas, a1, a2, used_fallback = _compute_airport_variation_flex(
        df_delay=df_delay,
        df_no_date=df_filtered_no_date,
        delay_limit=delay_limit,
    )

    with col2:
        st.subheader("Variação por aeroporto")
        if a1 is None or a2 is None:
            st.info("Não foi possível identificar dois anos para comparar, mesmo fora do filtro de datas.")
        else:
            if used_fallback:
                st.caption(f"Comparando {a1} → {a2} (ignorando filtro de datas; demais filtros preservados)")
            else:
                st.caption(f"Comparando {a1} → {a2}")

            if _USE_PLOTLY and aumentos.height > 0:
                fig_up = px.bar(
                    aumentos.to_pandas(),
                    x="Delta",
                    y="Aeroporto",
                    orientation="h",
                    title="Maiores aumentos",
                )
                fig_up.update_layout(height=240, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_up, use_container_width=True)
            else:
                st.dataframe(aumentos.to_pandas())

            if _USE_PLOTLY and quedas.height > 0:
                fig_dn = px.bar(
                    quedas.to_pandas(),
                    x="Delta",
                    y="Aeroporto",
                    orientation="h",
                    title="Maiores quedas",
                )
                fig_dn.update_layout(height=240, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_dn, use_container_width=True)
            else:
                st.dataframe(quedas.to_pandas())

    st.markdown("---")

    # 3) Atrasos por ano
    ag_ano = agg_atrasos_por_ano(df_delay)
    if ag_ano.height > 0:
        pdf = ag_ano.to_pandas()
        if _USE_PLOTLY:
            fig = px.line(pdf, x="Ano", y="Quantidade Atrasos", markers=True, title="Total de atrasos por ano")
            fig.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(pdf)
    else:
        st.info("Sem dados anuais de atrasos.")

    # 4) Dias da semana por ano
    ag_sem = agg_dias_semana_por_ano(df_delay)
    if ag_sem.height > 0:
        ag_sem = (
            ag_sem
            .with_columns(pl.col("dia_sem").cast(pl.Int32).fill_null(0).mod(7).alias("_dow"))
            .with_columns(
                pl.when(pl.col("_dow") == 0).then(pl.lit("Seg"))
                 .when(pl.col("_dow") == 1).then(pl.lit("Ter"))
                 .when(pl.col("_dow") == 2).then(pl.lit("Qua"))
                 .when(pl.col("_dow") == 3).then(pl.lit("Qui"))
                 .when(pl.col("_dow") == 4).then(pl.lit("Sex"))
                 .when(pl.col("_dow") == 5).then(pl.lit("Sáb"))
                 .otherwise(pl.lit("Dom"))
                 .alias("Dia")
            )
            .drop("_dow")
        )
        pdf = ag_sem.to_pandas()
        if _USE_PLOTLY:
            fig = px.bar(pdf, x="Dia", y="Quantidade Atrasos", facet_col="Ano",
                         title="Dias da semana com mais atrasos (por ano)")
            fig.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(pdf)
    else:
        st.info("Sem dados por dia da semana.")

    # 5) Período do dia por ano
    ag_per = agg_periodo_por_ano(df_delay)
    if ag_per.height > 0:
        pdf = ag_per.to_pandas()
        if _USE_PLOTLY:
            fig = px.bar(pdf, x="periodo", y="Quantidade Atrasos", facet_col="Ano",
                         title="Período do dia com mais atrasos (por ano)")
            fig.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(pdf)
    else:
        st.info("Sem dados por período do dia.")

    # 6) Companhias por ano (Top N)
    ag_comp = agg_companhias_por_ano(df_delay)
    if ag_comp.height > 0:
        N = 15
        tops = (
            ag_comp
            .with_columns(pl.col("Quantidade Atrasos").rank("dense", descending=True).over("Ano").alias("rk"))
            .filter(pl.col("rk") <= N)
            .drop("rk")
        )
        pdf = tops.to_pandas()
        if _USE_PLOTLY:
            fig = px.bar(pdf, x="Quantidade Atrasos", y="empresa", facet_col="Ano",
                         orientation="h", title=f"Companhias que mais atrasam (Top {N} por ano)")
            fig.update_layout(height=520, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(pdf)
    else:
        st.info("Sem dados por companhia.")

def render_sample_and_downloads(df_filtrado: pl.DataFrame):
    st.markdown("---")
    st.subheader("Amostra dos dados filtrados")
    n = min(1000, df_filtrado.height)
    if n > 0:
        st.dataframe(df_filtrado.head(n).to_pandas(), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma linha após os filtros.")

    col1, col2 = st.columns(2)

    @st.cache_data(show_spinner=False)
    def _export_parquet_bytes(df: pl.DataFrame) -> bytes:
        buf = io.BytesIO()
        df.write_parquet(buf)
        return buf.getvalue()

    @st.cache_data(show_spinner=False)
    def _export_csv_bytes(df: pl.DataFrame) -> bytes:
        return df.write_csv().encode("utf-8")

    with col1:
        if st.button("Gerar arquivo Parquet (filtrado)"):
            data = _export_parquet_bytes(df_filtrado)
            st.download_button("Baixar Parquet", data=data, file_name="voos_filtrado.parquet",
                               mime="application/octet-stream")

    with col2:
        if st.button("Gerar arquivo CSV (filtrado)"):
            data = _export_csv_bytes(df_filtrado)
            st.download_button("Baixar CSV", data=data, file_name="voos_filtrado.csv", mime="text/csv")
