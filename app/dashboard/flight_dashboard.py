# dashboard/voos_dashboard.py
from __future__ import annotations
from typing import Optional
import polars as pl
import streamlit as st

from .services.io import (
    _load_parquet_from_path,
    _load_parquet_from_bytes,
    _unique_values,
    _date_bounds,
)
from .services.aggregations import (
    COLS,
    _apply_filters,
    _agg_voos_por_dia,
    _agg_status,
    _agg_top_rotas,
    _base_atraso,
    _agg_aeroporto_mais_atrasos,
    _agg_variacao_aeroporto,
    _agg_atrasos_por_ano,
    _agg_dias_semana_por_ano,
    _agg_periodo_por_ano,
    _agg_companhias_por_ano,
)
from .ui.charts import (
    render_kpis,
    render_general_charts,
    render_delay_insights,
)
from .ui.maps import render_route_map
from .ui.charts import render_sample_and_downloads


class FlightsDashboard:
    """Classe principal que gera a UI e chama serviços."""

    def render_dashboard(self, df: Optional[pl.DataFrame] = None) -> None:
        """Renderiza todo o dashboard. Opcionalmente recebe um DataFrame pronto (Polars)."""
        st.set_page_config(page_title="Painel de Voos (Polars)", layout="wide")

        df_raw = self._render_input_section(df)
        self._render_sanity(df_raw)

        # Filtros
        (
            empresas, situacoes, status, tipos_linha,
            origs, dests, faixa, bt_filtrar
        ) = self._render_sidebar_filters(df_raw)

        df_filtrado = (
            _apply_filters(
                df_raw, empresas, situacoes, status, tipos_linha, origs, dests, faixa
            ) if bt_filtrar else df_raw
        )

        # KPIs + gráficos gerais
        render_kpis(df_filtrado)
        render_general_charts(df_filtrado)

        # Insights de atraso
        st.sidebar.header("Parâmetros de Atraso")
        limite_min = st.sidebar.slider(
            "Considerar atraso acima de (minutos)", 0, 180, 15, 5, key="limite_min_insights"
        )
        df_delay = _base_atraso(df_filtrado, limite_min)
        
        

        st.header("Insights de Atrasos")
        render_delay_insights(
            df_delay,
            _agg_aeroporto_mais_atrasos,
            _agg_variacao_aeroporto,
            _agg_atrasos_por_ano,
            _agg_dias_semana_por_ano,
            _agg_periodo_por_ano,
            _agg_companhias_por_ano,
            df_filtered_no_date=df_delay,   # mesmo DF filtrado; o fallback ignora só a data internamente
            delay_limit=limite_min,
        )

        # Mapa de rotas
        render_route_map(df_filtrado, df_delay)

        # Amostra e downloads
        render_sample_and_downloads(df_filtrado)

    # -------------------------
    # Privados (UI de entrada e sanity)
    # -------------------------
    def _render_input_section(self, df_initial: Optional[pl.DataFrame]) -> pl.DataFrame:
        if df_initial is not None:
            return df_initial

        st.sidebar.subheader("Entrada de dados")
        fonte = st.sidebar.radio(
            "Como quer fornecer o DataFrame?",
            ("Caminho fixo (Parquet)", "Selecionar arquivo (Parquet)"),
            horizontal=True
        )

        df: Optional[pl.DataFrame] = None
        if fonte == "Caminho fixo (Parquet)":
            caminho_default = "logs/eventlog.parquet"
            caminho = st.sidebar.text_input("Caminho do arquivo Parquet", value=caminho_default)
            if st.sidebar.button("Carregar do caminho", type="primary"):
                try:
                    df = _load_parquet_from_path(caminho)
                    st.sidebar.success(f"Arquivo carregado: {caminho}")
                except Exception as e:
                    st.sidebar.error(f"Erro ao ler '{caminho}': {e}")
        else:
            file = st.sidebar.file_uploader("Selecionar arquivo Parquet", type=["parquet"])
            if file is not None:
                try:
                    df = _load_parquet_from_bytes(file.read())
                    st.sidebar.success(f"Arquivo carregado: {getattr(file, 'name', 'upload')}")
                except Exception as e:
                    st.sidebar.error(f"Erro ao ler o arquivo selecionado: {e}")

        # Fallback
        if df is None:
            try:
                df = _load_parquet_from_path("logs/eventlog.parquet")
                st.sidebar.info("Usando fallback: logs/eventlog.parquet")
            except Exception:
                st.warning("Carregue um Parquet (via caminho fixo ou seleção de arquivo) para continuar.")
                st.stop()
        return df

    def _render_sanity(self, df: pl.DataFrame):
        colunas_esperadas = [
            COLS.EMPRESA_ICAO, COLS.NUMERO_VOO, COLS.ORIGEM_ICAO, COLS.DESTINO_ICAO,
            COLS.PARTIDA_PREV, COLS.PARTIDA_REAL, COLS.CHEGADA_PREV, COLS.CHEGADA_REAL,
            COLS.SITUACAO_VOO, "Código Justificativa",
            COLS.ORIGEM, COLS.ORIGEM_MUN, "Origem GPS", "Origem Coordenadas", "Tamanho Origem",
            COLS.DESTINO, COLS.DEST_MUN, "Destino GPS", "Destino Coordenadas", "Tamanho Destino",
            "Justificativa", COLS.TIPO_LINHA, COLS.EMPRESA, COLS.STATUS_VOO
        ]
        faltando = [c for c in colunas_esperadas if c not in df.columns]
        if faltando:
            st.warning(f"As colunas abaixo não foram encontradas e alguns recursos podem desabilitar: {faltando}")

    def _render_sidebar_filters(self, df: pl.DataFrame):
        st.sidebar.header("Filtros")
        airlines = st.sidebar.multiselect("Empresa Aérea", _unique_values(df, COLS.EMPRESA))
        situations = st.sidebar.multiselect("Situação do Voo", _unique_values(df, COLS.SITUACAO_VOO))
        statuses = st.sidebar.multiselect("Status do Voo", _unique_values(df, COLS.STATUS_VOO))
        line_types = st.sidebar.multiselect("Tipo de Linha", _unique_values(df, COLS.TIPO_LINHA))
        origins = st.sidebar.multiselect("Aeródromo Origem", _unique_values(df, COLS.ORIGEM))
        destinations = st.sidebar.multiselect("Aeródromo Destino", _unique_values(df, COLS.DESTINO))

        min_dt, max_dt = _date_bounds(df, COLS.PARTIDA_PREV)
        if min_dt and max_dt:
            start_date = st.sidebar.date_input("Data Inicial da Partida Prevista", value=min_dt.date(), format="YYYY-MM-DD")
            end_date = st.sidebar.date_input("Data Final da Partida Prevista", value=max_dt.date(), format="YYYY-MM-DD")
            date_range = (str(start_date), str(end_date))
        else:
            date_range = (None, None)

        apply_btn = st.sidebar.button("Aplicar filtros", type="primary")
        return airlines, situations, statuses, line_types, origins, destinations, date_range, apply_btn

