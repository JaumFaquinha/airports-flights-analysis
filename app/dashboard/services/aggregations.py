
# dashboard/services/aggregations.py
from __future__ import annotations
from typing import Optional
import polars as pl
import streamlit as st

class COLS:
    EMPRESA_ICAO = "ICAO Empresa Aérea"
    NUMERO_VOO = "Número Voo"
    ORIGEM_ICAO = "ICAO Aeródromo Origem"
    DESTINO_ICAO = "ICAO Aeródromo Destino"
    PARTIDA_PREV = "Partida Prevista"
    PARTIDA_REAL = "Partida Real"
    CHEGADA_PREV = "Chegada Prevista"
    CHEGADA_REAL = "Chegada Real"
    SITUACAO_VOO = "Situação Voo"
    STATUS_VOO = "Status do Voo"
    TIPO_LINHA = "Tipo Linha"
    EMPRESA = "Empresa Aérea"
    ORIGEM_MUN = "Origem Município"
    DEST_MUN = "Destino Município"
    ORIGEM = "Aeródromo Origem"
    DESTINO = "Aeródromo Destino"
    COORD_ORIG = "Origem Coordenadas"
    COORD_DEST = "Destino Coordenadas"

# -------------- filtros --------------
def _apply_filters(
    df: pl.DataFrame,
    empresas: list,
    situacoes: list,
    status: list,
    tipos_linha: list,
    icao_origem: list,
    icao_destino: list,
    faixa_partida: Optional[tuple],  # (start_str, end_str)
) -> pl.DataFrame:
    exprs = []
    if empresas:
        exprs.append(pl.col(COLS.EMPRESA).is_in(empresas))
    if situacoes:
        exprs.append(pl.col(COLS.SITUACAO_VOO).is_in(situacoes))
    if status:
        exprs.append(pl.col(COLS.STATUS_VOO).is_in(status))
    if tipos_linha:
        exprs.append(pl.col(COLS.TIPO_LINHA).is_in(tipos_linha))
    if icao_origem:
        exprs.append(pl.col(COLS.ORIGEM_ICAO).is_in(icao_origem))
    if icao_destino:
        exprs.append(pl.col(COLS.DESTINO_ICAO).is_in(icao_destino))

    if faixa_partida and COLS.PARTIDA_PREV in df.columns:
        start_str, end_str = faixa_partida
        dtype = df.schema.get(COLS.PARTIDA_PREV)
        if dtype in (pl.Datetime, pl.Date):
            day_expr = pl.col(COLS.PARTIDA_PREV).dt.strftime("%Y-%m-%d")
            if start_str:
                exprs.append(day_expr >= pl.lit(start_str))
            if end_str:
                exprs.append(day_expr <= pl.lit(end_str))
        else:
            s = pl.col(COLS.PARTIDA_PREV)
            if start_str:
                exprs.append(s >= pl.lit(start_str))
            if end_str:
                exprs.append(s <= pl.lit(end_str))

    if not exprs:
        return df
    filtro = exprs[0]
    for e in exprs[1:]:
        filtro = filtro & e
    return df.filter(filtro)

# -------------- agregações gerais --------------
@st.cache_data(show_spinner=False)
def _agg_voos_por_dia(df: pl.DataFrame) -> pl.DataFrame:
    if COLS.PARTIDA_PREV in df.columns and df.schema.get(COLS.PARTIDA_PREV) in (pl.Datetime, pl.Date):
        return (
            df.with_columns(pl.col(COLS.PARTIDA_PREV).dt.date().alias("Dia"))
              .group_by("Dia")
              .agg(pl.len().alias("Voos"))
              .sort("Dia")
        )
    elif COLS.PARTIDA_PREV in df.columns:
        return (
            df.with_columns(pl.col(COLS.PARTIDA_PREV).str.slice(0, 10).alias("dia_str"))
              .group_by("dia_str")
              .agg(pl.len().alias("Voos"))
              .sort("dia_str")
        )
    return pl.DataFrame({"Dia": [], "Voos": []})

@st.cache_data(show_spinner=False)
def _agg_status(df: pl.DataFrame) -> pl.DataFrame:
    col = COLS.STATUS_VOO if COLS.STATUS_VOO in df.columns else COLS.SITUACAO_VOO
    if col not in df.columns:
        return pl.DataFrame({"status": [], "Quantidade": []})
    return (df.group_by(col).agg(pl.len().alias("Quantidade"))
              .rename({col: "status"})
              .sort("Quantidade", descending=True))

@st.cache_data(show_spinner=False)
def _agg_top_rotas(df: pl.DataFrame, topn: int = 20) -> pl.DataFrame:
    base_cols = [c for c in (COLS.ORIGEM_ICAO, COLS.DESTINO_ICAO) if c in df.columns]
    if len(base_cols) < 2:
        return pl.DataFrame({"origem": [], "destino": [], "Quantidade": []})
    return (
        df.group_by([COLS.ORIGEM_ICAO, COLS.DESTINO_ICAO])
          .agg(pl.len().alias("Quantidade"))
          .sort("Quantidade", descending=True)
          .head(topn)
          .rename({COLS.ORIGEM_ICAO: "origem", COLS.DESTINO_ICAO: "destino"})
    )

# -------------- atrasos e insights --------------
@st.cache_data(show_spinner=False)
def _base_atraso(df: pl.DataFrame, limite: int) -> pl.DataFrame:
    req = [COLS.PARTIDA_PREV, COLS.PARTIDA_REAL]
    if any(c not in df.columns for c in req) or df.height == 0:
        return pl.DataFrame(schema={"atraso_min": pl.Float64})
    return (
        df
        .with_columns([
            (pl.col(COLS.PARTIDA_REAL) - pl.col(COLS.PARTIDA_PREV)).dt.total_minutes().alias("atraso_min"),
            pl.col(COLS.PARTIDA_PREV).dt.year().alias("Ano"),
            pl.col(COLS.PARTIDA_PREV).dt.weekday().alias("dia_sem"),
            pl.col(COLS.PARTIDA_PREV).dt.hour().alias("hora"),
        ])
        .filter(pl.col("atraso_min").is_not_null() & (pl.col("atraso_min") > limite))
    )

def _periodo_expr(h: pl.Expr) -> pl.Expr:
    return (
        pl.when((h >= 0) & (h < 6)).then(pl.lit("Madrugada"))
         .when((h >= 6) & (h < 12)).then(pl.lit("Manhã"))
         .when((h >= 12) & (h < 18)).then(pl.lit("Tarde"))
         .otherwise(pl.lit("Noite"))
         .alias("periodo")
    )

@st.cache_data(show_spinner=False)
def _agg_aeroporto_mais_atrasos(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or COLS.ORIGEM not in df.columns:
        return pl.DataFrame({"Aeroporto": [], "Quantidade Atrasos": []})
    return (
        df.group_by(COLS.ORIGEM)
          .agg(pl.len().alias("Quantidade Atrasos"))
          .rename({COLS.ORIGEM: "Aeroporto"})
          .sort("Quantidade Atrasos", descending=True)
    )

@st.cache_data(show_spinner=False)
def _agg_variacao_aeroporto(df: pl.DataFrame):  # ajuste o import conforme sua estrutura

    if df.is_empty() or any(c not in df.columns for c in [COLS.ORIGEM, "ano"]):
        return (pl.DataFrame(), pl.DataFrame(), None, None)

    # garante tipo inteiro para o ano
    df2 = df.with_columns(pl.col("ano").cast(pl.Int32))

    # atrasos por ano e aeroporto
    por_ano = df2.group_by(["ano", COLS.ORIGEM]).agg(pl.len().alias("qtd"))

    # escolhe os dois anos mais recentes
    anos = (
        por_ano.select(pl.col("ano").drop_nulls().unique().sort())
               .to_series().to_list()
    )
    if len(anos) < 2:
        return (pl.DataFrame(), pl.DataFrame(), None, None)

    a1, a2 = anos[-2], anos[-1]

    # marca "prev" e "curr" para não depender do nome 2023/2024 como coluna
    pivot = (
        por_ano
        .filter(pl.col("ano").is_in([a1, a2]))
        .with_columns(
            pl.when(pl.col("ano") == a1).then(pl.lit("prev")).otherwise(pl.lit("curr")).alias("_slot")
        )
        .pivot(values="qtd", index=COLS.ORIGEM, columns="_slot")
        .fill_null(0)
        .rename({"prev": "y_prev", "curr": "y_curr"})
        .with_columns((pl.col("y_curr") - pl.col("y_prev")).alias("delta"))
        .rename({COLS.ORIGEM: "aeroporto"})
    )

    # top 10 aumentos / top 10 quedas
    maiores_aumentos = pivot.sort("delta", descending=True).head(10)
    maiores_quedas   = pivot.sort("delta", descending=False).head(10)

    return (maiores_aumentos, maiores_quedas, a1, a2)

@st.cache_data(show_spinner=False)
def _agg_atrasos_por_ano(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "Ano" not in df.columns:
        return pl.DataFrame({"Ano": [], "Quantidade Atrasos": []})
    return df.group_by("Ano").agg(pl.len().alias("Quantidade Atrasos")).sort("Ano")

@st.cache_data(show_spinner=False)
def _agg_dias_semana_por_ano(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or any(c not in df.columns for c in ["Ano", "dia_sem"]):
        return pl.DataFrame({"Ano": [], "dia_sem": [], "Quantidade Atrasos": []})
    return (
        df.group_by(["Ano", "dia_sem"])
          .agg(pl.len().alias("Quantidade Atrasos"))
          .sort(["Ano", "Quantidade Atrasos"], descending=[False, True])
    )

@st.cache_data(show_spinner=False)
def _agg_periodo_por_ano(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or any(c not in df.columns for c in ["Ano", "hora"]):
        return pl.DataFrame({"Ano": [], "periodo": [], "Quantidade Atrasos": []})
    return (
        df.with_columns(_periodo_expr(pl.col("hora")))
          .group_by(["Ano", "periodo"])
          .agg(pl.len().alias("Quantidade Atrasos"))
          .sort(["Ano", "Quantidade Atrasos"], descending=[False, True])
    )

@st.cache_data(show_spinner=False)
def _agg_companhias_por_ano(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or COLS.EMPRESA not in df.columns or "Ano" not in df.columns:
        return pl.DataFrame({"Ano": [], "empresa": [], "Quantidade Atrasos": []})
    return (
        df.group_by(["Ano", COLS.EMPRESA])
          .agg(pl.len().alias("Quantidade Atrasos"))
          .rename({COLS.EMPRESA: "empresa"})
          .sort(["Ano", "Quantidade Atrasos"], descending=[False, True])
    )
