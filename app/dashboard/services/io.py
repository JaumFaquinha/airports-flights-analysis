
# dashboard/services/io.py
from __future__ import annotations
from typing import Optional, Tuple
import polars as pl
import streamlit as st

@st.cache_data(show_spinner=False)
def _load_parquet_from_path(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)

@st.cache_data(show_spinner=False)
def _load_parquet_from_bytes(file_bytes: bytes) -> pl.DataFrame:
    return pl.read_parquet(file_bytes)

@st.cache_data(show_spinner=False)
def _unique_values(df: pl.DataFrame, col: str, limit: int = 20000) -> list:
    if col not in df.columns:
        return []
    vals = df.select(pl.col(col).unique()).to_series().to_list()
    if len(vals) > limit:
        vals = vals[:limit]
    return sorted([v for v in vals if v is not None])

@st.cache_data(show_spinner=False)
def _date_bounds(df: pl.DataFrame, col: str) -> Tuple[Optional[object], Optional[object]]:
    if col not in df.columns:
        return (None, None)
    dtype = df.schema.get(col)
    if dtype not in (pl.Datetime, pl.Date):
        return (None, None)
    s = df.get_column(col).drop_nulls()
    if s.is_empty():
        return (None, None)
    return (s.min(), s.max())
