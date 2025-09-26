# dashboard/ui/maps.py
from __future__ import annotations
import polars as pl
import streamlit as st
import pydeck as pdk
import numpy as np

from ..services.aggregations import COLS

@st.cache_data(show_spinner=False)
def _prepare_routes(df_base: pl.DataFrame, coord_fmt: str, topn: int, usar_delay: bool):
    if df_base.is_empty() or any(c not in df_base.columns for c in [COLS.COORD_ORIG, COLS.COORD_DEST]):
        return (pl.DataFrame(schema={"origem_lon": pl.Float64}), pl.DataFrame(schema={"lon": pl.Float64}))

    dfc = (
        df_base
        .with_columns([
            pl.col(COLS.COORD_ORIG).str.replace_all(r"\s+", "").str.split_exact(",", 2).alias("_oc"),
            pl.col(COLS.COORD_DEST).str.replace_all(r"\s+", "").str.split_exact(",", 2).alias("_dc"),
        ])
        .with_columns([
            pl.col("_oc").struct.field("field_0").cast(pl.Float64).alias("_o_a"),
            pl.col("_oc").struct.field("field_1").cast(pl.Float64).alias("_o_b"),
            pl.col("_dc").struct.field("field_0").cast(pl.Float64).alias("_d_a"),
            pl.col("_dc").struct.field("field_1").cast(pl.Float64).alias("_d_b"),
        ])
        .drop(["_oc", "_dc"])
    )

    def _pick(lon_cand: pl.Expr, lat_cand: pl.Expr, other_lon: pl.Expr, other_lat: pl.Expr):
        if coord_fmt == "lon,lat":
            return lon_cand, lat_cand
        if coord_fmt == "lat,lon":
            return lat_cand, lon_cand
        valid_lon = (lon_cand <= -28.0) & (lon_cand >= -75.0)
        valid_lat = (lat_cand <= 6.0) & (lat_cand >= -34.0)
        return (
            pl.when(valid_lon & valid_lat).then(lon_cand).otherwise(other_lon),
            pl.when(valid_lon & valid_lat).then(lat_cand).otherwise(other_lat),
        )

    o_lon, o_lat = _pick(pl.col("_o_a"), pl.col("_o_b"), pl.col("_o_b"), pl.col("_o_a"))
    d_lon, d_lat = _pick(pl.col("_d_a"), pl.col("_d_b"), pl.col("_d_b"), pl.col("_d_a"))

    dfc = dfc.with_columns([
        o_lon.alias("origem_lon"),
        o_lat.alias("origem_lat"),
        d_lon.alias("destino_lon"),
        d_lat.alias("destino_lat"),
    ]).drop(["_o_a", "_o_b", "_d_a", "_d_b"])

    def _is_valid(lon: pl.Expr, lat: pl.Expr) -> pl.Expr:
        return (
            lon.is_not_null() & lat.is_not_null() &
            (lon <= -28.0) & (lon >= -75.0) &
            (lat <= 6.0) & (lat >= -34.0)
        )

    dfc = dfc.filter(
        _is_valid(pl.col("origem_lon"), pl.col("origem_lat")) &
        _is_valid(pl.col("destino_lon"), pl.col("destino_lat"))
    )

    if dfc.is_empty():
        return (pl.DataFrame(schema={"origem_lon": pl.Float64}), pl.DataFrame(schema={"lon": pl.Float64}))

    agg_cols = [pl.len().alias("Quantidade")]
    if usar_delay and "atraso_min" in dfc.columns:
        agg_cols.append(pl.col("atraso_min").mean().alias("atraso_medio"))

    rotas = (
        dfc.group_by(["origem_lon", "origem_lat", "destino_lon", "destino_lat"])
           .agg(agg_cols)
           .sort("Quantidade", descending=True)
           .head(topn)
    )

    nos = (
        pl.concat([
            rotas.select([pl.col("origem_lon").alias("lon"), pl.col("origem_lat").alias("lat"), pl.col("Quantidade")]),
            rotas.select([pl.col("destino_lon").alias("lon"), pl.col("destino_lat").alias("lat"), pl.col("Quantidade")]),
        ], how="vertical")
        .group_by(["lon", "lat"])
        .agg(pl.col("Quantidade").sum().alias("Quantidade"))
    )

    return rotas, nos

def render_route_map(df_filtrado: pl.DataFrame, df_delay: pl.DataFrame | None = None):
    st.markdown("---")
    st.header("Mapa de Rotas (Brasil)")

    st.sidebar.header("Mapa")
    coord_fmt = st.sidebar.selectbox("Formato das coordenadas", ["auto (detectar)", "lon,lat", "lat,lon"], index=0)
    top_n = st.sidebar.slider("Máx. rotas no mapa (Top N por volume)", 100, 5000, 1000, 100)
    usar_delay = st.sidebar.checkbox("Colorir por atraso médio (min)", value=True)

    df_mapa = df_delay if (df_delay is not None and "atraso_min" in df_delay.columns and df_delay.height > 0) else df_filtrado
    rotas, nos = _prepare_routes(df_mapa, coord_fmt, top_n, usar_delay)

    if rotas.is_empty():
        st.info("Sem dados de rotas válidas para o mapa com os filtros atuais.")
        return

    rotas_pd = rotas.to_pandas()
    nos_pd = nos.to_pandas()

    # Tipos
    for c in ["origem_lon", "origem_lat", "destino_lon", "destino_lat"]:
        if c in rotas_pd.columns:
            rotas_pd[c] = rotas_pd[c].astype(float)
    for c in ["lon", "lat"]:
        if c in nos_pd.columns:
            nos_pd[c] = nos_pd[c].astype(float)

    # Cores
    if usar_delay and "atraso_medio" in rotas_pd.columns:
        atraso = rotas_pd["atraso_medio"].astype(float)
        a_min, a_max = atraso.quantile(0.05), atraso.quantile(0.95)
        a_rng = max(a_max - a_min, 1e-6)
        c01 = ((atraso - a_min) / a_rng).clip(0, 1)
        r = (c01 * 255).round().astype(int)
        g = (100 * (1 - c01)).round().astype(int)
        b = (255 * (1 - c01)).round().astype(int)
    else:
        qtd = rotas_pd["Quantidade"].astype(float)
        q_min, q_max = qtd.quantile(0.05), qtd.quantile(0.95)
        q_rng = max(q_max - q_min, 1e-6)
        c01 = ((qtd - q_min) / q_rng).clip(0, 1)
        r = (50 + 205 * c01).round().astype(int)
        g = (100 * (1 - c01)).round().astype(int)
        b = (200 * (1 - c01)).round().astype(int)

    rotas_pd["rgba"] = list(zip(r, g, b, [180]*len(r)))
    rotas_pd["_w"] = (1.0 + (rotas_pd["Quantidade"].astype(float)) ** 0.5) * 1.2
    nos_pd["radius"] = np.minimum(50000.0, 20000.0 + 3000.0 * np.sqrt(nos_pd["Quantidade"].astype(float)).values)

    view_state = pdk.ViewState(latitude=-14.235, longitude=-51.925, zoom=3.5, pitch=0)

    nodes_layer = pdk.Layer(
        "ScatterplotLayer",
        data=nos_pd,
        get_position="[lon, lat]",
        get_radius="radius",
        get_fill_color=[30, 144, 255, 140],
        pickable=True,
        opacity=0.6,
    )

    arcs_layer = pdk.Layer(
        "ArcLayer",
        data=rotas_pd,
        get_source_position="[origem_lon, origem_lat]",
        get_target_position="[destino_lon, destino_lat]",
        get_width="_w",
        get_source_color="rgba",
        get_target_color="rgba",
        pickable=True,
        great_circle=True,
        auto_highlight=True,
    )

    tooltip = {
        "html": "<b>Rotas</b><br/>Qtd: {qtd}"
                + ("<br/>Atraso médio: {atraso_medio} min" if ("atraso_medio" in rotas_pd.columns) else ""),
        "style": {"backgroundColor": "rgba(30,30,30,0.8)", "color": "white"},
    }

    r = pdk.Deck(
        layers=[arcs_layer, nodes_layer],
        initial_view_state=view_state,
        map_style="light",
        tooltip=tooltip,
    )
    st.pydeck_chart(r, use_container_width=True)
