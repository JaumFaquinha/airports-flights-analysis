import polars as pl

from app.utils.utils import load_json_file

class Transformer:
    
    def __init__(self):
        pass
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._map_rows(df)
        df = self._remove_invalid_fligths(df)
        df = self._is_late(df)
        df = self._drop_unused_columns(df)
        df = self._normalize_dates(df)
        
        return df
      
    def _set_airports_names(self, df: pl.DataFrame) -> pl.DataFrame:
        
        airports = load_json_file("app/docs/json/airport-codes.json")

        df_airports = (
            pl.from_records(airports, infer_schema_length=10000)
            .select([
                pl.col("icao_code").cast(pl.Utf8),
                pl.col("name").cast(pl.Utf8),
                pl.col("continent").cast(pl.Utf8),
                pl.col("iso_country").cast(pl.Utf8),
                pl.col("municipality").cast(pl.Utf8),
                pl.col("gps_code").cast(pl.Utf8),
                pl.col("coordinates").cast(pl.Utf8),
                pl.col("type").cast(pl.Utf8),
            ])
            .unique(subset=["icao_code"], keep="first")
        )

        # ---- Join para ORIGEM ----
        df = df.join(
            df_airports.rename({
                "icao_code": "ICAO Aeródromo Origem",
                "name": "Aeródromo Origem",
                "continent": "Origem Continente",
                "iso_country": "Origem País ISO",
                "municipality": "Origem Município",
                "gps_code": "Origem GPS",
                "coordinates": "Origem Coordenadas",
                "type": "Tamanho Origem"
            }),
            on="ICAO Aeródromo Origem",
            how="left",
        )

        # ---- Join para DESTINO ----
        df = df.join(
            df_airports.rename({
                "icao_code": "ICAO Aeródromo Destino",
                "name": "Aeródromo Destino",
                "continent": "Destino Continente",
                "iso_country": "Destino País ISO",
                "municipality": "Destino Município",
                "gps_code": "Destino GPS",
                "coordinates": "Destino Coordenadas",
                "type": "Tamanho Destino"
            }),
            on="ICAO Aeródromo Destino",
            how="left",
        )
        
        df = df.filter(
            (pl.col("Tamanho Origem").is_not_null()) & 
            (pl.col("Tamanho Origem") != "heliport")
            ) 
        df = df.filter(
            (pl.col("Tamanho Destino").is_not_null()) & 
            (pl.col("Tamanho Destino") != "heliport")
            )
        
        mapping = {
            "large_airport": "Grande Porte",
            "medium_airport": "Médio Porte",
            "small_airport": "Pequeno Porte"
        }
        
        df = df.with_columns([
            (pl.col("Tamanho Origem")
            .map_elements(lambda x: mapping.get(x, ""))
            .alias("Tamanho Origem")),
            (pl.col("Tamanho Destino")
            .map_elements(lambda x: mapping.get(x, ""))
            .alias("Tamanho Destino"))
        ])
        

        return df
      
    def _map_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        
        df = self._set_airports_names(df)
        df = self._map_justification_codes(df)
        df = self._map_airlines_types(df)
        df = self._map_airlines_codes(df)
        return df
        
    def _is_late(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns([
            (pl.when(
                (pl.col("Partida Real") > pl.col("Partida Prevista"))| 
                (pl.col("Chegada Real") > pl.col("Chegada Prevista"))
                )
                .then(pl.lit("Atrasado"))
                .otherwise(pl.lit("Ok"))                
                .alias("Status do Voo")
            )
        ]) 
    
    def _remove_invalid_fligths(self, df: pl.DataFrame) -> pl.DataFrame:
    
        df = self._remove_null_fligths(df)
        df = self._filter_brazil_only(df)
        
        return df
    
    def _remove_null_fligths(self, df: pl.DataFrame) -> pl.DataFrame:
        
        df = df.filter(
            (pl.col("Situação Voo").is_not_null()) & 
            (pl.col("Situação Voo") == "REALIZADO")
            )
        df = df.filter([
            ((pl.col("Partida Real").is_not_null()) & 
            (pl.col("Chegada Real").is_not_null())),
            ((pl.col("Partida Prevista").is_not_null()) & 
            (pl.col("Chegada Prevista").is_not_null()))
            ])
        
        return df
    
    def _filter_brazil_only(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df = df.filter(
            (pl.col("Destino País ISO").is_not_null()) & 
            (pl.col("Destino País ISO") == "BR")
            ) 
        df = df.filter(
            (pl.col("Origem País ISO").is_not_null()) & 
            (pl.col("Origem País ISO") == "BR")
            )
        return df
   
    def _map_justification_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        
        codes = load_json_file("app/docs/json/justification-codes.json")  # dict
        return df.with_columns(
            pl.col("Código Justificativa")
            .map_elements(lambda x: codes.get(x, "Código não encontrado"))
            .alias("Justificativa")
        )
    
    def _map_airlines_types(self, df: pl.DataFrame) -> pl.DataFrame:
        codes = load_json_file("app/docs/json/airline-types.json")  # dict
        return df.with_columns(
            pl.col("Código Tipo Linha")
            .map_elements(lambda x: codes.get(x, "Código não encontrado"))
            .alias("Tipo Linha")
        )   
        
    def _drop_unused_columns(self, df: pl.DataFrame) -> pl. DataFrame:
        return df.drop([
            "Origem Continente", 
            "Origem País ISO", 
            "Destino Continente", 
            "Destino País ISO",
            "Código Autorização (DI)",
            "Código Tipo Linha",
            ])
        
    def _map_airlines_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        
        codes = load_json_file("app/docs/json/airlines-codes.json")
        df_airlines = (
            pl.from_records(codes, infer_schema_length=10000)
            .select([
                pl.col("Nome").cast(pl.Utf8).alias("empresa_nome"),
                pl.col("Sigla").cast(pl.Utf8).alias("icao_empresa"),
            ])
            .unique(subset=["icao_empresa"], keep="first")
        )
        
        df = df.join(
            df_airlines,
            left_on="ICAO Empresa Aérea",
            right_on="icao_empresa",
            how="left",
        ).with_columns(
            pl.col("empresa_nome").alias("Empresa Aérea")  
        ).drop(["empresa_nome"])
        
        return df
    
    def _normalize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        
        date_cols = [c for c in df.columns if c.startswith(("Partida Prevista", "Partida Real", "Chegada Prevista", "Chegada Real"))]
        
        df = df.with_columns([
            pl.col(c)
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)  # converte string para datetime naive
            .dt.replace_time_zone("America/Sao_Paulo")          # define o fuso como São Paulo (UTC-3)
            for c in date_cols
        ])
        
        return df