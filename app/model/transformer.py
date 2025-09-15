import polars as pl
from pathlib import Path
import glob

from app.utils.utils import load_json_file

class Transformer:
    
    def __init__(self):
        pass
    
    
    
    # Você irá analisar atrasos em voos no Brasil:
    #     Qual o aeroporto que tem mais atrasos no geral?
    #     Qual o aeroporto aumentou o número de atrasos e qual diminuiu o número de atrasos?
    #     Os atrasos aumentaram ou diminuíram no período?
    #     Dias da semana com mais atrasos (a cada ano)
    #     Período do dia com mais atrasos (a cada ano)
    #     Companhia que mais atrasa (a cada ano)
    
    def transform(self, df: pl.DataFrame):
        
        #Retirar voos que não contém Situação de Voo como Realizado.
        #Validar se a Saída Real bate com Saída Prevista setar como atrasado
        #Validar se a Chegada Real bate com Chegada Prevista
        
        df = self._map_rows(df)
        df = self._remove_invalid_fligths(df)
        df = self._is_late(df)
        
        return df
        
    def _map_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._map_justification_codes(df)
        
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
    
        df = df.filter(
            (pl.col("Situação Voo").is_not_null()) & 
            (pl.col("Situação Voo") == "REALIZADO")
            )
        df = df.filter(
            (pl.col("Partida Real").is_not_null()) & 
            (pl.col("Chegada Real").is_not_null())
            )
        
        return df
    
    def _map_justification_codes(self, df: pl.DataFrame) -> pl.DataFrame:
        
        codes = load_json_file("app/docs/json/justification-codes.json")  # dict
        return df.with_columns(
            pl.col("Código Justificativa")
            .map_elements(lambda x: codes.get(x, "Código não encontrado"))
            .alias("Justificativa")
        )