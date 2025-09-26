
import glob
import polars as pl
from io import StringIO
from pathlib import Path



class DataFrameManager:
    
    def __init__(self):
        pass
    
    def get_full_dataframe(self, csv_files_path: Path) -> pl.DataFrame:        
        files = glob.glob(csv_files_path)
        dfs = [pl.read_csv(
            f, 
            separator=";", 
            schema_overrides={"Número Voo": pl.Utf8, "Código Autorização (DI)": pl.Utf8}
            ) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
        
    def csv_to_dataframe(self, file_path) -> pl.DataFrame:
        csv = StringIO(file_path.GetContentString())
        return pl.read_csv(csv)
    
    def csv_to_json(self, file_path):
        df = pl.read_csv(file_path)
        df.write_ndjson("app/docs/json/airport-codes.json")
    
    def parquet_to_dataframe(self, file_path):
        return pl.read_parquet(file_path)
    
    