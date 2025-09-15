from pathlib import Path
from app.model.dataframe_manager import DataFrameManager
from app.model.transformer import Transformer
from app.utils.utils import load_json_file





if __name__ == "__main__":
    
    eventlog_path = Path("logs/eventlog.parquet") 
    mng = DataFrameManager()
    mng.csv_to_json("app/docs/csv/airport-codes (1).csv")
    if eventlog_path.exists():
        eventlog = mng.parquet_to_dataframe(eventlog_path)
        transformer = Transformer()
        eventlog = transformer.transform(eventlog)
        
    else:
        df = mng.get_full_dataframe()
    pass
    