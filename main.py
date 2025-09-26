
import polars as pl
from app.dashboard.flight_dashboard import FlightsDashboard
from app.model.dataframe_manager import DataFrameManager
from app.model.transformer import Transformer
from app.utils.utils import load_json_file
from pathlib import Path

TRANSFORM_NEEDED = False
RAWLOG_PATH = Path("logs/test_logs/eventlog_no_transformation.parquet")
TRANSFORMED_LOG_PATH = Path("logs/eventlog.parquet")
CSV_FILES_PATH = Path("app/docs/*.csv")

def execute_transformation() -> pl.DataFrame:
    
    mng = DataFrameManager()
    if RAWLOG_PATH.exists():
        eventlog = mng.parquet_to_dataframe(RAWLOG_PATH)
    else:
        eventlog = mng.get_full_dataframe(CSV_FILES_PATH)
    
    transformer = Transformer()
    eventlog = transformer.transform(eventlog)
    eventlog.write_parquet(TRANSFORMED_LOG_PATH)
    return eventlog
    


if __name__ == "__main__":
    try:
        dash = FlightsDashboard()
        if TRANSFORM_NEEDED:
            dash.render_dashboard(execute_transformation())
        elif TRANSFORMED_LOG_PATH.exists():
            eventlog = pl.read_parquet(TRANSFORMED_LOG_PATH)
            dash.render_dashboard(eventlog)
    except ValueError:
        raise ValueError