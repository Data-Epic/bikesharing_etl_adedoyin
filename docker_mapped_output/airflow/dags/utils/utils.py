import pandas as pd
from logger import logger

def stream_flag_data(file_path):
        """
        Generator that yields rows from a Parquet file.
        Filters for late night rides and long duration trips.
        """
        try:
            df = pd.read_parquet(file_path)

            for _, row in df.iterrows():
                row_dict = row.to_dict()
                hour = row['started_at'].hour

                if hour >= 23 or hour < 5:
                    logger.warning(f"Late night ride: {row_dict}")
                    yield row_dict

                elif row['duration_seconds'] > 2700:
                    logger.warning(f"Trip duration is longer than 45 minutes: {row_dict}")
                    yield row_dict

        except Exception as e:
            logger.error(f"Failed to stream data: {e}")