import logging
import os
from datetime import datetime

CURRENT_DATE = datetime.now().strftime("%d-%m-%Y")
LOG_NAME = f"bike_rides_{CURRENT_DATE}.log"
LOCAL_STORAGE = '/opt/airflow/data'

log_path = os.path.join(LOCAL_STORAGE, LOG_NAME)

def setup_logger(log_path):
    logging.basicConfig(
        level=logging.WARNING, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
        ]
    )
    return logging

logger = setup_logger(log_path).getLogger(__name__)
