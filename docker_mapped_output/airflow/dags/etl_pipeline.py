from datetime import datetime, timedelta
import requests
import os
import io
import logging
import polars as pl
from zipfile import ZipFile

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 29),
    'email': ['adedoyinsamuel25@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DATA_URL = 'https://s3.amazonaws.com/capitalbikeshare-data/202212-capitalbikeshare-tripdata.zip'
LOCAL_STORAGE = opt/
MINIO_BUCKET_RAW = 'bikeshare-raw-data'
MINIO_BUCKET_CLEANED = 'bikeshare-cleaned-data'
AWS_CONN_ID = 'minio_s3_conn'

@dag(
    dag_id='bikeshare_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 10 * * 1',
    catchup=False,
    tags=['bikeshare_etl'],
)
def bikeshare_etl_pipeline():

    @task()
    def extract_data(data_url: str):
        """Download ZIP from URL and upload to MinIO/S3"""
        response = requests.get(data_url, stream=True, timeout=30)
        response.raise_for_status()
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        key = f"{current_date}_capitalbikeshare-tripdata.zip"
        
        buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            buffer.write(chunk)
        buffer.seek(0)
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        conn = s3_hook.get_connection(AWS_CONN_ID)
        
        endpoint_url = conn.extra_dejson.get("endpoint_url")     
        
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=MINIO_BUCKET_RAW,
            replace=True,
        )
        
        return f"{endpoint_url}/{MINIO_BUCKET_RAW}/{key}"

    @task()
    def unzip_file(zip_file_url: str) -> str:
        """Download ZIP from MinIO/S3 URL, extract it, and upload files to same bucket"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        parts = zip_file_url.split(f"/{MINIO_BUCKET_RAW}/")
        if len(parts) != 2:
            raise ValueError(f"Invalid MinIO/S3 URL format: {zip_file_url}")
        
        zip_key = parts[1]
        
        # Download the ZIP file
        zip_bytes = s3_hook.read_key(key=zip_key, bucket_name=MINIO_BUCKET_RAW)
        
        folder_name = zip_key.rsplit('.', 1)[0]
        
        with ZipFile(io.BytesIO(zip_bytes), 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.is_dir():
                    file_data = zip_ref.read(file_info.filename)
                    file_key = f"{folder_name}/{file_info.filename}"
                    s3_hook.load_bytes(
                        bytes_data=file_data,
                        key=file_key,
                        bucket_name=MINIO_BUCKET_RAW,
                        replace=True
                    )
        
        return f"{parts[0]}/{MINIO_BUCKET_RAW}/{folder_name}/"

    @task()
    def get_latest_data(folder_name: str) -> str:
        """Return the first CSV file path in the folder"""
        for fname in os.listdir(folder_name):
            if fname.endswith('.csv'):
                return os.path.join(folder_name, fname)
        raise FileNotFoundError(f'No CSV found in {folder_name}')

    @task()
    def transform_data(file_path: str) -> pl.DataFrame:
        """Read CSV, parse dates, compute durations and dedupe"""
        df = pl.read_csv(
            file_path,
            dtypes={
                'ride_id': pl.Utf8,
                'rideable_type': pl.Utf8,
                'started_at': pl.Utf8,
                'ended_at': pl.Utf8,
                'start_station_name': pl.Utf8,
                'start_station_id': pl.Int64,
                'end_station_name': pl.Utf8,
                'end_station_id': pl.Int64,
                'start_lat': pl.Float64,
                'start_lng': pl.Float64,
                'end_lat': pl.Float64,
                'end_lng': pl.Float64,
                'member_casual': pl.Utf8,
            },
        )
        df = df.with_columns([
            pl.col('started_at').str.strptime(pl.Datetime, fmt=None).alias('started_at'),
            pl.col('ended_at').str.strptime(pl.Datetime, fmt=None).alias('ended_at'),
        ]).with_columns([
            (pl.col('ended_at') - pl.col('started_at')).alias('duration'),
            (pl.col('ended_at') - pl.col('started_at')).dt.seconds().alias('duration_seconds'),
            pl.col('ended_at').dt.week_of_year().alias('week'),
        ]).unique(subset=['ride_id'])
        return df

    @task()
    def stream_flag_data(df: pl.DataFrame) -> pl.DataFrame:
        """Log rides exceeding duration or starting late"""
        logger = logging.getLogger('ride_flags')
        logger.setLevel(logging.WARNING)
        handler = logging.FileHandler('ride_flags.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.addHandler(handler)
        long_rides = df.filter(pl.col('duration_seconds') > 2700)
        logger.warning(f'Long rides > 45 mins: {long_rides.height}')
        late_rides = df.filter(
            (pl.col('started_at').dt.hour() >= 23) &
            (pl.col('started_at').dt.minute() >= 59)
        )
        logger.warning(f'Rides starting after 11:59 PM: {late_rides.height}')
        return df

    @task()
    def load_cleaned_data_to_minio(df: pl.DataFrame) -> str:
        """Write parquet to MinIO partitioned by member and week"""
        timestamp = datetime.now().strftime('%Y-%m-%d')
        key = f'capitalbikeshare-tripdata_{timestamp}.parquet'
        buf = io.BytesIO()
        df.write_parquet(buf, partition_cols=['member_casual', 'week'])
        buf.seek(0)
        S3Hook(aws_conn_id=AWS_CONN_ID).load_bytes(
            bytes_data=buf.getvalue(),
            key=key,
            bucket_name=MINIO_BUCKET_CLEANED,
            replace=True,
        )
        return key

    @task()
    def load_cleaned_data_to_postgres(parquet_key: str) -> bool:
        """Placeholder for loading parquet to Postgres"""
        print(f'Loaded {parquet_key} to PostgreSQL database')
        return True

    zip_file = extract_data(DATA_URL)
    folder = unzip_file(zip_file)
    csv_path = get_latest_data(folder)
    transformed = transform_data(csv_path)
    flagged = stream_flag_data(transformed)
    minio_key = load_cleaned_data_to_minio(flagged)
    load_cleaned_data_to_postgres(minio_key)

bikeshare_etl_pipeline()
