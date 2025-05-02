from datetime import datetime, timedelta
import requests
import os
import io
from zipfile import ZipFile
import shutil
from dotenv import load_dotenv
from utils.utils import stream_flag_data
from utils.logger import log_path

import polars as pl
import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


load_dotenv()

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
LOCAL_STORAGE = '/opt/airflow/data'
CURRENT_DATE = datetime.now().strftime("%d-%m-%Y")
MINIO_BUCKET_RAW = 'bikeshare-raw-data'
MINIO_BUCKET_CLEANED = 'bikeshare-transformed-data'
MINIO_PARTITIONED_DATA = "bikeshare-partitioned-data"
MINIO_LOG = "bikeshare-flagged-log"
AWS_CONN_ID = 'minio_s3_conn'
DATABASE_URL=os.getenv("DATABASE_URL")

@dag(
    dag_id='bikeshare_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 10 * * 1',
    catchup=False,
    tags=['bikeshare_etl'],
)
def bikeshare_etl_pipeline():

    @task()
    def download_zipfile_to_bucket(data_url: str) -> str:
        """Download ZIP from URL and upload to MinIO/S3"""
        try:
            response = requests.get(data_url, stream=True, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(f"Failed to download data from {data_url}: {e}")

        key = f"{CURRENT_DATE}_capitalbikeshare-tripdata.zip"
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
    def download_and_unzip_locally() -> str:
        """Download ZIP from URL, save locally, and extract"""
        zip_path = os.path.join(LOCAL_STORAGE, f"{CURRENT_DATE}_capitalbikeshare-tripdata.zip")

        try:
            response = requests.get(DATA_URL, stream=True, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(f"Failed to download data from {DATA_URL}: {e}")

        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        folder_name =os.path.join(LOCAL_STORAGE,f"{CURRENT_DATE}_capitalbikeshare-tripdata")
        os.makedirs(folder_name, exist_ok=True)

        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(folder_name)

        os.remove(zip_path)

        return folder_name

    @task()
    def upload_files_to_minio(folder_path: str) -> str:
        """Upload CSV files to MinIO, return CSV path"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError(f'No CSV files found in {folder_path}')

        for csv_file in csv_files:
            local_path = os.path.join(folder_path, csv_file)
            s3_key = f"{CURRENT_DATE}_{csv_file}"

            s3_hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=MINIO_BUCKET_RAW,
                replace=True
            )

        return local_path

    @task()
    def transform_data(file_path: str) -> str:
        """Read CSV, transform it, and upload Parquet to MinIO"""

        schema = {
            'ride_id': pl.String,
            'rideable_type': pl.String,
            'started_at': pl.String,
            'ended_at': pl.String,
            'start_station_name': pl.String,
            'start_station_id': pl.String,
            'end_station_name': pl.String,
            'end_station_id': pl.String,
            'start_lat': pl.Float64,
            'start_lng': pl.Float64,
            'end_lat': pl.Float64,
            'end_lng': pl.Float64,
            'member_casual': pl.String,
        }

        data = pl.read_csv(file_path, schema=schema)

        data = data.with_columns([
            pl.col('started_at').str.to_datetime(),
            pl.col('ended_at').str.to_datetime(),
            (pl.col('ended_at').str.to_datetime() - pl.col('started_at').str.to_datetime()).alias('duration'),
            (pl.col('ended_at').str.to_datetime() - pl.col('started_at').str.to_datetime())
                .dt.total_seconds().alias('duration_seconds'),
            pl.col('ended_at').str.to_datetime().dt.week().alias('week')
        ])

        data = data.unique(subset='ride_id')

        output_file = f"{LOCAL_STORAGE}/{CURRENT_DATE}_capitalbikeshare-tripdata.parquet"
        data.write_parquet(output_file)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_key = f"{CURRENT_DATE}/capitalbikeshare-tripdata.parquet"
        s3_hook.load_file(
            filename=output_file,
            key=s3_key,
            bucket_name=MINIO_BUCKET_CLEANED,
            replace=True
        )

        return output_file
    

    @task()
    def load_cleaned_data_to_minio(output_file: str) -> str:
        """
        Read cleaned Parquet file, partition it by 'member_casual' and 'week',
        and upload each partition to MinIO as a separate Parquet file.
        """

        df = pd.read_parquet(output_file)


        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_keys = []

        grouped = df.groupby(['member_casual', 'week'])

        for (member, week), group_df in grouped:
            file_name = f"{member}_week{week}_{CURRENT_DATE}.parquet"
            s3_key = f"{CURRENT_DATE}/{member}/week={week}/{file_name}"

            buffer = io.BytesIO()
            group_df.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3.load_bytes(
                bytes_data=buffer.read(),
                key=s3_key,
                bucket_name=MINIO_PARTITIONED_DATA,
                replace=True
            )

            uploaded_keys.append(s3_key)

        return f"Uploaded {len(uploaded_keys)} partitioned files to {MINIO_PARTITIONED_DATA}"


    @task
    def stream_log_data(output_file):
        for data in stream_flag_data(output_file):
            data
        return True


    @task
    def load_log_to_bucket(folder_path):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        log_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.log')]

        for file in log_files:
            local_path = os.path.join(folder_path, file)
            s3_key = file
            s3_hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=MINIO_LOG,
                replace=True
            )

        return local_path

    @task()
    def load_cleaned_data_to_postgres(output_file: str) -> bool:
        """
        Load cleaned Parquet data into the Postgres table 'bikeshare_data'.
        """
        try:
            df = pd.read_parquet(output_file)

            engine = create_engine(DATABASE_URL)

            df.to_sql(
                name="bikeshare_data",
                con=engine,
                if_exists="replace",
                index=False
            )
            return True
        except Exception as e:
            print(f"An Error Occured: {e}")

    @task()
    def cleanup_local_files(folder_path: str):
        """Clean up all files and subdirectories within the given folder path, but keep the folder itself."""
        try:
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            print(f"Successfully cleaned up contents of {folder_path}")
        except Exception as e:
            print(f"Failed to clean up contents of {folder_path}: {str(e)}")


    zip_key = download_zipfile_to_bucket(DATA_URL)
    extracted_folder = download_and_unzip_locally()
    csv_path = upload_files_to_minio(extracted_folder)
    transformed_data = transform_data(csv_path)
    logged_data = stream_log_data(transformed_data)
    upload_log_to_bucket = load_log_to_bucket(log_path)
    load_data_to_bucket = load_cleaned_data_to_minio(transformed_data)
    postgres_loaded = load_cleaned_data_to_postgres(transformed_data)
    cleanup = cleanup_local_files(extracted_folder)

    # Set dependencies
    zip_key >> extracted_folder
    extracted_folder >> csv_path
    csv_path >> transformed_data
    transformed_data >> logged_data >> upload_log_to_bucket
    transformed_data >> load_data_to_bucket
    transformed_data >> postgres_loaded

    # Ensure cleanup runs after all other tasks
    [upload_log_to_bucket, load_data_to_bucket, postgres_loaded] >> cleanup


# Instantiate the DAG
bikeshare_etl_dag = bikeshare_etl_pipeline()
