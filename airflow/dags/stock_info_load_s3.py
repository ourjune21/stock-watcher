from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


import yfinance as yf
import pandas as pd
import logging
from io import StringIO, BytesIO


def s3_connection():
    try:
        s3_hook = S3Hook(aws_conn_id="s3_connection")
        logging.info("S3 bucket connected!")
        return s3_hook.get_conn()
    except Exception as e:
        logging.error(f"Failed to connect to S3: {e}")
        raise


# S3에서 파일을 읽어오는 함수
def read_s3_csv(bucket_name, key):
    try:
        s3_client = s3_connection()
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)

        # 객체에서 CSV 데이터 읽기
        csv_data = obj["Body"].read().decode("utf-8")

        # StringIO를 사용하여 pandas로 CSV 파일 읽기
        return pd.read_csv(StringIO(csv_data), index_col=0)
    except Exception as e:
        logging.error(f"Error reading file from S3: {e}")
        raise


@task
def extract():
    logging.info("Extracting ticker data from S3...")

    bucket_name = Variable.get("foreign_bucket")
    marketcap_key = "foreign/top_300_by_marketcap.csv"
    sector_key = "foreign/top_300_by_sector.csv"
    volume_key = "foreign/top_300_by_volume.csv"

    # S3에서 CSV 파일 읽기
    df_marketcap = read_s3_csv(bucket_name, marketcap_key)
    df_sector = read_s3_csv(bucket_name, sector_key)
    df_volume = read_s3_csv(bucket_name, volume_key)

    # ticker 추출
    tickers = list(
        set(
            df_volume["Symbol"].to_list()
            + df_sector["Symbol"].to_list()
            + df_marketcap["Symbol"].to_list()
        )
    )

    logging.info(f"Extracted {len(tickers)} tickers from S3.")
    return tickers


@task
def transform(tickers):
    logging.info("Transforming data...")
    start_date = "2024-10-22"
    end_date = "2024-11-22"
    rows = []

    for i, name in enumerate(tickers):
        # i = 0
        # name = tickers[0]
        try:
            logging.info(f"Downloading data for ticker: {name} ({i+1}/{len(tickers)})")
            df = yf.download(name, start=start_date, end=end_date, interval="1h")

            stock_info = yf.Ticker(name).info
            fullname = stock_info.get("longName", "N/A")

            for _, row in df.iterrows():
                rows.append(
                    {
                        "Name": name,
                        "Fullname": fullname,
                        "Date": row.name,
                        "Close": row.get("Close", None)[0],
                        "High": row.get("High", None)[0],
                        "Low": row.get("Low", None)[0],
                        "Open": row.get("Open", None)[0],
                        "Volume": row.get("Volume", None)[0],
                    }
                )
        except Exception as e:
            logging.error(f"Error processing ticker {name}: {e}")

    merged_df = pd.DataFrame(rows)
    merged_df = merged_df.to_json(orient="records")
    logging.info(f"Transformed data for {len(rows)} rows.")
    return merged_df


@task
def load(merged_df):
    # S3 버킷 정보
    bucket_name = Variable.get("foreign_bucket")    
    s3_csv_key = "foreign/complete/stock_info_complete.csv"
    s3_parquet_key = "foreign/complete/stock_info_complete.parquet"

    try:
        # merged_df가 JSON 형식이므로, 이를 DataFrame으로 변환
        merged_df = pd.read_json(merged_df, orient="records")
        # 리스트 형태가 있는지 확인하고, 해당 열의 값이 리스트라면 첫 번째 값을 추출합니다.
        for column in ["Close", "High", "Low", "Open", "Volume"]:
            merged_df[column] = merged_df[column].apply(
                lambda x: x[0] if isinstance(x, list) else x
            )

        # S3 클라이언트 연결
        s3 = s3_connection()

        # CSV 형식으로 S3에 저장
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=s3_csv_key, Body=csv_buffer.getvalue())
        logging.info(f"CSV data uploaded to S3 at {s3_csv_key}.")

        # Parquet 형식으로 S3에 저장
        parquet_buffer = BytesIO()
        merged_df.to_parquet(parquet_buffer, index=False)
        s3.put_object(
            Bucket=bucket_name, Key=s3_parquet_key, Body=parquet_buffer.getvalue()
        )
        logging.info(f"Parquet data uploaded to S3 at {s3_parquet_key}.")

    except Exception as e:
        logging.error(f"Error during load step: {e}")
        raise


with DAG(
    dag_id="stock_info_load_s3",
    start_date=datetime(2024, 11, 24),
    catchup=False,
    tags=["API"],
    schedule="0 * * * *",
) as dag:

    tickers = extract()
    stock_data = transform(tickers)
    load(stock_data)
