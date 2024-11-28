import boto3
import yfinance as yf
import pandas as pd
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from airflow.models import Variable
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta
import pendulum

AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
AWS_REGION_NAME = Variable.get('aws_region_name')
def get_connection_to_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME
    )

def download_file_from_s3(bucket_name, key):
    s3_client = get_connection_to_s3()
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read()

def upload_file_to_s3(bucket_name, key, data):
    s3_client = get_connection_to_s3()
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)

def get_stock_df(row, start_date, end_date):
  stock = yf.Ticker(row['symbol'])
  df = stock.history(start = start_date, end = end_date, interval = "1h")
  df['Ticker'] = row['symbol']
  df['Fullname'] = row['fullname']
  return df

@task
def load_symbols(bucket_name, key):
    file_content = download_file_from_s3(bucket_name, key)
    buffer = io.BytesIO(file_content)
    table = pq.read_table(buffer)
    df = table.to_pandas()
    json_data = df.to_json(orient='records')
    return json_data

@task
def get_all_stock_df(json_data, start_date, end_date):
  print('start_date:', start_date)
  print('end_date:', end_date)
  df = pd.read_json(json_data)
  all_data = []
  for index,row in df.iterrows():
      print(row['symbol'])
      try:
          df = get_stock_df(row, start_date, end_date)
          all_data.append(df)
      except Exception as e:
          print(f"주가 데이터를 얻지 못함: {row['symbol'], e}")

  completed_df = pd.concat(all_data)
  completed_df = completed_df.reset_index(drop=False).rename(columns={'index': 'Datetime'})
  completed_df = completed_df[['Ticker', 'Fullname', 'Datetime', 'Close', 'High', 'Low', 'Open', 'Volume']]
  completed_json = completed_df.to_json(orient='records')
  return completed_json

@task
def save_to_s3(bucket_name, parquet_key, csv_key, json_data):
    df = pd.read_json(json_data)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    upload_file_to_s3(bucket_name, parquet_key, parquet_buffer.getvalue())
    print(f"Parquet data successfully saved to s3://{bucket_name}/{parquet_key}")

    upload_file_to_s3(bucket_name, csv_key, csv_buffer.getvalue())
    print(f"CSV data successfully saved to s3://{bucket_name}/{csv_key}")

with DAG(
    dag_id='kosdaq_yfinance_s3',
    start_date=datetime(2024, 11, 1),
    schedule_interval='15 1-6 * * 1-5',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    bucket_name = Variable.get('s3_bucket_name_authorization')
    input_key = 'domestic/top_300_kosdaq_stocks.parquet'
    parquet_key = f"domestic/domestic_kosdaq_300_by_marketcap.parquet"
    csv_key = f"domestic/domestic_kosdaq_300_by_marketcap.csv"
    start_date = "2024-11-01"
    end_date = (pendulum.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    json_data = load_symbols(bucket_name, input_key)
    completed_json = get_all_stock_df(json_data, start_date, end_date)
    save_task = save_to_s3(bucket_name, parquet_key, csv_key, completed_json)

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_kosdaq_s3_dw",
        trigger_dag_id="kosdaq_s3_dw",
        wait_for_completion=False,
        reset_dag_run=True
    )

    save_task >> trigger_next_dag

