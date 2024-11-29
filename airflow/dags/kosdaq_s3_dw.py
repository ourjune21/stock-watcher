import boto3
import pandas as pd
from io import StringIO
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime

SNOWFLAKE_DATABASE = Variable.get('snowflake_database_authorization')
SNOWFLAKE_SCHEMA = Variable.get('snowflake_schema_authorization')

FILE_NAME = 'domestic_kosdaq_300_by_marketcap'
TABLE_NAME = f'{FILE_NAME}'
S3_BUCKET = Variable.get('s3_bucket_name_authorization')
S3_FILE_KEY = f'domestic/{FILE_NAME}.csv'

AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')
AWS_REGION_NAME = Variable.get('aws_region_name')

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

def get_s3_file(bucket_name, file_key):
    s3 = boto3.client(
        service_name="s3",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return data


def infer_schema(data):
    df = pd.read_csv(StringIO(data))
    schema = []
    for column in df.columns:
        dtype = df[column].dtype
        if pd.api.types.is_integer_dtype(dtype):
            column_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            column_type = 'FLOAT'
        elif pd.api.types.is_datetime64_any_dtype(dtype) or column in ['Date', 'Datetime']:
            column_type = 'TIMESTAMP'
        else:
            column_type = 'VARCHAR(255)'
        if column.upper() in ["LOW", "HIGH", "OPEN", "CLOSE"]:
            column = f'"{column}"'
        schema.append((column, column_type))
    return schema, df


def create_table(schema, table_name, cur):
    column_definitions = ", ".join([f"{col} {dtype}" for col, dtype in schema])
    cur.execute(f'DROP TABLE IF EXISTS {SNOWFLAKE_SCHEMA}.{table_name}')
    create_query = f"CREATE TABLE {SNOWFLAKE_SCHEMA}.{table_name} ({column_definitions});"
    cur.execute(create_query)


def load_data_to_snowflake(table_name, s3_bucket, s3_key, cur):
    copy_query = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
    FROM s3://{s3_bucket}/{s3_key}
    CREDENTIALS = (AWS_KEY_ID='{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """
    try:
        cur.execute(copy_query)
        print("Data loaded successfully.")
    except Exception as e:
        print("Error loading data:", e)


with DAG(
    dag_id='kosdaq_s3_dw',
    default_args={
        'retries': 1,
    },
    schedule_interval=None,
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    @task
    def fetch_s3_data():
        s3_data = get_s3_file(S3_BUCKET, S3_FILE_KEY)
        return s3_data

    @task
    def process_schema(s3_data: str):
        schema, df = infer_schema(s3_data)
        return {"schema": schema, "df_sample": df.to_json()}

    @task
    def snowflake_operations(schema_and_data: dict):
        # Snowflake 연결
        cur = return_snowflake_conn()
        try:
            cur.execute("BEGIN")
            print("Connected to Snowflake and warehouse activated.")
            schema = schema_and_data["schema"]

            create_table(schema, TABLE_NAME, cur)

            load_data_to_snowflake(TABLE_NAME, S3_BUCKET, S3_FILE_KEY, cur)
            cur.execute("COMMIT")
        except Exception as e:
            cur.execute("ROLLBACK")
            print(e)
            raise e

    s3_data = fetch_s3_data()
    schema_and_data = process_schema(s3_data)
    snowflake_operations(schema_and_data)
