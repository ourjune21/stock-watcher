import boto3
import requests
import logging
import json
import time
from datetime import datetime, timedelta
from airflow.utils.timezone import convert_to_utc
# Import the timezone
from pendulum import timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task

from plugins import slack

# Stock configuration
STOCKS = [
    {'code': '005930', 'name': '삼성전자'},
    {'code': '000660', 'name': 'SK하이닉스'},
    {'code': '373220', 'name': 'LG에너지솔루션'},
    {'code': '207940', 'name': '삼성바이오로직스'},
    {'code': '005380', 'name': '현대차'}
]

def get_stock_price(stock_code, headers):
    """
    Fetch current stock price for a given stock code
    """
    MOCK_DOMAIN = "https://openapivts.koreainvestment.com:29443"
    API_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-time-itemconclusion"
    api_url = MOCK_DOMAIN + API_ENDPOINT

    current_time = datetime.now().strftime("%H%M%S")

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_HOUR_1": current_time
    }

    response = requests.get(api_url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return {
            'code': stock_code,
            'price': int(data.get('output1', {}).get('stck_prpr', 0)),
            'volume': int(data.get('output1', {}).get('acml_vol', 0)),
            'timestamp': datetime.now().isoformat()
        }
    else:
        logging.error(f"Failed to fetch stock price for {stock_code}")
        return None

@task
def extract_stock_prices(**context):
    """
    Extract stock prices for configured stocks
    """
    # Retrieve authentication tokens from Airflow variables
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN")
    APP_KEY = Variable.get("APP_KEY")
    APP_SECRET = Variable.get("APP_SECRET")

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHPST01060000"
    }

    # Fetch prices for all stocks
    stock_prices = []
    for stock in STOCKS:
        price_data = get_stock_price(stock['code'], headers)
        if price_data:
            price_data['name'] = stock['name']
            stock_prices.append(price_data)
            time.sleep(0.1)
    
    return stock_prices

@task
def save_to_s3(stock_prices, **context):
    """
    Save stock prices to S3 bucket with specific configuration
    """
    # S3 Configuration
    S3_BUCKET = Variable.get("S3_BUCKET")
    S3_FOLDER = "domestic/"
    
    # AWS Connection에서 AWS 자격증명 가져오기
    aws_conn_id = 'my_aws_connection'  # 설정한 Connection ID
    aws_connection = BaseHook.get_connection(aws_conn_id)
    aws_access_key_id = aws_connection.login
    aws_secret_access_key = aws_connection.password
    
    # Create S3 client with dynamic credentials
    s3_client = boto3.client(
        's3',
        region_name="us-west-2",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{S3_FOLDER}{timestamp}_stock_prices.json"
    
    # Save to S3
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=filename,
            Body=json.dumps(stock_prices, ensure_ascii=False, indent=2).encode('utf-8')
        )
        logging.info(f"Saved stock prices to {filename}")
        return stock_prices
    except Exception as e:
        logging.error(f"Failed to save to S3: {e}")
        raise  # Re-raise the exception for Airflow error handling

@task
def compare_and_notify(current_prices, **context):
    """
    Compare with previous prices and send Slack notification
    """
    # In a real scenario, you'd fetch previous prices from S3 or a database
    # This is a simplified example
    
    # Prepare notification message
    message = "📈 Stock Price Update:\n"
    for stock in current_prices:
        message += f"{stock['name']} ({stock['code']}): {stock['price']} KRW\n"
    
    # Send Slack notification
    slack.send_message(message)

with DAG(
    dag_id='stock_price_monitoring',
    start_date=datetime(2024, 11, 25, 9, 0, 0, tzinfo=timezone('Asia/Seoul')),  # Specify Korean time zone
    schedule_interval='*/5 0-9 * * 1-5',  # Adjusted for UTC
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    # Define task flow
    stock_prices = extract_stock_prices()
    saved_prices = save_to_s3(stock_prices)
    compare_and_notify(saved_prices)
