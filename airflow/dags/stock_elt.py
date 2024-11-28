from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/stock_dbt"

def check_dbt_results(context):
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='dbt_test')
    if 'Failed' in str(result):
        raise Exception('DBT tests failed!')


conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id = 'stock_elt',
    start_date=datetime(2024, 11, 28),
    description='A sample Airflow DAG to invoke dbt runs using a BashOperator',
    schedule=None,
    catchup=False,
    tags=['ELT','dbt'],
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"/home/airflow/.local/bin/dbt deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",
    )

    dbt_deps >> dbt_run >> dbt_test


'''
사용방법
1. 각 dag 파일에 다음과 같이 TriggerDagRunOperator를 구현하고 task_id는 'trigger_stock_elt' 로 그대로 놔둔다. (같은 dag 파일에서만 task_id가 겹치지만 않으면 된다.)
2. trigger_dag_id는 'stock_elt' 도 그대로 놔둔다.
3. 마지막 작업으로 >> trigger_stock_elt 를 호출하여 dbt로 elt를 트리거한다.

    trigger_cryptocurrencies_elt = TriggerDagRunOperator(
        task_id='trigger_stock_elt',
        trigger_dag_id='stock_elt',
        wait_for_completion=False,
    )

ex)
    raw_data = extract(symbols)
    transformed_data = transform(raw_data)
    load(cursor, transformed_data, target_table) >> trigger_stock_elt
'''
