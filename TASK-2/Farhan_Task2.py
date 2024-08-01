from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json

dag = DAG(
    dag_id='alterra_farhan_Task2',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)

predict_gender = SimpleHttpOperator(
    task_id="profile_gender",
    endpoint="/gender/by-first-name-multiple",
    method="POST",
    data=json.dumps([
        {
            "first_name": "Farhan",
            "country": "ID"
        },
        {
            "first_name": "Mike",
            "country": "US"
        }
    ]),
    response_filter=lambda response: json.loads(response.text),
    http_conn_id="gender_api",
    log_response=True,
    dag=dag
)

# SQL command to create a table in PostgreSQL
create_table_in_db_task = PostgresOperator(
    task_id='create_table_in_db_farhan',
    sql=(
        'CREATE TABLE IF NOT EXISTS farhan_gender_name_prediction ('
        'input JSON, '
        'details JSON, '
        'result_found BOOLEAN, '
        'first_name VARCHAR(255), '
        'probability FLOAT, '
        'gender VARCHAR(50), '
        'timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP'
    ')'),
    postgres_conn_id='pg_conn_farhan',
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
    autocommit=True,
)

def loadDataToPostgres(**kwargs):
    ti = kwargs['ti']
    predictions = ti.xcom_pull(task_ids='profile_gender')
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_farhan')
    for prediction in predictions:
        input_data = json.dumps(prediction['input'])
        details_data = json.dumps(prediction['details'])
        result_found = prediction['result_found']
        first_name = prediction['first_name']
        probability = prediction['probability']
        gender = prediction['gender']
        pg_hook.run("""
            INSERT INTO farhan_gender_name_prediction (input, details, result_found, first_name, probability, gender)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, parameters=(input_data, details_data, result_found, first_name, probability, gender))
load_predictions_to_db_task = PythonOperator(
    task_id='profile_gender_to_postgres',
    python_callable=loadDataToPostgres,
    provide_context=True,
    dag=dag,
)

predict_gender >> create_table_in_db_task >> load_predictions_to_db_task
