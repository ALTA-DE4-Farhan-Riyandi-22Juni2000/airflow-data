from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    'Farhan_Riyandi_Task_1', 
    description='Task 1',
    schedule_interval='0 */5 * * *',
    start_date=datetime(2022, 10, 21), 
    catchup=False
) as dag:
    # ti = task instance
    def push_variable_to_xcom(ti=None):
        ti.xcom_push(key='book_title', value='Data Engineer')
        ti.xcom_push(key='book_title1', value='Data Science')
        ti.xcom_push(key='book_title2', value='Data Analyst')


    def pull_multiple_value_once(ti=None):
        book_title = ti.xcom_pull(task_ids='push_variable_to_xcom', key='book_title')
        book_title1 = ti.xcom_pull(task_ids='push_variable_to_xcom', key='book_title1')
        book_title2 = ti.xcom_pull(task_ids='push_variable_to_xcom', key='book_title2')

        print(f'print book_title variable from xcom: {book_title}, {book_title1}, {book_title2}')

    push_variable_to_xcom = PythonOperator(
        task_id = 'push_variable_to_xcom',
        python_callable = push_variable_to_xcom
    )

    pull_multiple_value_once = PythonOperator(
        task_id = 'pull_multiple_value_once',
        python_callable = pull_multiple_value_once
    )

    push_variable_to_xcom >> pull_multiple_value_once