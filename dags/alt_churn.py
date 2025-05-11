import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='alt_churn',
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,
        provide_context=True
    )

    create_table_task >> extract_task >> transform_task >> load_task