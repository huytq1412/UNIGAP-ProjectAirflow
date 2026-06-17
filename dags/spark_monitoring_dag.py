from airflow import DAG
from datetime import datetime, timedelta
from operators.spark_operators import YarnHealthCheckOperator, PostgresDataOutputCheckOperator
from utils.telegram_alert import send_telegram_error_alert, send_telegram_success_alert


default_args = {
    'owner': 'huytq',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_telegram_error_alert
}

with DAG(
    dag_id='spark_monitoring_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    on_success_callback=send_telegram_success_alert
) as dag:

    # Task 1: Kiểm tra cụm YARN/Spark
    check_yarn_cluster = YarnHealthCheckOperator(
        task_id='check_yarn_health',
        resourcemanager_url='http://resourcemanager:8088',
        nodes_number=2 # Hiện tại môi trường setup có 2 worker nodes
    )

    # Task 2: Kiểm tra toàn diện Dim & Fact
    verify_postgres_data = PostgresDataOutputCheckOperator(
        task_id='verify_postgres_data',
        postgres_conn_id='postgres_local',
        sql_queries={
            # Các bảng Dim: Chỉ kiểm tra xem có dữ liệu không
            "country_mapping": "SELECT COUNT(1) FROM country_mapping;",
            "dim_browser": "SELECT COUNT(1) FROM dim_browser;",
            "dim_os": "SELECT COUNT(1) FROM dim_os;",
            "dim_country": "SELECT COUNT(1) FROM dim_country;",
            # Bảng Fact: Kiểm tra dữ liệu theo ngày thực thi của Airflow
            "fact_product_views": "SELECT COUNT(1) FROM fact_product_views WHERE date <= '{{ ds }}';"
        }
    )

    check_yarn_cluster >> verify_postgres_data