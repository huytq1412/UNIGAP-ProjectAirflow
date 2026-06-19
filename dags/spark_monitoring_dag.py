from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
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
        resourcemanager_url=Variable.get("yarn_resourcemanager_url"),
        nodes_number=int(Variable.get("yarn_nodes_number", default_var=2))
    )

    # Task 2: Kiểm tra toàn diện Dim & Fact
    verify_postgres_data = PostgresDataOutputCheckOperator(
        task_id='verify_postgres_data',
        postgres_conn_id='postgres_local',
        sql_queries={
            # Bảng Fact: Kiểm tra dữ liệu theo ngày thực thi của Airflow
            "fact_product_views": "SELECT COUNT(1) FROM fact_product_views WHERE date = '{{ ds }}';",

            # Các bảng Dim: Lọc dữ liệu có trong bảng fact mà ko có trong bảng dim vào CTE, count nếu = 0 (không tồn tại) thì trả về 1 (Pass)
            # Bảng dim_browser
            "dim_browser": """
                WITH fault_data AS (
                    SELECT 1 
                    FROM fact_product_views f
                    LEFT JOIN dim_browser d ON f.browser_id = d.id
                    WHERE f.date = '{{ ds }}' AND (d.id IS NULL OR f.browser_id IS NULL)
                )
                SELECT CASE WHEN COUNT(1) = 0 THEN 1 ELSE 0 END FROM fault_data;""",
            # Bảng dim_os
            "dim_os": """
                WITH fault_data AS (
                    SELECT 1 
                    FROM fact_product_views f
                    LEFT JOIN dim_os d ON f.os_id = d.id
                    WHERE f.date = '{{ ds }}' AND (d.id IS NULL OR f.os_id IS NULL)
                )
                SELECT CASE WHEN COUNT(1) = 0 THEN 1 ELSE 0 END FROM fault_data;""",
            # Bảng dim_country
            "dim_country": """
                WITH fault_data AS (
                    SELECT 1 
                    FROM fact_product_views f
                    LEFT JOIN dim_country d ON f.country_id = d.id
                    WHERE f.date = '{{ ds }}' AND (d.id IS NULL OR f.country_id IS NULL)
                )
                SELECT CASE WHEN COUNT(1) = 0 THEN 1 ELSE 0 END FROM fault_data;"""
        }
    )

    check_yarn_cluster >> verify_postgres_data