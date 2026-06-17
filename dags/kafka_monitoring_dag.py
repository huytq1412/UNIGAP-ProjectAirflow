from airflow import DAG
from datetime import datetime, timedelta
from operators.kafka_operators import KafkaHealthCheckOperator, KafkaDataFlowMonitorOperator
from utils.telegram_alert import send_telegram_error_alert, send_telegram_success_alert

TARGET_TOPIC = 'product_view'

default_args = {
    'owner': 'huytq',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_telegram_error_alert
}

with DAG(
    dag_id='kafka_monitoring_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/10 * * * *',
    catchup=False,
    on_success_callback=send_telegram_success_alert
) as dag:

    # Task 1: Kiểm tra hạ tầng Kafka và kiểm tra trạng thái của luồng Mongo
    health_check = KafkaHealthCheckOperator(
        task_id='check_kafka_health',
        kafka_conn_id='kafka_local',
        target_topic=TARGET_TOPIC,
        group_id='local_consumer_group'
    )

    # Task 2: Đo Lag, Throughput, Processing Rate cho luồng MongoDB
    data_flow_monitor = KafkaDataFlowMonitorOperator(
        task_id='monitor_data_flow',
        kafka_conn_id='kafka_local',
        target_topic=TARGET_TOPIC,
        group_id='local_consumer_group',
        max_lag_threshold=10000
    )

    health_check >> data_flow_monitor