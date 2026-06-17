from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition, KafkaException
import time

class KafkaHealthCheckOperator(BaseOperator):
    """
    Kiểm tra Broker, Topic và giám sát Trạng thái của Consumer Group.
    """

    def __init__(self, kafka_conn_id, target_topic, group_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.target_topic = target_topic
        self.group_id = group_id

    def execute(self, context):
        connection = BaseHook.get_connection(self.kafka_conn_id)

        conf = {
            'bootstrap.servers': connection.host,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': connection.login,
            'sasl.password': connection.password
        }

        admin_client = AdminClient(conf)

        try:
            # Kiểm tra Broker & Topic
            metadata = admin_client.list_topics(timeout=10)
            self.log.info(f"Kết nối thành công! Cụm Kafka có {len(metadata.brokers)} brokers đang chạy.")

            if self.target_topic not in metadata.topics:
                raise ValueError(f"LỖI: Không tìm thấy Topic '{self.target_topic}'!")

            self.log.info(f"Topic '{self.target_topic}' sẵn sàng.")

            # Kiểm tra Trạng thái Consumer Group
            group_futures = admin_client.describe_consumer_groups([self.group_id], request_timeout=10)

            for group, future in group_futures.items():
                try:
                    group_status = future.result()
                    state = str(group_status.state).upper()

                    self.log.info(f"Group '{group}' đang ở trạng thái: {state}")

                    if state == 'DEAD':
                        raise ValueError(f"LỖI: Consumer Group '{group}' đã CHẾT (DEAD)!")
                except Exception as e:
                    self.log.warning(f"Không thể đọc trạng thái của Group '{group}'. Lỗi: {e}")

        except KafkaException as e:
            raise Exception(f"LỖI MẠNG KAFKA: {str(e)}")


class KafkaDataFlowMonitorOperator(BaseOperator):
    """
    Tính toán Lag, Throughput (Tốc độ vào), và Processing Rate (Tốc độ ra) bằng cách dùng XCom.
    """

    def __init__(self, kafka_conn_id, target_topic, group_id, max_lag_threshold, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.target_topic = target_topic
        self.group_id = group_id
        self.max_lag_threshold = max_lag_threshold

    def execute(self, context):
        connection = BaseHook.get_connection(self.kafka_conn_id)

        conf = {
            'bootstrap.servers': connection.host,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': connection.login,
            'sasl.password': connection.password,
            'group.id': self.group_id,
            'enable.auto.commit': False
        }

        consumer = Consumer(conf)

        try:
            # Lấy số lượng partitions của Topic
            metadata = consumer.list_topics(topic=self.target_topic, timeout=10)
            partitions = metadata.topics[self.target_topic].partitions

            # Tạo danh sách các Partition của topic
            tps = [TopicPartition(self.target_topic, p) for p in partitions]

            # Lấy số lượng committed offsets của từng partition trong topic
            committed_offsets = consumer.committed(tps, timeout=10)

            total_high = 0
            total_committed = 0

            for partition in committed_offsets:
                # Lấy số thứ tự của message mới nhất vừa được đẩy vào (High Watermark) và số thứ tự của message cũ nhất hiện còn đang được lưu trữ (Low Watermark)
                low, high = consumer.get_watermark_offsets(partition, timeout=10)

                if partition.offset >= 0:
                    current_offset = partition.offset
                else:
                    current_offset = low

                # Tổng số message đã nhận được
                total_high += high

                # Tổng số message hiện tại đã commit offset
                total_committed += current_offset

            # Message chờ xử lý
            current_lag = total_high - total_committed
            current_time = time.time()
            self.log.info(f"CONSUMER LAG (Message chờ xử lý): {current_lag}")

            # ==============================================================
            # SỬ DỤNG AIRFLOW XCOM: TÍNH TOÁN THROUGHPUT & PROCESSING RATE
            # ==============================================================
            ti = context['ti']  # Lấy đối tượng Task Instance hiện tại

            # Pull dữ liệu của lần chạy trước của chính Task này
            prev_state = ti.xcom_pull(
                task_ids=self.task_id,
                key='kafka_flow',
                include_prior_dates=True
            )

            if prev_state:
                time_diff = current_time - prev_state['timestamp']
                high_diff = total_high - prev_state['high_watermark']
                committed_diff = total_committed - prev_state['committed_offset']

                if time_diff > 0:
                    # Tính tốc độ: Số message thay đổi / Số giây trôi qua
                    throughput = high_diff / time_diff
                    processing_rate = committed_diff / time_diff

                    self.log.info(f"THỐNG KÊ LƯU LƯỢNG (Trong {time_diff:.1f} giây qua):")
                    self.log.info(f" ➔ Tốc độ data đổ vào (Throughput): {throughput:.2f} msg/s")
                    self.log.info(f" ➔ Tốc độ xử lý (Processing Rate): {processing_rate:.2f} msg/s")

                    # Logic Báo động Nâng cao
                    if current_lag > 0 and processing_rate == 0:
                        self.log.warning("CẢNH BÁO: Kẹt data nhưng hệ thống đang ngừng xử lý hoàn toàn (Rate = 0)!")
            else:
                self.log.info("Lần chạy đầu tiên: Đang thu thập mốc thời gian, chưa thể tính tốc độ.")

            # Push dữ liệu của lần chạy này vào bộ nhớ để lần sau lấy ra dùng
            new_state = {
                'timestamp': current_time,
                'high_watermark': total_high,
                'committed_offset': total_committed
            }
            ti.xcom_push(key='kafka_flow', value=new_state)
            # ==============================================================

            # Kích hoạt báo động Lag truyền thống
            if current_lag > self.max_lag_threshold:
                raise ValueError(f"KẾT LUẬN: Lag ({current_lag}) vượt qua ngưỡng an toàn ({self.max_lag_threshold})!")

        except KafkaException as e:
            raise Exception(f"LỖI MẠNG KAFKA: {str(e)}")

        finally:
            consumer.close()