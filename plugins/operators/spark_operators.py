from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

class YarnHealthCheckOperator(BaseOperator):
    """
    Operator request tới REST API của YARN ResourceManager để kiểm tra sức khỏe của các NodeManagers và tài nguyên hiện có.
    """

    def __init__(self, resourcemanager_url, nodes_number, *args, **kwagrs):
        super().__init__(*args, **kwagrs)
        self.resourcemanager_url = resourcemanager_url
        self.nodes_number = nodes_number

    def execute(self, context):
        server_api = f"{self.resourcemanager_url}/ws/v1/cluster/metrics"
        self.log.info(f"Đang request tới YARN REST API: {server_api}")

        try:
            res = requests.get(server_api, timeout=10)
            # Báo lỗi nếu mã trả về không phải 200 OK
            res.raise_for_status()

            self.log.info(f"Master node (ResourceManager) vẫn đang sẵn sàng")

            cluster_metrics = res.json().get('clusterMetrics', {})
        except Exception as e:
            raise Exception(f"Không thể kết nối đến YARN ResourceManager: {str(e)}")

        # Số worker nodes đang hoạt động
        worker_nodes = cluster_metrics.get('activeNodes', 0)
        # Số CPU cores còn trống
        available_cores = cluster_metrics.get('availableVirtualCores', 0)
        # Số RAM còn khả dụng
        available_memory = cluster_metrics.get('availableMB', 0)

        self.log.info("YARN Cluster Metrics:")
        self.log.info(f" ➔ Active Worker Nodes: {worker_nodes} / {self.nodes_number}")
        self.log.info(f" ➔ Available Cores: {available_cores}")
        self.log.info(f" ➔ Available Memory: {available_memory} MB")

        if worker_nodes < self.nodes_number:
            raise ValueError(
                f"CẢNH BÁO: Số NodeManager hoạt động ({worker_nodes}) ít hơn dự kiến ({self.nodes_number}). Hãy kiểm tra lại hệ thống")

        if available_cores == 0:
            raise ValueError(f"CẢNH BÁO: Hết sạch CPU Core khả dụng. Hãy kiểm tra lại hệ thống")

        if available_cores == 0:
            raise ValueError(f"CẢNH BÁO: Hết sạch RAM khả dụng. Hãy kiểm tra lại hệ thống")

        self.log.info("Cụm YARN/Spark vẫn đang hoạt động ổn định.")


class PostgresDataOutputCheckOperator(BaseOperator):
    """
    Operator kiểm tra dữ liệu trên PostgreSQL. Hỗ trợ kiểm tra hàng loạt bảng (Dim và Fact) thông qua một Dict truyền vào.
    """

    # Khai báo cho phép Airflow quét và dịch các biến {{ ds }}
    template_fields = ('sql_queries',)

    def __init__(self, postgres_conn_id, sql_queries, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_queries = sql_queries

    def execute(self, context):
        self.log.info("Đang khởi động tiến trình KCS TOÀN DIỆN tại Postgres...")

        # Mở một đường kết nối duy nhất dùng chung cho tất cả các câu truy vấn
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Danh sách những bảng bị lỗi
        failed_checks = []

        for table_name, sql_query in self.sql_queries.items():
            self.log.info(f"Đang kiểm tra bảng: [{table_name}]")

            try:
                record = hook.get_first(sql_query)
                count = record[0] if record else 0

                self.log.info(f"Kết quả ({table_name}): {count} bản ghi.")

                if count == 0:
                    failed_checks.append(table_name)
                    self.log.error(f"Cảnh báo: Không có dữ liệu ở bảng {table_name}!")
                else:
                    self.log.info("Insert thành công. Dữ liệu đã nằm trong DB PostgreSQL")

            except Exception as e:
                raise Exception(f"Lỗi truy vấn PostgreSQL: {str(e)}")

        if failed_checks:
            failed_str = ", ".join(failed_checks)
            raise ValueError(f"Cảnh báo: Các bảng sau trống dữ liệu: {failed_str}")

        self.log.info("Kiểm tra hoàn tất. Toàn bộ các bảng Dim và Fact đều chứa dữ liệu hợp lệ!")