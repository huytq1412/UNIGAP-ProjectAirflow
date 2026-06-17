# Data Monitoring Pipeline (Project Airflow)

Dự án này là một hệ thống giám sát chất lượng dữ liệu tự động (Data Observability) được điều phối bởi **Apache Airflow**. Hệ thống chịu trách nhiệm kiểm tra tính toàn vẹn của dữ liệu từ các luồng streaming (Apache Kafka) và batch processing (Apache Spark / PostgreSQL), đồng thời cung cấp cơ chế cảnh báo (Alerting) theo thời gian thực qua Telegram.

## 🏗️ Tính năng nổi bật
* **Điều phối (Orchestration):** Sử dụng Apache Airflow để lập lịch và quản lý sự phụ thuộc giữa các task.
* **Giám sát Kafka Real-time (`kafka_monitoring_pipeline`):**
    * Chạy định kỳ mỗi 10 phút (`*/10 * * * *`).
    * Kiểm tra trạng thái Broker, tính khả dụng của topic `product_view` và Consumer Group `local_consumer_group`.
    * **Nổi bật:** Sử dụng **XCom** để lưu vết giữa các lần chạy, từ đó tính toán chính xác *Throughput* (Tốc độ data đổ vào) và *Processing Rate* (Tốc độ xử lý thực tế), đồng thời cảnh báo khi độ trễ (Lag) vượt ngưỡng 10.000 messages.
* **Giám sát Spark & Data Warehouse (`spark_monitoring_pipeline`):**
    * Chạy định kỳ hàng ngày (`@daily`).
    * Giao tiếp với YARN ResourceManager API để đảm bảo Worker Nodes, CPU Cores và Memory luôn sẵn sàng.
    * Sử dụng `PostgresHook` và cơ chế Template (`{{ ds }}`) để truy vấn trực tiếp DB, đảm bảo dữ liệu đầu ra của 4 bảng Dim (`dim_browser`, `dim_os`, `dim_country`, `country_mapping`) và 1 bảng Fact (`fact_product_views`).
* **Hệ thống Cảnh báo (Telegram Alerting):**
    * Bắn cảnh báo ngay lập tức khi một task thất bại (Task-level) hoặc khi toàn bộ luồng thành công (DAG-level).
    * Xử lý an toàn các ký tự đặc biệt trong Log bằng chuẩn `html`.

---
## 📁 Cấu trúc thư mục 

```text
ProjectAirflow/
├── airflow/                        # Chứa cấu hình hạ tầng Docker cho Airflow
│   ├── config/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements.txt
├── dags/                           # Định nghĩa các luồng công việc (DAGs)
│   ├── kafka_monitoring_dag.py     # Luồng công việc giám sát Kafka
│   └── spark_monitoring_dag.py     # Luồng công việc giám sát Spark
├── plugins/                        # Chứa các module mở rộng tự viết
│   ├── operators/
│   │   ├── kafka_operators.py      # Operators mở rộng giám sát các thuộc tính của Kafka
│   │   └── spark_operators.py      # Operators mở rộng giám sát các thuộc tính của Spark và kiểm tra dữ liệu Postgres
│   └── utils/
│       └── telegram_alerts.py      # Module xử lý API Telegram
├── pyproject.toml / poetry.lock    # File quản lý package local (Poetry)
├── .env.example                    # Biến môi trường mẫu
└── README.md                       # Tài liệu dự án
```
---
## 🗄️ Chi tiết Hạ tầng: Airflow
Hệ thống được đóng gói hoàn toàn bằng Docker Compose, triển khai theo mô hình phân tán CeleryExecutor, cho phép mở rộng khả năng xử lý song song (horizontal scaling) thay vì chạy cục bộ.

* Core Engine: Apache Airflow 2.10.4 (Python 3.11). Image gốc được build lại (Custom Dockerfile) để cài đặt sẵn OpenJDK-17, phục vụ các thư viện đòi hỏi môi trường JVM như pyspark và confluent-kafka.

* Mô hình Thực thi (CeleryExecutor):

  * Message Broker: Sử dụng Redis (7.2) làm hàng đợi (queue) để phân phối task.

  * Metadata Database: Sử dụng PostgreSQL 13 để lưu trữ trạng thái của các DAG/Task và cấu hình kết nối.

  * Worker Nodes: Các airflow-worker chịu trách nhiệm thực thi trực tiếp các operator độc lập. Có thể scale thêm worker tùy vào tải.

* Các Service Hỗ trợ:

  * Scheduler & Triggerer: Điều phối lịch chạy và hỗ trợ các Deferrable Operators (chờ sự kiện mà không chiếm dụng worker).

  * Monitoring UI: Tích hợp sẵn dịch vụ Flower (cổng 5555) để giám sát sức khỏe và luồng task của cụm Celery Workers, bên cạnh Webserver chính (cổng 18080).

* Mạng & Tích hợp (Networking & Volumes):

  * Network: Kết nối vào mạng ngoại vi streaming-network, đảm bảo các worker của Airflow có thể giao tiếp nội bộ trực tiếp với cụm Kafka và Hadoop/YARN.

  * Quản lý Dependencies: Quản lý tập trung qua requirements.txt (cho container build) và poetry (cho môi trường dev local), bao gồm các mảnh ghép (providers) chuyên dụng: apache-kafka, apache-spark, postgres.
---
## 🧠 Chi tiết các Logic Xử lý 
1. Module Kafka Operators (plugins/operators/kafka_operators.py)
* `KafkaHealthCheckOperator`:
Sử dụng thư viện `confluent_kafka.admin.AdminClient` để kết nối vào cụm Kafka (qua SASL_PLAINTEXT). Quét siêu dữ liệu (metadata) để xác nhận số lượng Broker đang sống và tìm kiếm Topic đích. Đồng thời, hàm `describe_consumer_groups` được gọi để bắt các Consumer Group đang rơi vào trạng thái DEAD.

* `KafkaDataFlowMonitorOperator`:
Lấy High Watermark (Offset mới nhất) và Low/Committed Offset từ các Partitions để tính tổng số Message đang chờ xử lý (current_lag).
Logic tính tốc độ: Sử dụng ti.xcom_pull để kéo thông số High Watermark và Timestamp của lần quét trước đó, sau đó tính toán phép chia để ra tốc độ Throughput và Processing Rate.

2. Module Spark/DB Operators (plugins/operators/spark_operators.py)
* `YarnHealthCheckOperator`:
Bắn HTTP GET Request tới REST API của YARN. Phân tích file JSON trả về để bóc tách 3 thông số lõi: activeNodes, availableVirtualCores, availableMB. So sánh với cấu hình kỳ vọng và ném ra ValueError nếu cạn kiệt tài nguyên.

* `PostgresDataOutputCheckOperator`:
Sử dụng PostgresHook để mở 1 luồng kết nối duy nhất tới Database. Lặp qua danh sách các bảng (Dictionary) và thực thi lệnh SELECT COUNT(1). Hàm hỗ trợ cơ chế Template của Airflow ({{ ds }}) để truy vấn linh động theo ngày chạy thực tế.

3. Module Alerting (plugins/utils/telegram_alerts.py)
* Sử dụng requests để gửi POST Request tới API của Telegram. Thu thập thông tin từ context của Airflow (Task ID, DAG ID, Exception Log). 
Dữ liệu gửi đi được bọc lại theo chuẩn "Parse Mode: HTML" của Telegram API. Hàm đi kèm với tham số timeout=10 và response.raise_for_status() để đảm bảo luồng Airflow không bị treo nếu mạng bị nghẽn.

---
## ⚙️ Hướng dẫn Cài đặt & Cấu hình
1. Yêu cầu hệ thống
* Python: >= 3.10
* Package Manager: Poetry
* Docker & Docker Compose (Để chạy cụm Airflow)

2. Khởi tạo Network Docker:
* Xem chi tiết trong file README.md của phần airflow/

3. Cấu hình bảo mật trên Airflow UI (Bắt buộc):
Hệ thống được thiết kế linh hoạt, đọc cấu hình trực tiếp từ hệ thống của Airflow thay vì Hard-code. Truy cập http://localhost:18080, đăng nhập và thực hiện các bước sau:

* Tạo Variables (Admin -> Variables):

  * telegram_bot_token: Token của Bot Telegram.

  * telegram_chat_id: ID của Group/User nhận cảnh báo.

* Tạo Connections (Admin -> Connections):

  * kafka_local:

    * Conn Type: Kafka (hoặc để trống nếu không có plugin UI, Airflow vẫn đọc được qua BaseHook).

    * Host: Địa chỉ bootstrap server (VD: broker:29092).

    * Login/Password: (Nếu có SASL_PLAINTEXT).

* postgres_local:

  * Conn Type: Postgres.

    * Host: postgres.

    * Schema: Tên Database (VD: airflow).

    * Login/Password: Tài khoản truy cập.

4. Khởi chạy Pipeline
* Bật (Unpause) 2 DAG `kafka_monitoring_pipeline` và `spark_monitoring_pipeline` trên giao diện để hệ thống bắt đầu quá trình giám sát.

---
## ⚠️ Hạn chế hiện tại (Limitations)
* Phụ thuộc vào Airflow XCom: Việc lưu trữ state (trạng thái quét Kafka trước đó) vào XCom của Airflow tuy tiện lợi nhưng nếu tần suất quét quá dày đặc sẽ làm phình to Metadata Database của Airflow.

* Hard-code các ngưỡng cảnh báo: Các giá trị như max_lag_threshold=10000 hay nodes_number=2 hiện đang được thiết lập tĩnh bên trong file code DAG. Việc thay đổi ngưỡng này yêu cầu phải sửa code và commit lại.

* Mức độ kiểm tra chất lượng dữ liệu (Data Quality): `PostgresDataOutputCheckOperator` hiện tại chỉ dừng lại ở mức độ kiểm tra sự tồn tại của dữ liệu (COUNT(1) > 0). Nó chưa thể phát hiện các dị thường như: Dữ liệu bị NULL, sai định dạng, hay vi phạm khóa chính.

---
## 🧩 Định hướng Cải tiến (Future Roadmap)
* Tham số hóa toàn bộ hệ thống: Chuyển toàn bộ các biến Hard-code (Thresholds) lên Airflow Variables hoặc sử dụng file YAML config rời. Điều này giúp các Data Operators có thể tự điều chỉnh độ nhạy của còi báo động mà không cần can thiệp vào mã nguồn.

* Tích hợp các Framework Data Quality Chuyên sâu: Nâng cấp Operator kiểm tra DB bằng cách tích hợp Great Expectations/Soda SQL. Khả năng mở rộng sẽ cho phép định nghĩa các Data Contracts rõ ràng hơn thay vì chỉ đếm số lượng bản ghi.

* Nâng cấp kênh Alerting: Bổ sung cơ chế thử lại cho hàm gọi Telegram API, tránh trường hợp rớt mạng tạm thời gây mất cảnh báo. Mở rộng thêm module để hỗ trợ MS Teams hoặc Slack bằng Factory Pattern.