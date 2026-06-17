## Overview

Hướng dẫn này giúp bạn cài đặt Airflow bằng Docker

## 1. Create network & Build docker image
**Create network**

```shell
docker network create streaming-network --driver bridge
```

**Build custom docker image**

```
docker build -t unigap/airflow:2.10.4 .
```

## 2. Initializing Environment

### 2.1 Setting the right Airflow user

Tạo các thư mục sau: `dags`, `logs`, `plugins`, `config`

```shell
mkdir -p ./dags ./logs ./plugins ./config
```

Lấy thông tin user id sử dụng lệnh sau:

```shell
id -u
```

và group id của group `docker` sử dụng lệnh sau:

```shell
getent group docker
```

Set thông tin thu được vào 2 biến `AIRFLOW_UID` và `DOCKER_GID` trong file `.env`

### 2.2 Initialize airflow.cfg

```shell
docker compose run airflow-cli bash -c "airflow config list > /opt/airflow/config/airflow.cfg"
```

### 2.3 Initialize the database

```shell
docker compose up airflow-init
```

## 3. Running Airflow

```shell
docker compose up -d
```

Lệnh này sẽ start các docker containers sau:

airflow-scheduler - The scheduler monitors all tasks and dags, then triggers the task instances once their dependencies
are complete.

airflow-dag-processor - The DAG processor parses DAG files.

airflow-api-server - The api server is available at http://localhost:18080.

airflow-worker - The worker that executes the tasks given by the scheduler.

airflow-triggerer - The triggerer runs an event loop for deferrable tasks.

airflow-init - The initialization service.

postgres - The database.

redis - The redis - broker that forwards messages from scheduler to worker.

**Check Status**

```shell
docker compose ps
```

## 4. Accessing the web interface

[web interface](http://localhost:18080)

username/password: `airflow/airflow`

## References

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)