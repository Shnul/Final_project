# Final Project

This project involves setting up a data processing pipeline using various tools and technologies including Kafka, Airflow, Elasticsearch, Kibana, Minio, and more. Below are the steps to get started and the details of the running Docker containers.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. Clone the repository and navigate to the project directory:
    ```sh
    git clone https://github.com/Shnul/Final_project.git
    cd Final_Project
    ```

2. Start the Docker containers:
    ```sh
    docker-compose up -d
    ```

## Running Containers

Here are the details of the running Docker containers:

| Container ID | Image                                                  | Command                  | Ports                                      | Names                           |
|--------------|--------------------------------------------------------|--------------------------|--------------------------------------------|---------------------------------|
| c47583e7b6e8 | obsidiandynamics/kafdrop:3.30.0                        | "/kafdrop.sh"            | 0.0.0.0:9003->9000/tcp                     | final_project-kafdrop-1         |
| f8488541a221 | apache/airflow:2.0.0                                   | "/usr/bin/dumb-init …"   | 8080/tcp                                   | airflow_scheduler               |
| ee972806f7f3 | apache/airflow:2.0.0                                   | "/usr/bin/dumb-init …"   | 0.0.0.0:8082->8080/tcp                     | airflow_webserver               |
| 2c9207303664 | wurstmeister/kafka:2.13-2.8.1                          | "start-kafka.sh"         | 0.0.0.0:9092->9092/tcp                     | final_project-course-kafka-1    |
| 37697facab69 | docker.elastic.co/kibana/kibana:7.13.2                 | "/bin/tini -- /usr/l…"   | 0.0.0.0:5601->5601/tcp                     | final_project-kibana-1          |
| c414923412fe | minio/minio:RELEASE.2022-11-08T05-27-07Z               | "/usr/bin/docker-ent…"   | 0.0.0.0:9001->9000/tcp, 0.0.0.0:9002->9001/tcp | final_project-minio-1           |
| 6994e3f9e729 | docker.elastic.co/elasticsearch/elasticsearch:7.13.2   | "/bin/tini -- /usr/l…"   | 0.0.0.0:9200->9200/tcp, 9300/tcp           | final_project-elasticsearch-1   |
| b4f83f207932 | postgres:12                                            | "docker-entrypoint.s…"   | 0.0.0.0:5432->5432/tcp                     | final_project-postgres-1        |
| 7c6bb059c999 | ofrir119/developer_env:spark340_ssh                    | "/startup.sh"            | 0.0.0.0:4040-4042->4040-4042/tcp, 0.0.0.0:8888->8888/tcp, 0.0.0.0:22022->22/tcp | final_project-dev_env-1         |
| 08100ed2ae1a | wurstmeister/zookeeper:latest                          | "/bin/sh -c '/usr/sb…"   | 22/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:2182->2181/tcp | final_project-zookeeper-1       |

## Directory Structure

```plaintext
Final_Project/
├── .venv/
├── .vscode/
├── airflow-data/
├── dags/
├── jdk-11.0.2/
├── kafka_2.13-3.0.0/
├── Python/
├── Scripts/
├── Spark/
├── zookeeper_data/
├── .dockerrc
├── .env
├── docker-compose.yaml
└── requirements.txt
