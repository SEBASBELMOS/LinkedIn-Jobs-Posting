import os
from airflow.operators.bash import BashOperator
from typing import Tuple

def get_kafka_paths() -> Tuple[str, str]:
    current_script_dir = os.path.dirname(os.path.abspath(__file__))

    project_root_dir = os.path.abspath(os.path.join(current_script_dir, os.pardir, os.pardir))
    
    kafka_dir = os.path.join(project_root_dir, "kafka")
    
    kafka_compose_file = os.path.join(kafka_dir, "docker-compose.yml")
    
    return kafka_compose_file, kafka_dir

def start_kafka_infrastructure_task(task_id: str):
    kafka_compose_file_path, kafka_dir_path = get_kafka_paths()
    return BashOperator(
        task_id=task_id,
        bash_command=f"docker-compose -f {kafka_compose_file_path} up -d zookeeper kafka consumer dash",
        cwd=kafka_dir_path
    )

def run_kafka_producer_task(task_id: str):
    kafka_compose_file_path, kafka_dir_path = get_kafka_paths()
    return BashOperator(
        task_id=task_id,
        bash_command=f"docker-compose -f {kafka_compose_file_path} run --rm producer",
        cwd=kafka_dir_path
    )