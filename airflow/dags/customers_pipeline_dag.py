from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from docker.types import Mount


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customers_cleaning_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    params={
        "input_file": "customers_dirty3.csv"
    },
    tags=["dataops", "customers"],
) as dag:

    run_pipeline = DockerOperator(
    task_id="run_customers_pipeline",
    image="dataops-customers",
    api_version="auto",
    auto_remove=True,
    command="python src/clean_customers.py {{ params.input_file }}",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(
            source="/opt/airflow/data",
            target="/app/data",
            type="bind"
        )
    ],
    mount_tmp_dir=False,
)


    run_pipeline
