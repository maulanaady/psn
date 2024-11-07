import os
from datetime import datetime, timedelta
import logging
import pendulum
from airflow.operators.empty import EmptyOperator
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.decorators import task


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=0),
    conn_id="telegram-dba",
    default_args={"owner": "analysis"},
    tags=["jupyter", "analysis", "inferencing", "weather"],
    schedule="@hourly",
    max_active_runs=1,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    @task(retries=3, retry_delay=timedelta(seconds=3))
    def prepare_inference_data(conn_id, **context):
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            command = """cd /data/jupyter/notebook/analysis/dewa_petir/dags/ \
                && /data/jupyter/notebook/bin/papermill load_test_data.ipynb \
                ./out/load_test_data_out.ipynb"""
            _, stdout, stderr = ssh_client.exec_command(command)
            # Fetch the output in real-time
            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())

            for line in iter(stderr.readline, ""):
                logging.info("%s", line.strip())
            ssh_client.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)

    @task(retries=3, retry_delay=timedelta(seconds=5))
    def model_inferencing(conn_id, location, **context):
        dt = datetime.strftime(
            context["data_interval_end"] + timedelta(hours=7), "%Y%m%d"
        )
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            command = f"""cd /data/jupyter/notebook/analysis/dewa_petir/dags/ \
                && /data/jupyter/notebook/bin/papermill -p location '{location}' \
                -p id {dt} forecast_models.ipynb \
                "./out/forecast_models_out_{location}.ipynb" """
            _, stdout, stderr = ssh_client.exec_command(command)

            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())

            for line in iter(stderr.readline, ""):
                logging.info("%s", line.strip())
            ssh_client.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)

    @task(retries=3, retry_delay=timedelta(seconds=5))
    def store_databases(conn_id, location, **context):
        dt = datetime.strftime(
            context["data_interval_end"] + timedelta(hours=7), "%Y-%m-%d %H:%M:%S"
        )
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            command = f"""cd /data/jupyter/notebook/analysis/dewa_petir/dags/ \
                && /data/jupyter/notebook/bin/papermill -p location '{location}' \
                -p id '{dt}' store_database.ipynb "./out/store_database_out_{location}.ipynb" """
            _, stdout, stderr = ssh_client.exec_command(command)

            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())

            for line in iter(stderr.readline, ""):
                logging.info("%s", line.strip())
            ssh_client.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)

    locations = [
        "Cikarang",
        "Jayapura",
        "Pontianak",
        "Manado",
        "Kupang",
        "Batam",
        "Tarakan",
        "Manokwari",
        "Ambon",
        "Timika",
        "Banjarmasin",
        "Morotal Utara",
        "Saukobye, Biak",
    ]

    (
        start
        >> prepare_inference_data(conn_id="jupyter-host")
        >> model_inferencing.partial(conn_id="jupyter-host").expand(location=locations)
        >> store_databases.partial(conn_id="jupyter-host").expand(location=locations)
    )
