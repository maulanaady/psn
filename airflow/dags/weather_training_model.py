import os
import pendulum
import logging
from airflow.operators.empty import EmptyOperator
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.decorators import task


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    default_args={"owner": "analysis"},
    tags=["jupyter", "analysis", "training", "weather"],
    schedule="0 17 * * *",
    max_active_runs=1,
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def prepare_training_data(conn_id, **context):
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            command = """cd /data/jupyter/notebook/analysis/dewa_petir/dags/ \
                && /data/jupyter/notebook/bin/papermill load_train_data.ipynb \
                "./out/load_train_data_out.ipynb" """
            _, stdout, stderr = ssh_client.exec_command(command)

            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())

            for line in iter(stderr.readline, ""):
                logging.info("%s", line.strip())
            ssh_client.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)

    @task
    def model_training(conn_id, location, **context):
        from datetime import datetime, timedelta

        dt = datetime.strftime(
            context["data_interval_end"] + timedelta(hours=7), "%Y%m%d"
        )
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            command = f"""cd /data/jupyter/notebook/analysis/dewa_petir/dags/ \
                && /data/jupyter/notebook/bin/papermill -p location '{location}' \
                -p id {dt} train_models.ipynb "./out/train_models_out_{location}.ipynb" """
            _, stdout, stderr = ssh_client.exec_command(command)

            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())

            for line in iter(stderr.readline, ""):
                logging.info("%s", line.strip())
            ssh_client.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)

    first_locations = [
        "Cikarang",
        "Jayapura",
        "Saukobye, Biak",
        "Morotal Utara",
        "Pontianak",
    ]
    second_locations = ["Manado", "Kupang", "Batam", "Tarakan", "Manokwari"]
    third_locations = ["Ambon", "Timika", "Banjarmasin"]

    (
        start
        >> prepare_training_data(conn_id="jupyter-host")
        >> model_training.partial(conn_id="jupyter-host").expand(
            location=first_locations
        )
        >> model_training.partial(conn_id="jupyter-host").expand(
            location=second_locations
        )
        >> model_training.partial(conn_id="jupyter-host").expand(
            location=third_locations
        )
    )
