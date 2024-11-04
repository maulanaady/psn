"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
"""

import os
from datetime import timedelta
import pendulum
import jinja2
from default.default_config import DefaultDAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "operations",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    schedule="@daily",
    start_date=pendulum.datetime(2024, 5, 27, tz="Asia/Jakarta"),
    catchup=False,
    tags=["airflow-maintenance-dags"],
    template_undefined=jinja2.Undefined,
) as dag:
    if hasattr(dag, "doc_md"):
        dag.doc_md = __doc__
    if hasattr(dag, "catchup"):
        dag.catchup = False

    start = EmptyOperator(task_id="start", dag=dag)

    # date = "{{ ts }}"
    db_cleanup_op = SSHOperator(
        task_id="db_cleanup",
        ssh_conn_id="host-airflow",
        command='sudo docker exec airflow-worker /bin/bash -c "/opt/airflow/script/db_cleanup.sh {{ ts }}"',
        environment={"DATA_INTERVAL_START": "{{ ts }}"},
    )

    start >> db_cleanup_op
