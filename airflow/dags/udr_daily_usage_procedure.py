import os
import logging
import pendulum
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    conn_id="telegram-dba",
    on_failure_callback=DefaultDAG.failure_callback,
    tags=["udr usage", "procedure"],
    schedule="0 19 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def sum_daily_usage(conn_id, uri, **context):
        try:
            conn = ConnectionHook.get_pg_connection(conn_id, uri=uri)
            with conn.cursor() as cur:
                cur.execute("CALL bb_usage.p_sum_daily_usage_caller();")
            conn.commit()
            conn.close()
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)
            logging.exception(err)

    sum_daily_usage_caller = sum_daily_usage("udr", uri=True)

    t3 = EmptyOperator(task_id="end")

    start >> sum_daily_usage_caller >> t3
