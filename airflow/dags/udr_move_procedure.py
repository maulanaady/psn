import os
import logging
import pendulum
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.decorators import task


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=0),
    tags=["move udr", "procedure"],
    conn_id="telegram-dba",
    on_failure_callback=DefaultDAG.failure_callback,
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
) as dag:

    @task()
    def monitoring_move_udr(conn_id, uri, **context):
        try:
            conn = ConnectionHook.get_pg_connection(conn_id, uri=uri)
            with conn.cursor() as cur:
                cur.execute("CALL bb_usage.p_usa_move_udr_to_udr_log();")
            conn.commit()
            conn.close()
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)
            logging.exception(err)

    monitoring_move_udr("udr", uri=True)
