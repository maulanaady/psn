import os
import pendulum
from airflow.decorators import task
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.datasets import Dataset

with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@hourly",
    conn_id="telegram-dba",
    on_failure_callback=DefaultDAG.failure_callback,
    tags=["copy_radius", "ubiqudiruma"],
    max_active_runs=1,
    catchup=False,
) as dag:

    @task(outlets=[Dataset("ubiqudiruma")])
    def get_and_insert(data_interval_start=None, data_interval_end=None, **context):
        import pandas as pd
        import logging

        execution_start = data_interval_start.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        execution_end = data_interval_end.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        logging.info(
            "prev_data_interval_start_success: %s",
            (
                context["prev_data_interval_start_success"]
                .in_tz("Asia/Jakarta")
                .strftime("%Y-%m-%d %H:%M:%S")
            ),
        )
        logging.info(
            "prev_data_interval_end_success: %s",
            (
                context["prev_data_interval_end_success"]
                .in_tz("Asia/Jakarta")
                .strftime("%Y-%m-%d %H:%M:%S")
            ),
        )

        sql = f"""select *, current_timestamp() as extract_date,
        str_to_date('{execution_end}','%%Y-%%m-%%d %%H:%%i:%%S') as etl_date from rd.radacct"""
        sql_log = f"""select radacctid, acctsessionid, acctuniqueid, username,
        groupname, realm, nasipaddress, nasidentifier, nasportid, nasporttype,
        acctstarttime, acctupdatetime, acctstoptime, acctinterval, acctsessiontime,
        acctauthentic, connectinfo_start, connectinfo_stop, acctinputoctets,
        acctoutputoctets, calledstationid, callingstationid, acctterminatecause,
        servicetype, framedprotocol, framedipaddress, acctstartdelay, acctstopdelay,
        xascendsessionsvrkey, operator_name, created_date, flag,
        str_to_date('{execution_end}','%%Y-%%m-%%d %%H:%%i:%%S') as etl_date
        from rd.radacct_log
        where created_date > str_to_date('{execution_start}','%%Y-%%m-%%d %%H:%%i:%%S')
        and created_date <= str_to_date('{execution_end}','%%Y-%%m-%%d %%H:%%i:%%S')"""

        try:
            maria_engine = ConnectionHook.get_mariaAlchemy_engine("radius_ubiqudiruma")
            with maria_engine.connect() as connection:
                df = pd.read_sql(sql, connection, dtype_backend="pyarrow")
                df_log = pd.read_sql(sql_log, connection, dtype_backend="pyarrow")
            maria_engine.dispose()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)
            logging.error("MariaDB SQLAlchemy engine creation error: %s", e)

        # df.to_parquet(path=f'/opt/airflow/data/{filename}.parquet',
        #               engine='pyarrow', compression='snappy', index=False)
        try:
            pg_engine = ConnectionHook.get_pgAlchemy_engine("ubiqudiruma")
            with pg_engine.connect() as connection:
                df.to_sql(
                    "radacct",
                    connection,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                df_log.to_sql(
                    "radacct_log",
                    connection,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            pg_engine.dispose()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **context)
            logging.error("PostgreSQL SQLAlchemy engine creation error: %s", e)

    get_and_insert()
