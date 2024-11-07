import os
import pendulum
import logging
from airflow.decorators import task
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.datasets import Dataset


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=0),
    schedule=[Dataset("ubiqudiruma")],
    conn_id="telegram-dba",
    on_failure_callback=DefaultDAG.failure_callback,
    tags=["transform radius", "ubiqudiruma"],
    max_active_runs=1,
    catchup=False,
) as dag:

    @task()
    def read_data(data_interval_start=None, data_interval_end=None, **kwargs):
        import pandas as pd

        start_time = data_interval_start.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = data_interval_end.in_tz("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")
        logging.info(
            "Reading main data in radacct (postgres) for period %s - %s",
            start_time,
            end_time,
        )
        with open(
            "/opt/airflow/include/sql/ubiqudiruma/read_source_data.sql", "r"
        ) as f:
            sql_script = f.read()
        sql_script = sql_script.replace("{start_time}", start_time).replace(
            "{end_time}", end_time
        )
        commands = sql_script.split(";\n")
        try:
            pg_engine = ConnectionHook.get_pgAlchemy_engine("ubiqudiruma")
            with pg_engine.connect() as connection:
                for command in commands:
                    if command.strip():
                        df = pd.read_sql(
                            command.strip(), connection, dtype_backend="pyarrow"
                        )
                        df.to_parquet(
                            path=f"/opt/airflow/data/ubiqudiruma/{start_time}_{end_time}.parquet",
                            engine="pyarrow",
                            compression="zstd",
                            index=False,
                        )
            pg_engine.dispose()
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **kwargs)

    @task()
    def transform(data_interval_start=None, data_interval_end=None, **kwargs):
        import duckdb

        logging.info("Start transforming data ...")
        start_time = data_interval_start.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = data_interval_end.in_tz("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")
        with open(
            "/opt/airflow/include/sql/ubiqudiruma/diruma_usage_calculation.sql", "r"
        ) as f:
            sql_script = f.read()
        sql_script = sql_script.replace("{start_time}", start_time).replace(
            "{end_time}", end_time
        )
        commands = sql_script.split(";\n")

        con = duckdb.connect()
        try:
            con.sql("BEGIN TRANSACTION;")
            for command in commands:
                if command.strip():
                    con.execute(command.strip())
            con.sql("COMMIT;")
        except Exception as e:
            con.execute("ROLLBACK;")
            DefaultDAG.exception_alert(e, **kwargs)
            logging.exception("Transaction failed: %s", e)
        finally:
            con.close()

    @task()
    def insert_to_analysis(**kwargs):
        import pandas as pd

        try:
            maria_engine = ConnectionHook.get_mariaAlchemy_engine("analysis")
            df = pd.read_parquet(
                "/opt/airflow/data/ubiqudiruma/ubiqudiruma_analysis.parquet",
                engine="pyarrow",
                use_nullable_dtypes=True,
                dtype_backend="pyarrow",
            )
            with maria_engine.connect() as connection:
                df.to_sql(
                    "diruma_usage",
                    connection,
                    schema="bb_diruma",
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            maria_engine.dispose()
        except RuntimeError as e:
            logging.exception(e)
            DefaultDAG.exception_alert(e, **kwargs)

    @task()
    def calculate_usage(data_interval_start=None, data_interval_end=None, **kwargs):
        start_time = data_interval_start.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = data_interval_end.in_tz("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")
        try:
            pg_con = ConnectionHook.get_pg_connection("ubiqudiruma", uri=True)
            with pg_con.cursor() as cursor:
                cursor.execute(
                    "CALL public.ubiqudiruma_usage_calculation(%s, %s);",
                    (start_time, end_time),
                )
                pg_con.commit()
                cursor.execute(
                    "CALL public.ubiqudiruma_usage_calculation_tmp(%s, %s);",
                    (start_time, end_time),
                )
                pg_con.commit()
            pg_con.close()
        except RuntimeError as e:
            logging.exception(e)
            DefaultDAG.exception_alert(e, **kwargs)

    @task()
    def delete_file(data_interval_start=None, data_interval_end=None):
        start_time = data_interval_start.in_tz("Asia/Jakarta").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = data_interval_end.in_tz("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")

        os.remove(f"/opt/airflow/data/ubiqudiruma/{start_time}_{end_time}.parquet")
        os.remove("/opt/airflow/data/ubiqudiruma/ubiqudiruma_analysis.parquet")

    (
        read_data().as_setup()
        >> transform()
        >> [insert_to_analysis(), calculate_usage()]
        >> delete_file().as_teardown()
    )
