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
        try:
            pg_engine = ConnectionHook.get_pgAlchemy_engine("ubiqudiruma")
            sql = f"""select * from public.radacct
            where etl_date > timestamp '{start_time}'
            and etl_date <= timestamp '{end_time}'"""

            with pg_engine.connect() as connection:
                df = pd.read_sql(sql, connection, dtype_backend="pyarrow")

            pg_engine.dispose()

            df.to_parquet(
                path=f"/opt/airflow/data/ubiqudiruma/{start_time}_{end_time}.parquet",
                engine="pyarrow",
                compression="zstd",
                index=False,
            )
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
        stg_sql = f"""create or replace table stg_radacct AS
        with main_df as (
        select * from read_parquet('/opt/airflow/data/ubiqudiruma/{start_time}_{end_time}.parquet')
        )
        select
        acctsessionid,
        acctuniqueid,
        concat(acctuniqueid,';',username,';',acctstarttime) as adj_acctuniqueid,
        username,
        groupname,
        realm,
        acctstarttime,
        acctupdatetime,
        acctstoptime,
        IF(
            acctstarttime >= coalesce(acctstoptime, timestamp '9999-12-31 23:59:59') and acctsessiontime > 0,
            acctstarttime + (acctsessiontime * INTERVAL '1 second'),
            acctstoptime
            ) as adj_acctstoptime,
        IF(acctstarttime >= coalesce(acctstoptime, timestamp '9999-12-31 23:59:59') and acctsessiontime > 0,1,0) as is_time_adjusted,
        acctinterval,
        acctsessiontime,
        IF(
            acctstarttime >= coalesce(acctstoptime, timestamp '9999-12-31 23:59:59') and acctsessiontime = 0,
            abs(extract(epoch from age(acctstarttime,acctstoptime))),
            acctsessiontime
            ) as adj_acctsessiontime,
        IF(acctstarttime >= coalesce(acctstoptime, timestamp '9999-12-31 23:59:59') and acctsessiontime = 0,1,0) as is_sessiontime_adjusted,
        acctinputoctets,
        acctoutputoctets,
        calledstationid,
        acctstartdelay,
        acctstopdelay,
        extract_date,
        md5(
        concat(coalesce(acctstarttime,timestamp '9999-12-31 23:59:59'),';',
        coalesce(acctsessiontime,0),';',
        coalesce(acctinputoctets,0),';',
        coalesce(acctoutputoctets,0),';',
        username,';',
        acctuniqueid)
        ) as stg_hashdiff
        from main_df
        """
        intersection_sql = """
        create or replace table joined as
        select a.acctuniqueid,
            a.username,
            a.acctinputoctets,
            a.acctoutputoctets,
            a.extract_date
        from stg_radacct a
        inner join (select * from read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
            ) b
            on a.acctuniqueid = b.acctuniqueid
            and a.username = b.username
            and a.acctinputoctets = b.acctinputoctets
            and a.acctoutputoctets = b.acctoutputoctets
        qualify row_number() over
        (partition by a.username, a.acctstarttime, a.acctuniqueid order by coalesce(b.extract_date, timestamp '9999-12-31 23:59:59') desc) = 1
        """
        cleaned_sql = """
        create or replace table cleaned as
        select a.*
        from stg_radacct a
        where not exists (
            select 1
            from joined b
            where a.acctuniqueid = b.acctuniqueid
            )
        """
        track_sql = """
        create or replace table track_radacct as
        select
        s.*   ,
        case  when  c.acctuniqueid  is  null  then  1  else  0  end  new_ind ,
        case  when  c.acctuniqueid  is  not  null
        and  s.stg_hashdiff  <>  c.hashdiff  then  1  else  0  end  track_ind
        from
        cleaned s
        left  join  (select * from read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
            )  c
            on  s.username = c.username
            and s.acctstarttime = c.acctstarttime
            and s.acctuniqueid  =  c.acctuniqueid
        qualify row_number() over (partition by s.username, s.acctstarttime, s.acctuniqueid order by coalesce(c.extract_date, timestamp '9999-12-31 23:59:59') desc) = 1
        """
        newid_sql = """
        create or replace table newid as
        with tmp as (
        select
        acctsessionid,
        acctuniqueid,
        adj_acctuniqueid,
        username,
        groupname,
        realm,
        acctstarttime,
        lag(acctstarttime) over(partition by username order by acctstarttime asc, extract_date asc) as prev_acctstarttime,
        acctstoptime,
        adj_acctstoptime,
        lag(adj_acctstoptime) over(partition by username order by acctstarttime asc, extract_date asc) as prev_adj_acctstoptime,
        is_time_adjusted,
        acctupdatetime,
        acctsessiontime,
        adj_acctsessiontime,
        is_sessiontime_adjusted,
        acctinputoctets,
        acctoutputoctets,
        calledstationid,
        stg_hashdiff as hashdiff,
        lag(acctinputoctets) over(partition by username, adj_acctuniqueid order by extract_date) as prev_acctinputoctets,
        lag(acctoutputoctets) over(partition by username, adj_acctuniqueid order by extract_date) as prev_acctoutputoctets,
        extract_date,
        coalesce(IF(acctinputoctets - prev_acctinputoctets >= 0,acctinputoctets - prev_acctinputoctets, acctinputoctets),
            acctinputoctets) diff_acctinputoctets,
        coalesce(IF(acctoutputoctets - prev_acctoutputoctets >= 0, acctoutputoctets - prev_acctoutputoctets, acctoutputoctets),
            acctoutputoctets) diff_acctoutputoctets
        from track_radacct
        where new_ind = 1
        order by username, acctstarttime, extract_date
        )
        select *
        from tmp
        order by username, acctstarttime, extract_date
        """
        tracked_sql = """
        create or replace table trackedid as
        with tmp as (
        select
        t.acctsessionid,
        t.acctuniqueid,
        t.adj_acctuniqueid,
        t.username,
        t.groupname,
        t.realm,
        t.acctstarttime,
        f.acctstarttime as prev_acctstarttime,
        t.acctstoptime,
        t.adj_acctstoptime,
        f.adj_acctstoptime as prev_adj_acctstoptime,
        t.is_time_adjusted,
        t.acctupdatetime,
        t.acctsessiontime,
        t.adj_acctsessiontime,
        t.is_sessiontime_adjusted,
        t.acctinputoctets,
        t.acctoutputoctets,
        t.calledstationid,
        t.stg_hashdiff as hashdiff,
        f.acctinputoctets as prev_acctinputoctets,
        f.acctoutputoctets as prev_acctoutputoctets,
        t.extract_date,
        coalesce(IF(t.acctinputoctets - f.acctinputoctets >= 0, t.acctinputoctets - f.acctinputoctets, t.acctinputoctets),
            t.acctinputoctets) diff_acctinputoctets,
        coalesce(IF(t.acctoutputoctets - f.acctoutputoctets >= 0, t.acctoutputoctets - f.acctoutputoctets, t.acctoutputoctets),
            t.acctoutputoctets) diff_acctoutputoctets
        from track_radacct t
        left join (select * from read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
            ) f
            on t.username = f.username
            and t.acctstarttime = f.acctstarttime
            and t.acctuniqueid  =  f.acctuniqueid
            where t.track_ind = 1
            qualify row_number() over (partition by t.username, t.acctstarttime, t.acctuniqueid order by coalesce(f.extract_date, timestamp '9999-12-31 23:59:59') desc) = 1
            order by t.username, t.acctstarttime, t.extract_date
        )
        select *
        from tmp
        order by username, acctstarttime, extract_date
        """
        try:
            con = duckdb.connect()
            con.sql("BEGIN TRANSACTION;")
            logging.info("Creating stg_radacct table ...")
            con.execute(stg_sql)
            logging.info("Creating intersection table ...")
            con.execute(intersection_sql)
            logging.info("Creating cleaned table ...")
            con.execute(cleaned_sql)
            logging.info("Creating track_radacct table ...")
            con.sql(track_sql)
            logging.info("Creating newid table ...")
            con.sql(newid_sql)
            logging.info("Creating trackedid table ...")
            con.sql(tracked_sql)
            con.sql(
                """create table diruma_usage as
                    select * from read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
                    """
            )
            con.sql("insert into diruma_usage select * from newid")
            con.sql("insert into diruma_usage select * from trackedid")
            con.sql(
                """COPY (SELECT * FROM diruma_usage)
                TO '/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet'
                (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)"""
            )
            con.sql(
                """COPY (select acctsessionid,
                acctuniqueid,
                adj_acctuniqueid,
                username,
                realm,
                acctstarttime,
                acctinputoctets,
                acctoutputoctets,
                diff_acctinputoctets,
                diff_acctoutputoctets,
                extract_date
                from
                (SELECT * FROM newid
                UNION ALL SELECT * FROM trackedid))
                TO '/opt/airflow/data/ubiqudiruma/ubiqudiruma_analysis.parquet'
                (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)"""
            )
            con.sql("COMMIT;")
            con.close()
        except Exception as e:
            con.execute("ROLLBACK;")
            DefaultDAG.exception_alert(e, **kwargs)
            logging.exception("Transaction failed: %s", e)

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
