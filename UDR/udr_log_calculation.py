import time,datetime,logging,sys,os
import polars as pl
import duckdb,logging,requests
import psycopg2
from dotenv import load_dotenv

def get_logger():
    date_prefix = datetime.datetime.now().strftime("%Y%m%d")
    log_file = f"udr_calculation_{date_prefix}.log"
    log_path = os.path.join("/ubq_udr/udr_encoder/log/calculate/",log_file)
    logging.basicConfig(
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%A, %d %B %Y %H:%M:%S',
    handlers=[logging.StreamHandler(),
                logging.FileHandler(log_path, mode='a')],
    level=logging.DEBUG)

def send_to_telegram(message):
    apiToken = os.getenv('TELEGRAM_API_TOKEN')
    chatID = os.getenv('TELEGRAM_CHAT_ID')

    apiURL = f'https://api.telegram.org/bot{apiToken}/sendMessage'
    try:
        logging.info(message)
        requests.post(
            apiURL, json={'chat_id': chatID, 'text': f'udr_log_calculation.py (postgres) - {message}'})
    except Exception as e:
        logging.exception(e)

def create_pid_file(pid_file):
    pid = str(os.getpid())
    with open(pid_file, 'w') as pid_file:
        pid_file.write(pid)

def preparation(con,reset_time):
    logging.info("Try to create preparation logic and table in duckdb")
    con.execute("select min(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info(f"Original minimum end_date: {result.strftime('%Y-%m-%d %H:%M:%S')}")
    con.sql("BEGIN TRANSACTION;")
    con.sql(f"""
        delete from sec_df_duckdb_monthly
        where end_date <= timestamp '{reset_time}' - interval 40 day ;
            """)
    con.sql("COMMIT;")
    con.execute("select min(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info(f"New minimum end_date: {result.strftime('%Y-%m-%d %H:%M:%S')}")

    con.execute("select max(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info(f"Original max end_date: {result.strftime('%Y-%m-%d %H:%M:%S')}")
    con.sql("BEGIN TRANSACTION;")
    con.sql(f"""
        delete from sec_df_duckdb_monthly
        where END_DATE >= timestamp '{reset_time}' ;
            """)
    con.sql("COMMIT;")
    con.execute("select max(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info(f"New max end_date: {result.strftime('%Y-%m-%d %H:%M:%S')}")    
    con.sql("""create table if not exists sec_df_duckdb_monthly
        (id BIGINT,
        subscriber_number VARCHAR,
        end_date TIMESTAMP,
        clock BIGINT,
        OVERALL_USAGE_ANYTIME BIGINT,
        OVERALL_USAGE_OFFPEAK BIGINT,
        OVERALL_AVAILABLE_TOKENS BIGINT,
        load_time TIMESTAMP,
        UNIQUE(subscriber_number,end_date)
        )
        """)

    con.sql("""
            CREATE MACRO if not exists is_not_anomaly(curr_clock,curr_value,prev_clock,prev_value) AS 
            CASE WHEN (curr_value - prev_value) <= (100*(curr_clock - prev_clock)/60) AND prev_value >= 0 AND curr_value >= 0
	        THEN TRUE ELSE FALSE END"""
            )
    logging.info('Done preparation..')

def reset_summary_flag(start_time,con,connection,table):
    reset_time = (start_time - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    logging.info(f"Reset for date >= {reset_time}")
    try:
        cursor = connection.cursor()
        query = f"""
            update {table}
            set summary_flag = 0
            where END_DATE >= TO_TIMESTAMP('{reset_time} 00:00:00','YYYY-MM-DD HH24:MI:SS')
            -- and END_DATE < current_date
            and SUMMARY_FLAG <> 0
            and TERMINAL_STATUS in ('normal', 'minor')
        """
        cursor.execute(query)
        logging.info(f'Reseted flag records: {cursor.rowcount}')
        cursor.close()
        preparation(con,reset_time)
        connection.commit()
    except Exception as e:
        logging.error(f"An error occurred when reset summary_flag with description : {str(e)}")
        send_to_telegram(f"An error occurred when reset summary_flag with description : {str(e)}")
        con.close()
        connection.close()
        sys.exit(1)

def get_connection(username, password, port=5432):
    logging.info(f'try to get connection to UDR db ...')
    conn_uri = f"postgresql://{username}:{password}@172.20.12.150:{port},172.20.12.177:{port}/udr?target_session_attrs=read-write"
    try:
        connection = psycopg2.connect(conn_uri)
        logging.info(f'Connected to UDR db..')
        return connection
    except Exception as e:
        logging.exception(e)
        send_to_telegram(f"An error occurred when trying to connect to database with description : {str(e)}")
        sys.exit(1)

def read_main_data(table_name, cnx):
    logging.info('Reading main data ...')
    main_sql = f"""
    select 
    id,
    clock,
    subscriber_number,
    overall_usage_anytime,
    overall_usage_offpeak,
    overall_available_tokens,
    end_date,
    terminal_status
    from {table_name}
    where end_date >= current_date - interval '31' day
    and terminal_status in ('normal','minor')
    and summary_flag = 0
    order by end_date
    limit 100000
    """
    try:
        main_df = pl.read_database(query = main_sql,connection=cnx)
        logging.info(f'Done, {len(main_df)} rows')
        return main_df
    except Exception as e:
        logging.error(f'Error occured when reading main data with description: {str(e)}')
        send_to_telegram(f'Error occured when reading main data with description: {str(e)}')
        cnx.close()
        sys.exit(1)

def transform(con):
    con.execute("select min(end_date) from main_df")
    min_result = con.fetchone()[0]
    con.execute("select max(end_date) from main_df")
    max_result = con.fetchone()[0]
    logging.info(f"end_date period: {min_result.strftime('%Y-%m-%d %H:%M:%S')} - {max_result.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info('Start transforming data ...')
    try:
        results = con.sql("""
        with current_proccess as (
        SELECT 
        id,
        clock,
        end_date,
        subscriber_number, 
        terminal_status,
        overall_usage_anytime,
        overall_usage_offpeak,
        overall_available_tokens,
        lag(clock) over(partition by SUBSCRIBER_NUMBER order by end_date) as lag_clock,                     
        lag(OVERALL_USAGE_ANYTIME) over(partition by SUBSCRIBER_NUMBER order by end_date) as lag_usage_anytime,
        lag(OVERALL_USAGE_OFFPEAK) over(partition by SUBSCRIBER_NUMBER order by end_date) as lag_usage_offpeak,
        lag(OVERALL_AVAILABLE_TOKENS) over(partition by SUBSCRIBER_NUMBER order by end_date) as lag_available_tokens
        FROM main_df       
        where terminal_status in ('normal','minor')                                   
        ),
        sec_df as (
        select 
        subscriber_number,
        end_date,
        clock as clock,
        OVERALL_USAGE_ANYTIME as prev_usage_anytime,
        OVERALL_USAGE_OFFPEAK as prev_usage_offpeak,
        OVERALL_AVAILABLE_TOKENS as prev_available_tokens
        from sec_df_duckdb_monthly
        qualify row_number() over (partition by subscriber_number order by end_date desc) = 1
        ),                          
        temp as                     
        (select 
        cr.id,
        cr.subscriber_number,
        cr.terminal_status,
        cr.clock,
        cr.end_date,
        coalesce(cr.lag_clock,cast(prev.clock as int64)) prev_clock,
        cr.overall_usage_anytime,
        coalesce(cr.lag_usage_anytime,cast(prev.prev_usage_anytime as int64),0) prev_usage,
        is_not_anomaly(cr.clock,cr.overall_usage_anytime,prev_clock,prev_usage) is_not_anomaly_usage,
        case when prev_clock is not null 
            then
                case when cr.overall_usage_anytime >= prev_usage
                and is_not_anomaly(cr.clock,cr.overall_usage_anytime,prev_clock,prev_usage)
                    then cr.overall_usage_anytime - prev_usage
                else 0 end
            else cr.overall_usage_anytime end as v_usage_anytime,
        cr.overall_usage_offpeak,
        coalesce(cr.lag_usage_offpeak,cast(prev.prev_usage_offpeak as int64),0) prev_offpeak,
        is_not_anomaly(cr.clock,cr.overall_usage_offpeak,prev_clock,prev_offpeak) is_not_anomaly_offpeak,                     
        case when prev_clock is not null 
            then
                case when cr.overall_usage_offpeak >= prev_offpeak
                and is_not_anomaly(cr.clock,cr.overall_usage_offpeak,prev_clock,prev_offpeak)
                    then cr.overall_usage_offpeak - prev_offpeak
                else 0 end
            else cr.overall_usage_offpeak end as v_usage_offpeak,           
        cr.overall_available_tokens,
        coalesce(cr.lag_available_tokens,cast(prev.prev_available_tokens as int64),0) prev_tokens,
        is_not_anomaly(cr.clock,prev_tokens,prev_clock,cr.overall_available_tokens) is_not_anomaly_token,                     
        case when prev_clock is not null 
            then
                case
                    when cr.overall_usage_anytime < prev_usage and cr.overall_usage_offpeak < prev_offpeak 
                    and cr.overall_available_tokens < prev_tokens
                        then 0       
                    when cr.overall_available_tokens < prev_tokens and cr.overall_available_tokens <> 0
                    and is_not_anomaly(cr.clock,prev_tokens,prev_clock,cr.overall_available_tokens)
                        then prev_tokens - cr.overall_available_tokens
                else 0 end
            else 0 end as v_usage_available_tokens           
        from current_proccess cr
        left join sec_df prev
            on cast(cr.subscriber_number as varchar) = cast(prev.subscriber_number as varchar)
        )
        select 
        case when 
            subscriber_number is not null and 
            coalesce(overall_usage_anytime,overall_usage_offpeak,overall_available_tokens) is not null
            and lower(terminal_status) in ('normal','minor') then
                v_usage_anytime else null 
        end as v_usage_anytime,
        case when 
            subscriber_number is not null and 
            coalesce(overall_usage_anytime,overall_usage_offpeak,overall_available_tokens) is not null
            and lower(terminal_status) in ('normal','minor') then
                v_usage_offpeak else null 
        end as v_usage_offpeak,
        case when 
            subscriber_number is not null and 
            coalesce(overall_usage_anytime,overall_usage_offpeak,overall_available_tokens) is not null
            and lower(terminal_status) in ('normal','minor') then
                v_usage_available_tokens else null
            end as v_usage_available_tokens,
        case when 
            subscriber_number is not null and 
            coalesce(overall_usage_anytime,overall_usage_offpeak,overall_available_tokens) is not null 
            and lower(terminal_status) in ('normal','minor') then
                1 else -1
            end as summary_flag,
        current_date as date_modified, 
        'python_duckdb' as modified_by,
        id,
        end_date
        from temp
        union all
        select 
        NULL as v_usage_anytime,
        NULL as v_usage_offpeak,
        NULL as v_usage_available_tokens,
        -1 as summary_flag,
        current_date as date_modified, 
        'python_duckdb' as modified_by,
        id,
        end_date
        FROM main_df       
        where terminal_status not in ('normal','minor')                                                                                                                                                                        
        """).pl()
        imported = results.rows()
        logging.info(f'Done: {len(imported)} rows')
        return imported
    except Exception as e:
        logging.error(f'Error occured when transforming data with description: {str(e)}')
        send_to_telegram(f'Error occured when transforming data with description: {str(e)}')
        con.close()
        sys.exit(1)    

def update_duckdb(con):
    con.sql("BEGIN TRANSACTION;")
    con.sql("""insert into sec_df_duckdb_monthly
            select 
            id, subscriber_number,
            end_date, clock,
            OVERALL_USAGE_ANYTIME,
            OVERALL_USAGE_OFFPEAK,
            OVERALL_AVAILABLE_TOKENS,
            get_current_timestamp() as load_time
            from main_df
            ON CONFLICT (subscriber_number,end_date) DO UPDATE 
            SET id = EXCLUDED.id,
            clock = EXCLUDED.clock,
            OVERALL_USAGE_ANYTIME = EXCLUDED.OVERALL_USAGE_ANYTIME,
            OVERALL_USAGE_OFFPEAK = EXCLUDED.OVERALL_USAGE_OFFPEAK,
            OVERALL_AVAILABLE_TOKENS = EXCLUDED.OVERALL_AVAILABLE_TOKENS,
            load_time = EXCLUDED.load_time;
        """)
    con.sql("COMMIT;")
def update_records(duckdb_conn,table_name,records):
    logging.info('Start updating records ...')
    stmt = f"""update {table_name} as t set
    USAGE_ANYTIME = c.v_usage_anytime,
    USAGE_OFFPEAK = c.v_usage_offpeak,
    USAGE_AVAILABLE_TOKENS = c.v_usage_available_tokens,
    SUMMARY_FLAG = c.summary_flag,
    DATE_MODIFIED = now(),
    MODIFIED_BY = c.modified_by
    from (values """
    try:
        conn = get_connection(username=pg_username, password=pg_password)
        cursor = conn.cursor()
        args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in records)
        text = """) as c(v_usage_anytime, v_usage_offpeak,v_usage_available_tokens,
        summary_flag,
        date_modified,
        modified_by,
        id,
        end_date
        )
        where c.id = t.id
        and c.end_date = t.end_date"""
        cursor.execute(stmt + args_str + text)
        update_duckdb(duckdb_conn)
        conn.commit()
        logging.info(f'Updated records: {cursor.rowcount} rows')
        cursor.close()        
    except Exception as e:
        if isinstance(e,psycopg2.errors.DeadlockDetected):
            logging.warning(f'Deadlock found, retrying to update records ...')
            conn.rollback()
            conn.close()
            time.sleep(10)
            update_records(duckdb_conn,table_name,records)
        elif isinstance(e,psycopg2.errors.InFailedSqlTransaction):
            logging.warning(f'Previously transaction is aborted, retrying to insert records')
            conn.rollback()
            conn.close()
            time.sleep(10)
            update_records(duckdb_conn,table_name,records)
        else:
            logging.error(f'Other Error occured while updating records with description {str(e)}. Skipping the update process.')
            send_to_telegram(f'Other Error occured while updating records with description {str(e)}')
            conn.close()
            sys.exit(1)
    finally:
        conn.close()

    
def main():
    script_path = os.path.abspath(__file__)
    filename = os.path.basename(script_path)

    pid_file_path = f'/tmp/{filename}.pid'

    if os.path.exists(pid_file_path):
        logging.info("Script is already running. Exiting.\n\n\n")
        sys.exit(0)
    try:
        create_pid_file(pid_file_path)

        logging.info("Script is running!")
        logging.info("Starting Process")
        start_time = datetime.datetime.now()

        cnx = get_connection(username=pg_username, password=pg_password)
        duckdb_conn = duckdb.connect("/ubq_udr/udr_encoder/bin/py/duckdb.db")
        table_name = 'bb_usage.usa_trx_usage_data_records_log'

        if start_time.strftime('%H%M') == '0003' or start_time.strftime('%H%M') == '1803':
            send_to_telegram(f"""Start to reset summary_flag for END_DATE >= {(start_time - datetime.timedelta(days=1)).strftime('%Y-%m-%d')}""")
            reset_summary_flag(start_time,duckdb_conn,cnx,table_name)

        main_df = read_main_data(table_name,cnx)
        if len(main_df) == 0:
            cnx.close()
            duckdb_conn.close()
            logging.info("No new record exists.\n\n\n")
            sys.exit(0)
        results = transform(duckdb_conn)
        update_records(duckdb_conn,table_name,results)
        duckdb_conn.close()
        
    except Exception as e:
        send_to_telegram(f'An Error occured while running main step with description: {str(e)}')    
    finally:
        os.remove(pid_file_path)
        cnx.close()
        duckdb_conn.close()



if __name__ == '__main__':
    start_counter = time.perf_counter()

    get_logger()
    load_dotenv()
    pg_username = os.getenv('POSTGRES_USERNAME')
    pg_password = os.getenv('POSTGRES_PASSWORD')

    main()
    
    duration = time.perf_counter() - start_counter     
    converts = str(datetime.timedelta(seconds = duration))
    logging.info(f"Duration: {converts}.\n\n\n")

