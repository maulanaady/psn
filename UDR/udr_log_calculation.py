"""
This script performs data processing and logging for user data records (UDR) calculation using PostgreSQL and DuckDB.
It imports necessary libraries for time management, logging, database connections, and data manipulation.
Modules imported:
- time: Provides time-related functions.
- datetime: Supplies classes for manipulating dates and times.
- logging: Supports logging of events for tracking and debugging.
- sys: Provides access to system-specific parameters and functions.
- os: Offers functions for interacting with the operating system, such as file paths.
- polars: A DataFrame library for fast data processing.
- duckdb: A high-performance database management system for analytics.
- requests: Simplifies HTTP requests to external APIs.
- psycopg2: Enables interaction with PostgreSQL databases.
- dotenv: Loads environment variables from a .env file for configuration management.
"""
import datetime
import logging
import os
import sys
import time
from contextlib import contextmanager

import duckdb
import polars as pl
import psycopg2
import requests
from dotenv import load_dotenv


def get_logger(log_file: str):
    """Create a log file with name log_file.

    Args:
        log_file (str): The log file to be created.
    """
    date_prefix = datetime.datetime.now().strftime("%Y%m%d")
    log_file = f"{log_file}_{date_prefix}.log"
    log_path = os.path.join("/ubq_udr/udr_encoder/log/calculate/", log_file)
    logging.basicConfig(
        format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
        datefmt="%A, %d %B %Y %H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode="a")],
        level=logging.DEBUG,
    )


def send_to_telegram(message):
    """Send a message to a Telegram chat via the Telegram Bot API.

    This function sends a log message to a specified Telegram chat.

    Args:
        message (str): The message to send.
        logger (logging.Logger): A logger instance for logging information.

    Raises:
        Exception: Any exceptions raised during the request to the Telegram API.
    """
    api_token = os.getenv("TELEGRAM_API_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    api_url = f"https://api.telegram.org/bot{api_token}/sendMessage"
    try:
        logging.info(message)
        requests.post(
            api_url,
            json={
                "chat_id": chat_id,
                "text": f"udr_log_calculation.py (postgres) - {message}",
            },
            timeout=10,
        )
    except Exception as e:
        logging.exception(e)


def create_pid_file(pid_file):
    """Create a PID file with the current process ID.

    Args:
        pid_file (str): The path to the PID file to be created.
    """
    pid = str(os.getpid())
    with open(pid_file, "w", encoding="utf-8") as f:
        f.write(pid)


def preparation(con, reset_time):
    """Prepare the DuckDB database by cleaning up old records and
    setting up the necessary table and macros.

    This function performs the following actions:
    1. Creates the `sec_df_duckdb_monthly` table if it does not exist.
    2. Creates a macro `is_not_anomaly` if it does not exist to evaluate
       whether a value is not an anomaly based on specific criteria.
    3. Logs the original minimum `end_date` from the `sec_df_duckdb_monthly` duckdb table.
    4. Deletes records from the `sec_df_duckdb_monthly` table where the `end_date`
       is older than 40 days from the specified `reset_time`.
    5. Logs the new minimum `end_date` after the deletion.
    6. Logs the original maximum `end_date` from the `sec_df_duckdb_monthly` table.
    7. Deletes records from the `sec_df_duckdb_monthly` table where the
       `end_date` is newer than the specified `reset_time`.
    8. Logs the new maximum `end_date` after the deletion.

    Args:
        con (duckdb.Connection): The DuckDB connection object used to execute SQL commands.
        reset_time (str): The timestamp used as a reference for deleting records from the table,
        expected in a format compatible with SQL timestamp.

    Returns:
        None: The function does not return any value; it performs actions directly on the database.

    Raises:
        Exception: Any exceptions raised during database operations will propagate,
        allowing for error handling upstream.
    """
    logging.info("Try to create preparation logic and table in duckdb")
    con.sql(
        """create table if not exists sec_df_duckdb_monthly
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
        """
    )
    con.sql(
        """
        CREATE MACRO if not exists is_not_anomaly(curr_clock,curr_value,prev_clock,prev_value) AS
        CASE WHEN (curr_value - prev_value) <= (100*(curr_clock - prev_clock)/60)
        AND prev_value >= 0 AND curr_value >= 0
        THEN TRUE ELSE FALSE END"""
    )
    con.execute("select min(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info("Original minimum end_date: %s", result.strftime("%Y-%m-%d %H:%M:%S"))
    con.sql("BEGIN TRANSACTION;")
    con.sql(
        f"""
        delete from sec_df_duckdb_monthly
        where end_date <= timestamp '{reset_time}' - interval 40 day ;
            """
    )
    con.sql("COMMIT;")
    con.execute("select min(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info("New minimum end_date: %s", result.strftime("%Y-%m-%d %H:%M:%S"))

    con.execute("select max(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info("Original max end_date: %s", result.strftime("%Y-%m-%d %H:%M:%S"))
    con.sql("BEGIN TRANSACTION;")
    con.sql(
        f"""
        delete from sec_df_duckdb_monthly
        where END_DATE >= timestamp '{reset_time}' ;
            """
    )
    con.sql("COMMIT;")
    con.execute("select max(end_date) from sec_df_duckdb_monthly")
    result = con.fetchone()[0]
    logging.info("New max end_date: %s . Done", result.strftime("%Y-%m-%d %H:%M:%S"))


def reset_summary_flag(start_time, con, connection, table):
    """Reset the `summary_flag` for specific records
       in the given table and perform data preparation.

    This function performs the following actions:
    1. Resets the `summary_flag` to 0 for records in the specified `table` where:
       - The `END_DATE` is greater than or equal to the previous day's date (`reset_time`).
       - The `SUMMARY_FLAG` is not 0.
       - The `TERMINAL_STATUS` is either 'normal' or 'minor'.
    2. Logs the number of records updated with the reset `summary_flag`.
    3. Calls the `preparation` function to perform additional
       cleanup and setup in the DuckDB database.
    4. Commits the transaction to the database after successful updates.
    5. Logs and sends a message to Telegram in case of any errors,
       closes connections, and exits the program.

    Args:
        start_time (datetime.datetime): The reference time used to calculate `reset_time` (the previous day) for resetting the summary flag.
        con (duckdb.Connection): The DuckDB connection object used for executing queries related to preparation.
        connection (psycopg2.connection): The PostgreSQL connection object used to execute the update query.
        table (str): The name of the table where the `summary_flag` will be reset.

    Raises:
        SystemExit: If an exception occurs, the function will log the error,
        send a notification, close connections, and exit the program.

    Returns:
        None: The function does not return any value but performs operations directly
        on the database and logs the actions.
    """
    reset_time = (start_time - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info("Reset for date >= %s", reset_time)
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
        logging.info("Reseted flag records: %s", cursor.rowcount)
        cursor.close()
        preparation(con, reset_time)
        connection.commit()
    except Exception as e:
        logging.exception(e)
        send_to_telegram(f"An error occurred when reset summary_flag with description : {str(e)}")
        con.close()
        connection.close()
        sys.exit(1)


@contextmanager
def get_connection(username, password, port=5432):
    """Establish a connection to the UDR PostgreSQL database with retry logic.

    This function attempts to connect to a UDR PostgreSQL database using the provided credentials.
    If the connection fails, it will retry up to `max_retries` times with
    a delay between each attempt.
    Upon a successful connection, the connection object is yielded for use. If all retries fail,
    an error message is sent via Telegram, and an exception is raised.

    Args:
        username (str): The database username used to authenticate the connection.
        password (str): The password for the database user.
        port (int, optional): The port number on which the PostgreSQL server is listening. Defaults to 5432.

    Yields:
        psycopg2.connection: A connection object to the PostgreSQL database.

    Raises:
        Exception: If all retry attempts to connect to the database fail,
        an exception is raised after sending an error notification to Telegram.

    Retries:
        The function will attempt to connect to the database up to
        `max_retries` times, with `retry_delay` seconds between attempts.
        If the connection cannot be established after all attempts, it will send an alert to
        Telegram and raise an exception.

    Example:
        Usage with a context manager to handle the connection:

        ```python
        with get_connection('username', 'password') as conn:
            # Perform database operations using `conn`
        ```

    Logs:
        - Logs each attempt to connect and any errors that occur.
        - Logs the successful establishment of a connection or the final failure.
    """
    logging.info("try to get connection to UDR db ...")
    conn_uri = (
        f"postgresql://{username}:{password}@"
        f"172.20.12.150:{port},"
        f"172.20.12.177:{port}/udr?target_session_attrs=read-write"
    )
    max_retries, retry_delay = 10, 3
    attempts = 0
    while attempts < max_retries:
        try:
            connection = psycopg2.connect(conn_uri)
            logging.info("Connected to UDR db..")
            yield connection  # Yield the connection to the caller
            break
        except Exception as e:
            attempts += 1
            logging.error("Error connecting to database (attempt %s - %s )", attempts, str(e))
            if attempts < max_retries:
                time.sleep(retry_delay)  # Wait before retrying
            else:
                send_to_telegram(
                    f"An error occurred when trying to connect to database with description : {str(e)}"
                )
                raise
        finally:
            if connection:
                connection.close()


def read_main_data(table_name, cnx):
    """Read main data from the specified database table.

    This function retrieves records from the provided table, filtering by conditions such as `end_date`,
    `terminal_status`, and `summary_flag`. The data is ordered by `end_date` and limited to 100,000 rows.
    If an error occurs during data retrieval, an error message is logged, a notification is sent via Telegram,
    and the program exits.

    Args:
        table_name (str): The name of the database table to query.
        cnx (psycopg2.connection): A database connection object.

    Returns:
        polars.DataFrame: A dataframe containing the queried data.

    Raises:
        SystemExit: If an error occurs while reading the data, the program will exit after sending an error notification to Telegram.

    SQL Query:
        The function runs the following SQL query:
        ```sql
        SELECT
            id,
            clock,
            subscriber_number,
            overall_usage_anytime,
            overall_usage_offpeak,
            overall_available_tokens,
            end_date,
            terminal_status
        FROM {table_name}
        WHERE end_date >= current_date - interval '31' day
        AND terminal_status IN ('normal', 'minor')
        AND summary_flag = 0
        ORDER BY end_date
        LIMIT 100000;
        ```

    Example:
        ```python
        df = read_main_data('usage_table', cnx)
        ```

    Logs:
        - Logs the start and successful completion of the data retrieval process,
          including the number of rows retrieved.
        - If an error occurs, the error is logged, a message is sent to Telegram,
          and the connection is closed before exiting.
    """
    logging.info("Reading main data ...")
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
        main_df = pl.read_database(query=main_sql, connection=cnx)
        logging.info("Done, %s rows", len(main_df))
        return main_df
    except Exception as e:
        logging.exception(e)
        send_to_telegram(f"Error occured when reading main data with description: {str(e)}")
        cnx.close()
        sys.exit(1)


def transform(con):
    """Transform data from the main dataset and apply business rules for usage calculations.

    This function processes data from the `main_df` table and computes derived fields based on
    complex logic involving previous and current usage values. It filters data based on specific
    criteria, such as `terminal_status`, and calculates usage metrics like `v_usage_anytime`,
    `v_usage_offpeak`, and `v_usage_available_tokens`. The transformation process is performed
    using SQL queries, and the results are returned as a list of records.

    Args:
        con (duckdb.Connection): A DuckDB connection object for executing queries.

    Returns:
        list: A list of rows containing the transformed data.

    Raises:
        SystemExit: If an error occurs during the transformation process, an error is logged, a
                    message is sent to a Telegram bot, and the program exits.

    SQL Queries:
        - Retrieves the minimum and maximum `end_date` from the `main_df` table.
        - Performs a series of joins and calculations, applying custom business logic to
          determine if certain values are anomalies (via `is_not_anomaly` macro) and to compute
          various usage metrics.
        - The results of the transformation are returned in a structured format.

    Example:
        ```python
        transformed_data = transform(con)
        ```

    Logs:
        - Logs the range of `end_date` before starting the transformation.
        - Logs the number of rows processed after the transformation is complete.
        - If an error occurs, it logs the error, sends a notification to Telegram,
          and exits the program.

    Key Logic:
        - **Anomaly Detection**: Utilizes a custom `is_not_anomaly` macro to validate whether the
          current and previous usage values are consistent.
        - **Usage Calculations**: Computes actual usage metrics (`v_usage_anytime`, `v_usage_offpeak`,
          `v_usage_available_tokens`) by comparing current and previous values, and ensuring they
          are within expected thresholds.
        - **Summary Flag**: Assigns a `summary_flag` based on the terminal status and the availability
          of usage data.
    """
    con.execute("select min(end_date) from main_df")
    min_result = con.fetchone()[0]
    con.execute("select max(end_date) from main_df")
    max_result = con.fetchone()[0]
    logging.info(
        "end_date period: %s - %s",
        min_result.strftime("%Y-%m-%d %H:%M:%S"),
        max_result.strftime("%Y-%m-%d %H:%M:%S"),
    )
    logging.info("Start transforming data ...")
    try:
        results = con.sql(
            """
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
        """
        ).pl()
        imported = results.rows()
        logging.info("Done: %s rows", len(imported))
        return imported
    except Exception as e:
        logging.error(e)
        send_to_telegram(f"Error occured when transforming data with description: {str(e)}")
        con.close()
        sys.exit(1)


def update_duckdb(con):
    """Update the `sec_df_duckdb_monthly` table with new data from the `main_df` table.

    This function inserts data from the `main_df` table into the `sec_df_duckdb_monthly` table.
    If a record with the same `subscriber_number` and `end_date` already exists, it updates
    the existing record with new values from `main_df`. The operation is performed within
    a transaction to ensure atomicity.

    Args:
        con (duckdb.Connection): A DuckDB connection object for executing queries.

    Returns:
        None

    SQL Queries:
        - Inserts new records from the `main_df` table into the `sec_df_duckdb_monthly` table.
        - If a record already exists with the same `subscriber_number` and `end_date`, it
          updates the existing record with new values (`id`, `clock`, `OVERALL_USAGE_ANYTIME`,
          `OVERALL_USAGE_OFFPEAK`, `OVERALL_AVAILABLE_TOKENS`, `load_time`).
        - The query is executed within a transaction (`BEGIN` and `COMMIT`).

    Example:
        ```python
        update_duckdb(con)
        ```

    Logs:
        None by default, though logging could be added to track the status of the transaction
        or handle errors if desired.

    Key Logic:
        - **Transaction Management**: The operation is enclosed within a transaction to ensure
          that the insert/update happens atomically.
        - **Conflict Handling**: Uses the `ON CONFLICT` clause to update existing records
          when there are duplicate keys (`subscriber_number`, `end_date`).
        - **Timestamps**: Automatically sets the `load_time` to the current timestamp
          (`get_current_timestamp()`).
    """
    con.sql("BEGIN TRANSACTION;")
    con.sql(
        """insert into sec_df_duckdb_monthly
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
        """
    )
    con.sql("COMMIT;")


def update_records(duckdb_conn, table_name, records):
    """
    Updates multiple records in a PostgreSQL table and synchronizes the changes with DuckDB.

    This function takes a list of records and updates the corresponding entries in the given
    PostgreSQL `table_name`. The records are passed in as a list of tuples. After updating the
    PostgreSQL table, it triggers an update in DuckDB for consistency. If any deadlocks or
    failed transactions are detected, the function retries after a delay.

    Args:
        duckdb_conn (duckdb.Connection): A DuckDB connection object for executing the `update_duckdb` function.
        table_name (str): The name of the PostgreSQL table to be updated.
        records (list): A list of tuples containing the new values for updating the records.
                        Each tuple follows the format (v_usage_anytime, v_usage_offpeak,
                        v_usage_available_tokens, summary_flag, date_modified, modified_by, id, end_date).

    Example:
        ```python
        records = [
            (100, 50, 10, 1, '2023-10-01', 'system', 1, '2023-09-30'),
            (150, 75, 20, 1, '2023-10-02', 'system', 2, '2023-09-30')
        ]
        update_records(duckdb_conn, 'my_table', records)
        ```

    SQL Queries:
        - **Update Statement**: Updates the PostgreSQL table by setting the values for `USAGE_ANYTIME`,
          `USAGE_OFFPEAK`, `USAGE_AVAILABLE_TOKENS`, `SUMMARY_FLAG`, `DATE_MODIFIED`, and `MODIFIED_BY`.
        - **DuckDB Sync**: After successfully updating PostgreSQL, it calls `update_duckdb` to update DuckDB
          with the latest data.

    Transaction Handling:
        - **Retries on Deadlock**: If a deadlock is detected (`psycopg2.errors.DeadlockDetected`),
          the function will retry after a 10-second delay.
        - **Retries on Failed Transaction**: If a failed SQL transaction is detected (`psycopg2.errors.InFailedSqlTransaction`),
          it will also retry after a 10-second delay.
        - **Rollback on Errors**: For other errors, the function will log the issue, notify via Telegram,
          and skip the update process.

    Logging:
        - Logs the number of rows updated.
        - Logs errors and retries when deadlocks or failed transactions occur.
        - Sends Telegram alerts for errors that stop the process.

    Error Handling:
        - Retries on deadlock and failed transactions with exponential backoff (10 seconds delay).
        - Terminates the process if an unrecoverable error occurs, logging the error and sending
          a notification via Telegram.

    Returns:
        None
    """
    logging.info("Start updating records ...")
    stmt = f"""update {table_name} as t set
    USAGE_ANYTIME = c.v_usage_anytime,
    USAGE_OFFPEAK = c.v_usage_offpeak,
    USAGE_AVAILABLE_TOKENS = c.v_usage_available_tokens,
    SUMMARY_FLAG = c.summary_flag,
    DATE_MODIFIED = now(),
    MODIFIED_BY = c.modified_by
    from (values """
    with get_connection(username=pg_username, password=pg_password) as conn:
        try:
            with conn.cursor() as cursor:
                args_str = ",".join(
                    cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in records
                )
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
                logging.info("Updated records: %s rows", cursor.rowcount)
        except Exception as e:
            if isinstance(e, psycopg2.errors.DeadlockDetected):
                logging.warning("Deadlock found, retrying to update records ...")
                conn.rollback()
                time.sleep(10)
                update_records(duckdb_conn, table_name, records)
            elif isinstance(e, psycopg2.errors.InFailedSqlTransaction):
                logging.warning("Previously transaction is aborted, retrying to insert records")
                conn.rollback()
                time.sleep(10)
                update_records(duckdb_conn, table_name, records)
            else:
                logging.error(
                    "Other Error occured while updating records with description %s. Skipping the update process.",
                    str(e),
                )
                send_to_telegram(
                    f"Other Error occured while updating records with description {str(e)}"
                )
                sys.exit(1)


def main():
    """
    Main function for executing the ETL process that involves PostgreSQL and DuckDB.

    This function performs several operations:
    - Manages process control using a PID file to ensure that only one instance of the script is running.
    - Connects to a PostgreSQL and DuckDB database.
    - Optionally resets the `summary_flag` in the PostgreSQL table at specific times (00:03 and 18:03).
    - Reads data from PostgreSQL, processes and transforms it, and updates both PostgreSQL and DuckDB.
    - Logs all operations and sends notifications via Telegram in case of errors or important events.

    Steps:
        1. **PID Control**: Ensures that only one instance of the script is running by checking a PID file
           (`/tmp/{filename}.pid`). If the file exists, the script exits.
        2. **Database Connections**: Establishes connections to both PostgreSQL (via `get_connection()`)
           and DuckDB (via `duckdb.connect()`).
        3. **Reset `summary_flag`**: If the current time is 00:03 or 18:03, it resets the `summary_flag`
           for records with an `END_DATE` greater than or equal to the previous day.
        4. **Read Data**: Reads new records from the specified PostgreSQL table (`read_main_data`).
        5. **Transform and Update**: If new records exist, it transforms them using the `transform()`
           function and updates both PostgreSQL and DuckDB (`update_records`).
        6. **Error Handling**: Catches and logs exceptions, and sends notifications via Telegram if an error occurs.
        7. **Cleanup**: After execution, the script removes the PID file and closes database connections.

    Args:
        None

    Example:
        Run this script as a standalone program:
        ```bash
        python main.py
        ```

    Logging:
        - Logs process start and exit.
        - Logs when resetting `summary_flag` and when new records are found.
        - Logs number of records updated after transformation.
        - Logs errors and sends notifications via Telegram for significant events and failures.

    Error Handling:
        - Sends an alert to Telegram when an exception occurs.
        - Ensures that the PID file is removed and database connections are closed in the `finally` block.

    Returns:
        None
    """
    script_path = os.path.abspath(__file__)
    filename = os.path.basename(script_path)

    pid_file_path = f"/tmp/{filename}.pid"

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
        table_name = "bb_usage.usa_trx_usage_data_records_log"

        if start_time.strftime("%H%M") == "0003" or start_time.strftime("%H%M") == "1803":
            send_to_telegram(
                f"Start to reset summary_flag for END_DATE >= "
                f"{(start_time - datetime.timedelta(days=1)).strftime('%Y-%m-%d')}"
            )
            reset_summary_flag(start_time, duckdb_conn, cnx, table_name)

        main_df = read_main_data(table_name, cnx)
        if len(main_df) == 0:
            cnx.close()
            duckdb_conn.close()
            logging.info("No new record exists.\n\n\n")
            sys.exit(0)
        results = transform(duckdb_conn)
        update_records(duckdb_conn, table_name, results)
        duckdb_conn.close()

    except Exception as e:
        send_to_telegram(f"An Error occured while running main step with description: {str(e)}")
    finally:
        os.remove(pid_file_path)
        cnx.close()
        duckdb_conn.close()


if __name__ == "__main__":
    start_counter = time.perf_counter()

    get_logger("udr_calculation")
    load_dotenv()
    pg_username = os.getenv("POSTGRES_USERNAME")
    pg_password = os.getenv("POSTGRES_PASSWORD")

    main()

    duration = time.perf_counter() - start_counter
    CONVERTS = str(datetime.timedelta(seconds=duration))
    logging.info("Duration: %s \n\n", CONVERTS)
