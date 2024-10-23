import logging
import threading
import time
import os
import json
import sys
import shutil
from datetime import datetime, timedelta
from contextlib import contextmanager
import requests
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = "/ubq_udr/udr_encoder/data/RAW/"
PROC_PATH = "/ubq_udr/udr_encoder/data/PROC/"
FINISH_PATH = "/ubq_udr/udr_encoder/data/FINISH/"



def delete_old_file(dir_path):
    """Delete files older than 5 days in the specified directory.

    Args:
        dir_path (str): Directory where old files will be deleted. 
                        Files containing the date prefix from five days ago will be removed.
    """
    old_date_prefix = (datetime.now() -
                        timedelta(days=5)).strftime("%Y%m%d")

    # Traverse directories recursively
    for root, _, files in os.walk(dir_path):
        for file in files:
            if old_date_prefix in file:
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    logging.info("Deleted file: %s", file_path)
                except Exception as e:
                    logging.exception(e)


def create_pid_file(pid_file:str):
    """Create a PID file with the current process ID.

    Args:
        pid_file (str): The path to the PID file to be created.
    """
    pid = str(os.getpid())
    with open(pid_file, 'w', encoding='utf-8') as file:
        file.write(pid)

def get_logger(log_file: str):
    """Create a log file with name log_file.

    Args:
        log_file (str): The log file to be created.
    """
    now = datetime.now()
    path = '/ubq_udr/udr_encoder/log/parsing/'
    date_prefix = now.strftime("%Y%m%d")
    logfile = f"{path}/log_{log_file}_{date_prefix}.log"
    logging.basicConfig(
        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
        datefmt='%A, %d %B %Y %H:%M:%S',
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(logfile, mode='a')],
        level=logging.DEBUG)
    return logging.getLogger(__name__)


@contextmanager
def get_connection(username, password, counter, logger, port=5432, max_retries=10, retry_delay=3):    
    """Context manager for establishing a PostgreSQL connection with retries.
    Args:
        username (str): The username to connect to the database.
        password (str): The password for the database user.
        counter (int): A counter to identify the thread making the connection.
        logger (logging.Logger): The logger instance used for logging connection attempts and errors.
        port (int, optional): The port number on which the database is listening. Defaults to 5432.

    Raises:
        SystemExit: Exits the program if the maximum number of connection retries is reached.

    Returns:
        psycopg2.connection: A connection object to the PostgreSQL database.

    Retries:
        The function will attempt to connect to the database up to `max_retries` times,
        with a delay of `retry_delay` seconds between attempts. If all attempts fail,
        a message will be sent to a Telegram bot, and the program will exit.
    """
    conn_uri = f"postgresql://{username}:{password}@172.20.12.150:{port},172.20.12.177:{port}/udr?target_session_attrs=read-write"
    conn = None
    attempts = 0
    
    while attempts < max_retries:
        try:
            conn = psycopg2.connect(conn_uri)
            logger.info(f"DB [Thread {counter}] was connected.")
            yield conn  # Yield the connection to the caller
            break  # Exit the loop if connection is successful
        except Exception as e:
            attempts += 1
            logger.error(f'Error connecting to database (attempt {attempts}): {str(e)}')
            if attempts < max_retries:
                time.sleep(retry_delay)  # Wait before retrying
            else:
                send_to_telegram(f'Error getting connection with description: {str(e)}', logger)  # Notify about failure
                raise  # Raise the exception after max retries
        finally:
            if conn:
                conn.close()
                logger.info(f"DB [Thread {counter}] connection closed.")

def sort_by_modified_time(filename):
    """Extract and return the modification time from a given filename.

    The function assumes the filename follows a specific format where the 
    modification time is encoded at the end, following an underscore and 
    preceding the file extension. The time is expected to be in the 
    format 'YYYYMMDDHHMM'.

    Args:
        filename (str): The filename from which to extract the modification time.

    Returns:
        datetime: A datetime object representing the modification time.

    Raises:
        ValueError: If the time string cannot be parsed into a datetime object.
    """
    time_str = filename.split('_')[-1].split('.')[0]
    return datetime.strptime(time_str, '%Y%m%d%H%M')


def cek_raw_file(logger):
    """Move UDR files from the RAW directory to the PROC directory.

    The function searches for files with the '.udr' extension in the RAW 
    directory, sorts them by their modified time, and moves a maximum of 
    `max_file` files to the PROC directory. The function logs the process, 
    including any errors that occur during the file operations.

    Args:
        logger (logging.Logger): A logger instance for logging information and errors.

    Raises:
        SystemExit: Exits the program if no UDR files are found in the RAW directory.
    """
    logger.info('== Try to read file in path RAW ==')
    max_file = 35

    file_list = [file for file in os.listdir(
        RAW_PATH) if file.endswith(".udr")]

    if file_list:
        logger.info(f"Found {len(file_list)} UDR files, moving up to {max_file} files to PROC path.")
        file_list.sort(key=sort_by_modified_time)
        for file in file_list[:max_file]:
            file_src = os.path.join(RAW_PATH, os.path.basename(file))
            file_proc = os.path.join(PROC_PATH, os.path.basename(file))
            try:
                shutil.move(file_src, file_proc)
                logger.info(f'File {file} success move to {PROC_PATH}')
            except FileNotFoundError as fnf_error:
                logger.error(fnf_error)
    else:
        logger.info('No File in UDR RAW Folders, exiting.')
        sys.exit(0)


def cek_proc_file(procpath, logger):
    """Check and list UDR files in the specified PROC directory.

    This function scans the given directory for files with the '.udr' extension,
    logs the number of UDR files found, and returns a list of those files.

    Args:
        procpath (str): The path to the PROC directory to check for UDR files.
        logger (logging.Logger): A logger instance for logging information.

    Returns:
        list: A list of UDR file names found in the specified directory.
    """
    logger.info("Checking PROC directory for UDR files.")
    udr_files = [file for file in os.listdir(procpath) if file.endswith(".udr")]
    logger.info(f"Found {len(udr_files)} UDR files in PROC directory.")
    return udr_files

def timestamp_to_date(timestamp):
    """Convert a timestamp to a formatted date string.

    Args:
        timestamp (int): A Unix timestamp.

    Returns:
        str: A formatted date string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def _proc_normal(data, logger):
    """Process normal UDR data.

    This function checks the usage and capacity of the data. If the usage 
    and capacity conditions are met, it constructs a result dictionary 
    and logs the processing status.

    Args:
        data (dict): The data dictionary containing UDR information.
        logger (logging.Logger): A logger instance for logging information.

    Returns:
        dict or None: A dictionary with processed data if conditions are met, 
                      None otherwise.
    """
    clock = int(str(data['lastpolledtime'])[0:10])
    if (
    (data['overallcapacity'] > 1 and data['overallusage'] > 1) or 
    (data['offpeakoverallcapacity'] > 1 and data['offpeakoverallusage'] > 1)
    ):
        rdata = {
            "sbcNum": data['deviceid'],
            "clock": str(data['lastpolledtime'])[0:10],
            "ns": str(data['lastpolledtime'])[10:13],
            "endTime":  timestamp_to_date(clock),
            "capacityAnytime": data['overallcapacity'],
            "capacityOffPeak": data['offpeakoverallcapacity'],
            "overAllUsageAnytime": data['overallusage'],
            "overAllUsageOffPeak": data['offpeakoverallusage'],
            "overAllAvailTokens": data['availtokens'],
            "fapStatus": data['fapstatus'],
            "terminalStatus": data['terminalstatus'],
            "inPeakPeriod": data['inpeakperiod'],
            "esn": data.get('esn'),
            "spName": data.get('serviceplan'),
        }
        logger.info(f"""ID {data['deviceid']}: PolledTime:
                     {timestamp_to_date(clock)} Status: {data['terminalstatus']} .... OK""")
        return rdata

    logger.info(f"""ID {data['deviceid']}: PolledTime:
            {timestamp_to_date(clock)} Status: {data['terminalstatus']} .... IGNORED""")
    return None

def _proc_suspended(data):
    """Process suspended UDR data.

    Constructs a result dictionary from the given data for suspended 
    devices, setting default values where necessary.

    Args:
        data (dict): The data dictionary containing UDR information.

    Returns:
        dict: A dictionary with processed suspended device data.
    """
    clock = int(str(data['lastpolledtime'])[0:10])
    rdata = {
        "sbcNum": data['deviceid'],
        "clock": str(data['lastpolledtime'])[0:10],
        "ns": str(data['lastpolledtime'])[10:13] or '0',
        "endTime":  timestamp_to_date(clock),
        "capacityAnytime": data['overallcapacity'],
        "capacityOffPeak": data['offpeakoverallcapacity'],
        "overAllUsageAnytime": data['overallusage'],
        "overAllUsageOffPeak": data['offpeakoverallusage'],
        "overAllAvailTokens": data['availtokens'],
        "fapStatus": data['fapstatus'],
        "terminalStatus": data['terminalstatus'],
        "inPeakPeriod": data['inpeakperiod'],
        "esn": data.get('esn'),
        "spName": data.get('serviceplan')
    }
    return rdata


def _thread_process(proc_file, logger):
    """Start and manage threads to process UDR files.

    This function creates threads for processing each UDR file in 
    the specified list and waits for all threads to complete.

    Args:
        proc_file (list): A list of UDR file names to process.
        logger (logging.Logger): A logger instance for logging information.
    """
    logger.info("Starting threads for processing UDR files.")

    threads = []
    threads = [threading.Thread(target=process, args=(file, i, logger)) for i, file in enumerate(proc_file)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

def moved_after_insert(filename, procpath, finishpath, logger):
    """Move a processed UDR file to a finished directory.

    This function copies a file from the PROC directory to a finish 
    directory and removes the original file.

    Args:
        filename (str): The name of the file to move.
        procpath (str): The path of the PROC directory.
        finishpath (str): The path of the finished directory.
        logger (logging.Logger): A logger instance for logging information.
    """
    try:
        shutil.move(os.path.join(procpath, filename), os.path.join(finishpath, filename))
        logger.info(f" Move {filename}, Success move to : {finishpath}")
    except FileNotFoundError as fnf_error:
        logger.error(f"failed {fnf_error}")


def is_directory_empty(directory):
    """Check if a directory is empty.

    Args:
        directory (str): The path to the directory to check.

    Returns:
        bool: True if the directory is empty, False otherwise.
    """
    return not any(os.scandir(directory))


def find_udr_files(directory):
    """Find and list UDR files in the specified directory.

    This function scans the given directory for files with the '.udr' extension.

    Args:
        directory (str): The path to the directory to search for UDR files.

    Returns:
        list: A list of UDR file names found in the specified directory.
    """
    return [file for file in os.listdir(directory) if file.endswith(".udr")]

def send_to_telegram(message, logger):
    """Send a message to a Telegram chat via the Telegram Bot API.

    This function sends a log message to a specified Telegram chat.

    Args:
        message (str): The message to send.
        logger (logging.Logger): A logger instance for logging information.

    Raises:
        Exception: Any exceptions raised during the request to the Telegram API.
    """
    api_token = os.getenv('TELEGRAM_API_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')

    api_url = f'https://api.telegram.org/bot{api_token}/sendMessage'
    try:
        logger.info(message)
        requests.post(
            api_url, json={'chat_id': chat_id, 'text': f'CRONJOB udr_parsing - {message}'},
            timeout=10)
    except requests.Timeout as e:
        logger.warning(f"Request to Telegram API timed out: {e}")
    except Exception as e:
        logger.warning(e)


def cek_proc_file_is_not_empty(procpath, logger):
    """Check if the PROC directory is not empty and report UDR files.

    This function checks the specified PROC directory for UDR files. If files are found,
    it logs the number of files and sends a notification to Telegram.

    Args:
        procpath (str): The path to the PROC directory.
        logger (logging.Logger): A logger instance for logging information.
    """
    if not is_directory_empty(procpath):
        udr_files = find_udr_files(procpath)
        if udr_files:
            message = f"""[POSTGRES] UDR Parsing failed: {len(udr_files)} UDR
             files are pending in PROC directory."""
            logger.error(message)
            send_to_telegram(message, logger)
    else:
        logger.info(f"The {procpath} directory is empty.")


def insert_query(results, logger, counter, filename):
    """Insert parsed UDR data into a PostgreSQL database.

    This function constructs an SQL insert query from the provided results 
    and executes it on the database. It handles potential deadlock and 
    transaction errors, retrying as necessary.

    Args:
        results (list): A list of tuples containing the UDR data to insert.
        logger (logging.Logger): A logger instance for logging information.
        counter (int): The identifier for the current file being processed.
        filename (str): The name of the file being processed.

    Raises:
        Exception: Raises any exception encountered during the database operation.
    """
    pg_username = os.getenv('POSTGRES_USERNAME')
    pg_password = os.getenv('POSTGRES_PASSWORD')

    query = """INSERT INTO bb_usage.usa_trx_usage_data_records
            (subscriber_number,
            clock,
            ns,
            end_date,
            capacity_anytime,
            capacity_offpeak,
            overall_usage_anytime,
            overall_usage_offpeak,
            overall_available_tokens,
            fap_status,
            terminal_status,
            in_peak_period,
            esn,
            service_plan,
            udr_file_name,
            created_by
            ) VALUES %s
            ON CONFLICT DO NOTHING"""
    resp = False
    with get_connection(pg_username, pg_password, counter, logger) as conn:
        try:
            logger.info(f"parsed rowcount: {len(results)} for filename {filename}")
            if len(results) > 0:
                with conn.cursor() as cursor:
                    args_str = ','.join(
                                cursor.mogrify(
                                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", value
                                ).decode("utf-8") for value in results
                            )
                    cursor.execute(query, (args_str,))
                    conn.commit()
                    logger.info(f"""Done inserting records for file {filename},
                                affected rows: {cursor.rowcount}""")
                    resp = True
        except Exception as e:
            if isinstance(e,errors.DeadlockDetected):
                logger.warning(f'Deadlock found, retrying to insert records for file {filename}')
                time.sleep(3)
                conn.rollback()
                return  insert_query(results, logger, counter, filename)
            if isinstance(e,errors.InFailedSqlTransaction):
                logger.warning(f"""Previously transaction is aborted,
                                retrying to insert records for file {filename}""")
                time.sleep(3)
                conn.rollback()
                return insert_query(results, logger, counter, filename)
            logger.error(f"""Other Error occured while inserting records
                with description {str(e)}. Skipping file {filename}""")
            send_to_telegram(f"""Other Error occured while inserting records
                with description {str(e)}. Skipping file {filename}""", logger)
    return resp,len(results)

def is_valid_udr_file(file):
    """Check if the file is a valid UDR file."""
    return file is not None and file.endswith('.udr')

def process_line(line, filename, results, logger):
    """Process a single line from the UDR file."""
    try:
        jline = json.loads(line)
        terminal_status = jline.get('terminalstatus', "")
        data = process_terminal_status(jline, terminal_status, logger)
        if data is not None:
            data.update({'udrfilename': filename, 'createdby': "netmon"})
            results.append(tuple(data.values()))
    except json.JSONDecodeError as e:
        logger.warning(f"Error decoding JSON in line: {line}. Error: {e}")

def process_terminal_status(jline, terminal_status, logger):
    """Determine processing function based on terminal status."""
    if terminal_status in {'normal', 'minor'}:
        return _proc_normal(jline, logger)
    if terminal_status in {'critical', 'suspended'}:
        return _proc_suspended(jline)
    logger.info("DATA False")
    return None

def process(file, counter, logger):
    """Process a UDR file and insert its data into a PostgreSQL database.

    This function reads a UDR file, parses its contents, and processes each 
    line based on the terminal status. It collects valid data and calls 
    `insert_query` to insert the processed results into the database. 
    After successful insertion, it moves the processed file to a finished 
    directory.

    Args:
        file (str): The name of the UDR file to process.
        counter (int): An identifier for the current file being processed.
        logger (logging.Logger): A logger instance for logging information.

    Raises:
        FileNotFoundError: If the specified UDR file does not exist.
        json.JSONDecodeError: If a line in the file cannot be decoded as JSON.
    """
    logger.info(f"[Read Process {counter}] File {file}")
    logger.info(f"[Thread {counter}] File {file}")
    if not is_valid_udr_file(file):
        logger.info(f"Error: File {file} is None or does not have the .udr extension.")
        return
    path_file = os.path.join(PROC_PATH, file)
    results = []

    try:
        with open(path_file, 'r',encoding='utf-8') as files:
            for line in files:
                process_line(line.lower(), file, results, logger)

            insert_succeded,rowcount = insert_query(results, logger, counter, file)
            if (insert_succeded and rowcount > 0) or (not insert_succeded and rowcount == 0):
                moved_after_insert(file, PROC_PATH,
                                    FINISH_PATH, logger)
    except FileNotFoundError:
        logger.info("Error read file UDR")

def main():
    """Main function to run the UDR encoding process.

    This function initializes logging, checks for running instances, 
    and coordinates the overall process of checking raw files, processing 
    UDR files in threads, and inserting data into the database. 
    It logs the duration of the process and handles exceptions.

    Raises:
        Exception: Raises any exception encountered during execution.
    """
    logger = get_logger(log_file="thread_parsing_udr")

    script_path = os.path.abspath(__file__)
    filename = os.path.basename(script_path)
    pid_file_path = f'/tmp/{filename}.pid'

    if os.path.exists(pid_file_path):
        logging.info("Script is already running. Exiting.\n\n")
        sys.exit(0)

    try:
        # Create the PID file
        create_pid_file(pid_file_path)
        start = time.perf_counter()
        logger.info("Start UDR Encoder.")
        time.sleep(3)

        cek_raw_file(logger)
        proc_file = cek_proc_file(PROC_PATH, logger)

        _thread_process(proc_file, logger)
        cek_proc_file_is_not_empty(PROC_PATH, logger)
        delete_old_file('/ubq_udr/udr_encoder/log')

        end = time.perf_counter()
        duration = str(timedelta(seconds=end-start))

        logger.info("waktu proses : %s detik", duration)
        logger.info("END UDR Encoder\n\n")
    except Exception as e:
        send_to_telegram(f"""An Error occured while running main
         step with description: {str(e)}""",logger)
    finally:
        os.remove(pid_file_path)


if __name__ == "__main__":
    main()
