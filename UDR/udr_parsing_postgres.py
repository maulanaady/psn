import logging,threading
import time,logging,psycopg2
from datetime import datetime, timedelta
import shutil,os,json,sys
from psycopg2 import errors
from dotenv import load_dotenv


def delete_old_file(dir_path):
    old_date_prefix = (datetime.now() -
                        timedelta(days=5)).strftime("%Y%m%d")

    # Traverse directories recursively
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if old_date_prefix in file:
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted file: {file_path}")
                except Exception as e:
                    logging.exception(e) 


def create_pid_file(pid_file):
    pid = str(os.getpid())
    with open(pid_file, 'w') as pid_file:
        pid_file.write(pid)

def get_logger(log_file: str):
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
    logger = logging.getLogger(__name__)
    return logger


def get_connection(username, password, counter, logger, port=5432):
    logging.info('try to get connection ...')
    conn_uri = f"postgresql://{username}:{password}@172.20.12.150:{port},172.20.12.177:{port}/udr?target_session_attrs=read-write"

    max_retries = 10
    retry_delay = 3  # seconds
    retries = 0

    while retries < max_retries:
        try:
            conn = psycopg2.connect(conn_uri)
            logger.info(f"DB [Thread {counter}] was connected ..")
            return conn
        except Exception as e:
            logger.error(f'Error getting connection with description {str(e)}, retrying ...')
            retries += 1
            time.sleep(retry_delay)
    
    if retries == max_retries:
        send_to_telegram(f'Error getting connection with description {str(e)}', logger)
        sys.exit(1)


def sort_by_modified_time(filename):
    time_str = filename.split('_')[-1].split('.')[0]
    return datetime.strptime(time_str, '%Y%m%d%H%M')


def cek_raw_file(logger):
    logger.info('== Try to read file in path RAW ==')
    maxFile = 35
    counter = 1

    file_list = [file for file in os.listdir(
        raw_path) if file.endswith(".udr")]

    if len(file_list) != 0:
        logger.info(
            f'Try to move file [{len(file_list)}] files UDR in RAW path to PROC path ')
        
        file_list.sort(key=sort_by_modified_time)
        for file in file_list:
            if counter <= maxFile:
                filename = os.path.basename(file)
                file_src = raw_path+filename

                try:
                    moves = shutil.copy(file_src, proc_path)
                    if (moves):
                        os.remove(file_src)
                        logger.info(f'File {file} success move to {proc_path}')
                        counter += 1
                except FileNotFoundError as fnf_error:
                    logger.error(fnf_error)
    else:
        logger.info(f'No File in UDR RAW Folders, exiting ... \n')
        sys.exit(0)


def cek_proc_file(proc_path, logger):
    logger.info('== Try to read file in path PROC ==')
    
    udrFile = [file for file in os.listdir(proc_path) if file.endswith(".udr")]
    logger.info(f'UDR File in PROC : {len(udrFile)} files')

    return udrFile


def _timestampToDate(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def _proc_normal(data, logger):

    spname = data['serviceplan'] if 'serviceplan' in data else None
    esn = data['esn'] if 'esn' in data else None
    clock = int(str(data['lastpolledtime'])[0:10])

    if (data['overallcapacity'] > 1 and data['overallusage'] > 1) or (data['offpeakoverallcapacity'] > 1 and data['offpeakoverallusage'] > 1):

        rdata = {
            "sbcNum": data['deviceid'],
            "clock": str(data['lastpolledtime'])[0:10],
            "ns": str(data['lastpolledtime'])[10:13],
            "endTime":  _timestampToDate(clock),
            "capacityAnytime": data['overallcapacity'],
            "capacityOffPeak": data['offpeakoverallcapacity'],
            "overAllUsageAnytime": data['overallusage'],
            "overAllUsageOffPeak": data['offpeakoverallusage'],
            "overAllAvailTokens": data['availtokens'],
            "fapStatus": data['fapstatus'],
            "terminalStatus": data['terminalstatus'],
            "inPeakPeriod": data['inpeakperiod'],
            "esn": esn,
            "spName": spname,
        }
        logger.info(
            f"ID {data['deviceid']}: PolledTime: {_timestampToDate(clock)} Status: {data['terminalstatus']} .... OK")
        return rdata
    else:

        logger.info(
            f"ID {data['deviceid']}: PolledTime: {_timestampToDate(clock)} Status: {data['terminalstatus']} .... IGNORED")
        

def _proc_suspended(data):
    clock = int(str(data['lastpolledtime'])[0:10])
    spname = data['serviceplan'] if 'serviceplan' in data else None
    esn = data['esn'] if 'esn' in data else None
    
    rdata = {
        "sbcNum": data['deviceid'],
        "clock": str(data['lastpolledtime'])[0:10],
        "ns": str(data['lastpolledtime'])[10:13],
        "endTime":  _timestampToDate(clock),
        "capacityAnytime": data['overallcapacity'],
        "capacityOffPeak": data['offpeakoverallcapacity'],
        "overAllUsageAnytime": data['overallusage'],
        "overAllUsageOffPeak": data['offpeakoverallusage'],
        "overAllAvailTokens": data['availtokens'],
        "fapStatus": data['fapstatus'],
        "terminalStatus": data['terminalstatus'],
        "inPeakPeriod": data['inpeakperiod'],
        "esn": esn,
        "spName": spname
    }
# Ady: update data hasil parsing karena ada udr record dengan lastpolledtime = 0, 
# sehingga ns (dengan formula: "ns": str(data['lastpolledtime'])[10:13]) menghasilkan output ns = '',
# padahal kolom ns ini dipasang constraint not null di tabelnya
    if str(data['lastpolledtime'])[10:13] == '':
        rdata.update({'ns': 0})
    return rdata


def _thread_process(procFile, logger):
    logger.info("== Set alocation thread ==")

    threads = []

    for i in range(len(procFile)):
        file = procFile[i]
        thread = threading.Thread(target=process, args=(
            file, i, logger,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def moved_after_insert(filename, proc_path, finish_path, logger):
    try:
        moves = shutil.copy(proc_path+filename, finish_path)
        if (moves):
            os.remove(proc_path+filename)
            logger.info(f" Move {filename}, Success move to : {finish_path}")
        else:
            logger.error(f"{filename},  Failed move to : {finish_path}")
    except FileNotFoundError as fnf_error:
        logger.error(f"failed {fnf_error}")


def is_directory_empty(directory):
    return not any(os.scandir(directory))


def find_udr_files(directory):
    udr_files = [file for file in os.listdir(
        directory) if file.endswith(".udr")]
    return udr_files


def send_to_telegram(message, logger):
    apiToken = os.getenv('TELEGRAM_API_TOKEN')
    chatID = os.getenv('TELEGRAM_CHAT_ID')

    import requests

    apiURL = f'https://api.telegram.org/bot{apiToken}/sendMessage'
    try:
        logger.info(message)
        requests.post(
            apiURL, json={'chat_id': chatID, 'text': f'CRONJOB udr_parsing - {message}'})
    except Exception as e:
        logger.warning(e)


def cek_proc_file_is_not_empty(proc_path, logger):

    if not is_directory_empty(proc_path):
        udr_files = find_udr_files(proc_path)

        if udr_files:
            logger.warning(
                f"Found {len(udr_files)} .udr file(s) in the directory: {proc_path}")
            message = f"[POSTGRES] Failed UDR file : \n\n {udr_files}"
            send_to_telegram(message, logger)
            for udr_file in udr_files:
                logger.warning(udr_file)
        else:
            logger.info(
                "No .udr files found in directory PROC after finish process parsing.")
    else:
        logger.info(f"The {proc_path} directory is empty.")


def insert_query(results, logger, counter, filename):
    pg_username = os.getenv('POSTGRES_USERNAME')
    pg_password = os.getenv('POSTGRES_PASSWORD')

    filename = filename
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
            ) VALUES """
    conn = get_connection(pg_username, pg_password, counter, logger, port=5432)
    logger.info(f"parsed rowcount: {len(results)} for filename {filename}")
    if len(results) > 0:
        try:
            cursor = conn.cursor()
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in results)
            cursor.execute(query + args_str + " ON CONFLICT DO NOTHING;")
            cursor.close()
            conn.commit()
            logger.info(f'Done inserting records for file {filename}, affected rows: {cursor.rowcount}')
            resp = True
        except Exception as e:
            if isinstance(e,errors.DeadlockDetected):
                logger.warning(f'Deadlock found, retrying to insert records for file {filename}')
                time.sleep(3)
                conn.rollback()
                conn.close()
                insert_query(results, logger, counter, filename)
                resp = True
            elif isinstance(e,errors.InFailedSqlTransaction):
                logger.warning(f'Previously transaction is aborted, retrying to insert records for file {filename}')
                time.sleep(3)
                conn.rollback()
                conn.close()
                insert_query(results, logger, counter, filename)
                resp = True
            else:
                logger.error(f'Other Error occured while inserting records with description {str(e)}. Skipping file {filename}')
                send_to_telegram(f'Other Error occured while inserting records with description {str(e)}. Skipping file {filename}', logger)
                resp = False
        finally:
            conn.close()
            return resp,len(results)
    else:
        return False,len(results)


def process(file, counter, logger):
    logger.info("[Read Process {}] File {}".format(counter, file))
    
    logger.info("[Thread {}] File {}".format(counter, file))

    if file is not None and file.endswith('.udr'):
        filename = file
        path_file = proc_path+filename
        results = []
        try:
            with open(path_file, 'r') as file:
                for f in file:
                    line = f.lower()
                    try:
                        jline = json.loads(line)
                        terminalStatus = jline['terminalstatus'] if "terminalstatus" in jline else ""
                        if terminalStatus == 'normal':
                            data = _proc_normal(jline, logger)
                        elif terminalStatus == "minor":
                            data = _proc_normal(jline, logger)
                        elif terminalStatus == "critical":
                            data = _proc_suspended(jline)
                        elif terminalStatus == "suspended":
                            data = _proc_suspended(jline)
                        else:
                            logger.info("DATA False")
                            data = None

                        if data is not None:
                            data.update({'udrfilename': filename,
                                        'createdby': "netmon"})
                            result = (tuple(data.values()))
                            results.append(result)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"Error decoding JSON in line: {line}. Error: {e}")
                        continue
                insert_succeded,rowcount = insert_query(results, logger, counter, filename)
                if (insert_succeded == True and rowcount > 0) or (insert_succeded == False and rowcount == 0):
                    moved_after_insert(filename, proc_path,
                                        finish_path, logger)
        except FileNotFoundError:
            logger.info("Error read file UDR")
    # Move after insert
    else:
        logger.info(
            f"Error: File {file} is None or does not have the .udr extension.")

def main():
    logger = get_logger(log_file=f"thread_parsing_udr")

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
        logger.info("============= Start UDR Encoder =============")
        time.sleep(3)

        cek_raw_file(logger)
        procFile = cek_proc_file(proc_path, logger)

        _thread_process(procFile, logger)
        cek_proc_file_is_not_empty(proc_path, logger)
        delete_old_file('/ubq_udr/udr_encoder/log')

        end = time.perf_counter()
        duration = str(timedelta(seconds=end-start))

        logger.info("waktu proses : {} detik".format(duration))
        logger.info("============= END UDR Encoder =============\n\n")
    except Exception as e:
        send_to_telegram(f'An Error occured while running main step with description: {str(e)}',logger)    
    finally:
        os.remove(pid_file_path)


if __name__ == "__main__":

    raw_path = "/ubq_udr/udr_encoder/data/RAW/"
    proc_path = "/ubq_udr/udr_encoder/data/PROC/"
    finish_path = "/ubq_udr/udr_encoder/data/FINISH/"

    load_dotenv()
    
    main()
