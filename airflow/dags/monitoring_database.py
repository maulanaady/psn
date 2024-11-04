import os
import logging
import pendulum
from datetime import timedelta
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group

# from airflow.providers.ssh.operators.ssh import SSHOperator


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    conn_id="telegram-dba",
    tags=["monitoring"],
    schedule="*/10 * * * *",
    max_active_runs=1,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    # oracle_service_check = SSHOperator(
    #     task_id='oracle_service_check',
    #     ssh_conn_id='host-airflow',
    #     command='python3 /home/psn/script/py/oracle_check_service.py',
    #     )

    ############### database status ###############################
    @task
    def get_connection_status(conn_id, **kwargs):
        connection = BaseHook.get_connection(conn_id)
        result = {"db": conn_id, "status": None, "valid_": "Y", "IP": connection.host}
        try:
            conn = ConnectionHook.get_maria_connection(conn_id)
            result["status"] = "Connected"
            conn.close()
        except RuntimeError as e:
            DefaultDAG.exception_alert(e, **kwargs)
            result["status"] = str(e)
        return result

    # @task
    # def write_report(result, **context):
    #     import mysql.connector
    #     # from mysql.connector import errorcode
    #     try:
    #         conn = ConnectionHook.get_maria_connection('monitoring')
    #         if conn.is_connected():
    #             cursor = conn.cursor()
    #             query_insert = 'insert into db_status(db,status,valid_) values(%s,%s,%s);'
    #             value_insert = (result.get('db'), result.get(
    #                 'status'), result.get('valid_'))
    #             cursor.execute(query_insert, value_insert)
    #             cursor.close()
    #     except mysql.connector.Error as err:
    #         if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
    #             print("Something is wrong with your user name or password")
    #         elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
    #             print("Database does not exist")
    #     finally:
    #         if mysql.connector.connect(user='dba', password='dba2010', host='192.168.4.30', database='observer').is_connected():
    #             conn.close()

    @task
    def send_telegram_alert(result, **context):
        # from datetime import timedelta
        menit = int(
            (context["next_execution_date"] + timedelta(hours=7)).strftime("%M")
        )
        if result["status"] != "Connected":
            if (result["db"] != "cboss-galera") or (
                result["db"] == "cboss-galera" and menit == 0
            ):
                if result["db"] != "cboss-galera":
                    mention = (
                        'Hi <a href="tg://user?id=905830939">Himmawan</a>, '
                        '<a href="tg://user?id=42168291">Ririz</a> please check following status.\n'
                    )
                else:
                    mention = ""
                message = {
                    "text": (
                        f"{mention}<b>ALERT STATUS</b>:\n"
                        f"<i>{result['status']}</i>\n"
                        f"Database: [{result['db']}] - {result['IP']}\n"
                        f"Execution time: "
                        f"{(context['next_execution_date'] + timedelta(hours=7)).strftime('%Y-%m-%d, %H:%M:%S')}"
                    ),
                    "disable_web_page_preview": True,
                }
                connection = BaseHook.get_connection("telegram")
                telegram_hook = TelegramHook(
                    token=connection.password, chat_id=connection.host
                )
                telegram_hook.send_message(message)

    @task_group(tooltip="Monitoring database liveness")
    def monitoring_database_status(databases):
        # databases = ['cboss-mahaga', 'cboss-snt', 'cboss-snl', 'analysis', 'cboss-psn']
        get_connection_statuses = get_connection_status.expand(conn_id=databases)
        # write_reports = write_report.expand(result=get_connection_statuses)
        send_telegram_alerts = send_telegram_alert.expand(
            result=get_connection_statuses
        )
        # get_connection_statuses >> write_reports
        get_connection_statuses >> send_telegram_alerts
        # get_connection_statuses >> oracle_service_check

    ################ processlist ###################
    @task()
    def get_processlist(conn_id, **context):
        import csv

        try:
            conn = ConnectionHook.get_maria_connection(conn_id)
            cursor = conn.cursor()
            sql = """
            select id,
            user,
            host,
            db,
            command,
            time,
            state,
            query_id,
            progress
            from information_schema.processlist
            where command not like 'Binlog Dump'
            """
            cursor.execute(sql)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            header = [
                "Id",
                "User",
                "Host",
                "db",
                "Command",
                "Time",
                "State",
                "Query_id",
                "Progress",
            ]
            with open(f"/opt/airflow/data/{conn_id}.csv", "w", newline="") as f:
                write = csv.writer(f, delimiter="^", lineterminator=os.linesep)
                write.writerow(header)
                write.writerows(data)
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)

    @task()
    def get_summary(conn_id, **context):
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt

        ts = (context["data_interval_end"] + timedelta(hours=7)).strftime(
            "%Y-%m-%d, %H:%M:%S"
        )
        if os.path.exists(f"/opt/airflow/data/{conn_id}.csv"):
            data = pd.read_csv(f"/opt/airflow/data/{conn_id}.csv", delimiter="^")
            data = data[~(data["Command"].isin(["Sleep", "Daemon"]))]
            data = data[~(data["State"].isin(["User sleep"]))]
            if not data.empty:
                summary = (
                    data[["User", "Command"]].groupby("User").count().reset_index()
                )
                summary = summary[(summary["Command"] >= 50)]

                if not summary.empty:
                    user = list(summary["User"])
                    count = list(summary["Command"])
                    plt.figure(figsize=(10, 5))
                    plt.bar(user, count, fc="lightgrey", ec="black")

                    for i in range(len(user)):
                        plt.text(i, count[i], count[i], va="bottom")

                    plt.xlabel("User")
                    plt.ylabel("No. of Command")
                    plt.title(f"Monitoring Database {conn_id} Processlist at {ts}")
                    plt.savefig(f"/opt/airflow/data/{conn_id}.png")
                    del user, count, summary

                data_per_command = data[
                    (data["Time"].fillna(0).astype(np.int32) > 1800)
                    & ~(
                        data["Command"].isin(
                            ["Sleep", "Daemon", "Binlog Dump", "Killed"]
                        )
                    )
                ]
                data_per_command = data_per_command[
                    ~(data_per_command["State"].isin(["User sleep"]))
                ]
                data_per_command.fillna(value="-", inplace=True)
                if not data_per_command.empty:
                    columns = [
                        "Id",
                        "User",
                        "Host",
                        "db",
                        "Command",
                        "State",
                        "Query_id",
                        "Time",
                    ]
                    summary_per_command = (
                        data_per_command[columns]
                        .groupby(
                            ["Id", "User", "Host", "db", "Command", "State", "Query_id"]
                        )["Time"]
                        .sum()
                        .reset_index()
                    )
                    summary_per_command["State"] = summary_per_command[
                        "State"
                    ].str.slice(0, 12)
                    summary_per_command.to_csv(
                        f"/opt/airflow/data/{conn_id}_summary_per_command.csv",
                        sep=";",
                        index=False,
                        header=True,
                    )
                    user_per_command = list(summary_per_command["Id"].apply(str))
                    time_per_command = list(
                        summary_per_command["Time"].astype(np.int32)
                    )
                    del summary_per_command, data_per_command

                    plt.figure(figsize=(10, 5))
                    plt.bar(
                        user_per_command, time_per_command, fc="lightgrey", ec="black"
                    )
                    for i in range(len(user_per_command)):
                        plt.text(
                            i, time_per_command[i], time_per_command[i], va="bottom"
                        )

                    plt.xlabel("Id")
                    plt.ylabel("Cummulative Time (Seconds)")
                    plt.title(
                        f"Monitoring Database {conn_id} Processlist per Command at {ts}"
                    )
                    plt.savefig(f"/opt/airflow/data/{conn_id}_per_command.png")

    @task()
    def send_picture(conn_id, **context):
        import csv
        import requests

        if os.path.exists(f"/opt/airflow/data/{conn_id}_per_command.png"):
            with open(
                f"/opt/airflow/data/{conn_id}_summary_per_command.csv"
            ) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=";")
                line_count = 0
                captions = ""
                for row in csv_reader:
                    if line_count == 0:
                        caption = ""
                        line_count += 1
                    else:
                        detik = str(timedelta(seconds=int(row[7])))
                        caption = f"""{row[0]}
                            Server: {conn_id}
                            User: {row[1]}
                            Host: {row[2]}
                            DB: {row[3]}
                            Command: {row[4]}
                            State: {row[5]}
                            Query ID: {row[6]}
                            Time: {detik}
                            """
                        line_count += 1
                    captions = captions + caption
        connection = BaseHook.get_connection("telegram")
        token = connection.password
        chat_id = connection.host
        lists = os.listdir("/opt/airflow/data")
        for item in lists:
            if item.endswith(f"{conn_id}_per_command.png") or item.endswith(
                f"{conn_id}.png"
            ):
                files = {"photo": open(f"/opt/airflow/data/{item}", "rb")}
                if item.endswith(f"{conn_id}_per_command.png"):
                    msg = captions
                else:
                    msg = ""
                message = (
                    f"https://api.telegram.org/bot{token}/sendPhoto"
                    f"?chat_id={chat_id}&caption={msg}"
                )
                try:
                    requests.post(message, files=files)
                except Exception as e:
                    DefaultDAG.exception_alert(e, **context)
                    logging.info(str(e))

    @task(trigger_rule="none_failed")
    def delete_file(conn_id):
        lists = os.listdir("/opt/airflow/data")
        for item in lists:
            if (
                item.endswith(f"{conn_id}.csv")
                or item.endswith(f"{conn_id}.png")
                or item.endswith(f"{conn_id}_per_command.png")
                or item.endswith(f"{conn_id}_summary_per_command.csv")
            ):
                os.remove(os.path.join("/opt/airflow/data", item))

    @task_group(tooltip="Monitoring database runtime processlist")
    def monitoring_database_processlist(databases):
        # databases = ['cboss-psn','cboss-mahaga', 'cboss-snt', 'cboss-snl', 'analysis']
        get_processlists = get_processlist.expand(conn_id=databases)
        get_summaries = get_summary.expand(conn_id=databases)
        send_pictures = send_picture.expand(conn_id=databases)
        delete_files = delete_file.expand(conn_id=databases)

        get_processlists >> get_summaries >> send_pictures >> delete_files

    ############ galera gemilang #######################
    # @task(queue='main_queue',on_failure_callback=DefaultDAG.task_failure_alert)
    # def check_cluster(**context) -> dict:
    #     try:
    #         conn = ConnectionHook.get_maria_connection('galera-gemilang-02')
    #         cursor = conn.cursor()
    #         cursor.execute('show status like "wsrep_incoming_addresses"')
    #         gemilang_line = cursor.fetchall()
    #         if len(gemilang_line) == 0:
    #             conn = ConnectionHook.get_maria_connection('galera-gemilang-01')
    #             cursor = conn.cursor()
    #             cursor.execute('show status like "wsrep_incoming_addresses"')
    #             gemilang_line = cursor.fetchall()
    #             if len(gemilang_line) == 0:
    #                 conn = ConnectionHook.get_maria_connection('galera-gemilang-03')
    #                 cursor = conn.cursor()
    #                 cursor.execute('show status like "wsrep_incoming_addresses"')
    #                 gemilang_line = cursor.fetchall()
    #                 dic = {'GALERA-GEMILANG': gemilang_line}
    #             else:
    #                 dic = {'GALERA-GEMILANG': gemilang_line}
    #         else:
    #             dic = {'GALERA-GEMILANG': gemilang_line}
    #         return (dic)
    #     except Exception as error:
    #         DefaultDAG.exception_alert(error,**context)
    #         return {'GALERA-GEMILANG': None}

    # @task()
    # def send_telegram(dict:dict):
    #     connection = BaseHook.get_connection("telegram")
    #     token = connection.password
    #     chat_id = connection.host
    #     for i, list in dict.items():
    #         if list is not None:
    #             a = list[0]
    #             logging.info(f"test: {a}")
    #             if len(a) > 1:
    #                 b = a[1].split(',')
    #                 logging.info(f"test: {b}")
    #                 if len(b) < 3:
    #                     message = {
    #                         'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nSome node are down, current running node are:\n{b}',
    #                         "disable_web_page_preview": True,
    #                     }
    #                     telegram_hook = TelegramHook(token=token, chat_id=chat_id)
    #                     telegram_hook.send_message(message)
    #         elif list is None or len(list) == 0:
    #             message = {
    #                 'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nCan not get results.',
    #                 "disable_web_page_preview": True,
    #             }
    #             telegram_hook = TelegramHook(token=token, chat_id=chat_id)
    #             telegram_hook.send_message(message)

    # @task_group(tooltip="Monitoring database(galera) liveness")
    # def monitoring_galera_status():
    #     send_telegram(check_cluster())
    ##################################

    ############ galera gemilang #######################

    # @task()
    # def sshServer(conn_id: str, **context):
    #     from airflow.hooks.base import BaseHook
    #     print(conn_id)
    #     try:
    #         ssh_client = DefaultDAG.get_ssh_connection(conn_id)
    #         gemilang_cnx = BaseHook.get_connection('galera-gemilang-02')
    #         host = gemilang_cnx.host
    #         user = gemilang_cnx.login
    #         pwd = gemilang_cnx.password
    #         cmd = f"""mysql -h {host} -u{user} -p{pwd} -e 'show status like "wsrep_incoming_addresses"'"""
    #         stdin, stdout, stderr = ssh_client.exec_command(cmd)
    #         gemilang_line = stdout.readlines()
    #         if len(gemilang_line) == 0:
    #             gemilang_cnx = BaseHook.get_connection('galera-gemilang')
    #             host = gemilang_cnx.host
    #             user = gemilang_cnx.login
    #             pwd = gemilang_cnx.password
    #             cmd = f"""mysql -h {host} -u{user} -p{pwd} -e 'show status like "wsrep_incoming_addresses"'"""
    #             stdin, stdout, stderr = ssh_client.exec_command(cmd)
    #             gemilang_line = stdout.readlines()
    #             if len(gemilang_line) == 0:
    #                 gemilang_cnx = BaseHook.get_connection(
    #                     'galera-gemilang-03')
    #                 host = gemilang_cnx.host
    #                 user = gemilang_cnx.login
    #                 pwd = gemilang_cnx.password
    #                 cmd = f"""mysql -h {host} -u{user} -p{pwd} -e 'show status like "wsrep_incoming_addresses"'"""
    #                 stdin, stdout, stderr = ssh_client.exec_command(cmd)
    #                 gemilang_line = stdout.readlines()
    #                 dict = {'GALERA-GEMILANG': gemilang_line}
    #             else:
    #                 dict = {'GALERA-GEMILANG': gemilang_line}
    #         else:
    #             dict = {'GALERA-GEMILANG': gemilang_line}

    #         # oss_cnx = BaseHook.get_connection('galera-oss-70G')
    #         # oss_host = oss_cnx.host
    #         # oss_user = oss_cnx.login
    #         # oss_pwd = oss_cnx.password
    #         # cmd = f"""mysql -h {oss_host} -u{oss_user} -p{oss_pwd} -e 'show status like "wsrep_incoming_addresses"'"""
    #         # stdin, stdout, stderr = ssh_client.exec_command(cmd)
    #         # oss_line = stdout.readlines()
    #         # dict.update({'GALERA-OSS-70G': oss_line})
    #         return (dict)

    #     except Exception as error:
    #         DefaultDAG.exception_alert(error, **context)
    #         return {'GALERA-GEMILANG': None}

    # @task()
    # def send_telegram(dict):
    #     connection = BaseHook.get_connection("telegram")
    #     token = connection.password
    #     chat_id = connection.host
    #     if dict is not None:
    #         for i, list in dict.items():
    #             #                logging.info(len(list))
    #             if list is not None and len(list) > 0:
    #                 a = list[1].split('\t')
    #                 b = a[1].replace("\n", "").split(',')
    #                 if len(b) < 3:
    #                     # connection = BaseHook.get_connection("telegram")
    #                     # token = connection.password
    #                     # chat_id = connection.host
    #                     message = {
    #                         'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nSome node are down, current running node are:\n{b}',
    #                         "disable_web_page_preview": True,
    #                     }
    #                     telegram_hook = TelegramHook(
    #                         token=token, chat_id=chat_id)
    #                     telegram_hook.send_message(message)
    #             elif list is None or len(list) == 0:
    #                 message = {
    #                     'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nCan not get results.',
    #                     "disable_web_page_preview": True,
    #                 }
    #                 telegram_hook = TelegramHook(token=token, chat_id=chat_id)
    #                 telegram_hook.send_message(message)
    #     else:
    #         message = {
    #             'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nCan not get results.',
    #                     "disable_web_page_preview": True,
    #         }
    #         telegram_hook = TelegramHook(token=token, chat_id=chat_id)
    #         telegram_hook.send_message(message)

    # @task_group(tooltip="Monitoring database(galera) liveness")
    # def monitoring_galera_status():
    #     send_telegram(sshServer('ssh_gemilang'))

    ##################################
    t3 = EmptyOperator(task_id="end")

    databases = ["cboss-mahaga", "cboss-snt", "cboss-snl", "analysis", "cboss-psn"]
    # start >> [monitoring_database_status(databases), monitoring_database_processlist(databases),
    #           monitoring_galera_status()] >> t3

    (
        start
        >> [
            monitoring_database_status(databases),
            monitoring_database_processlist(databases),
        ]
        >> t3
    )
