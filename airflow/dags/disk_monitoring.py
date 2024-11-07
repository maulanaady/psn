import os
import pendulum
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task

with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    conn_id="telegram-dba",
    schedule="@hourly",
    tags=["monitoring"],
    max_active_runs=1,
    catchup=False,
) as dag:

    @task()
    def sshServer(conn_id, **context):
        hostname = conn_id[4:20].upper()
        connection = BaseHook.get_connection(conn_id)
        host = connection.host
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            _, stdout, _ = ssh_client.exec_command("df -h")
            line = stdout.readlines()
            list_dev = []
            for _, item in enumerate(line):
                x = item.split()
                x.append(hostname + " - " + host)
                list_dev.append(x)
            return list_dev
        except RuntimeError as error:
            DefaultDAG.exception_alert(error, **context)
            return None

    @task()
    def sendTelegram(result, **context):
        if result[0][-1] == "BACKUPDB - 192.168.1.45":
            minimum_disk = 90
        else:
            minimum_disk = 80
        output = []
        for i in result:
            for j in i:
                if j in ["/tmpdir", "/"] and i[-1].endswith("192.168.41.7"):
                    output.append(i)
                elif j in ["/backup"] and i[-1].endswith("192.168.1.45"):
                    output.append(i)
                elif j in ["/"]:
                    output.append(i)

        host = output[0][-1]
        mention = (
            f"<b>[{host}]</b>\n"
            'Hi <a href="tg://user?id=905830939">Himmawan</a>, '
            '<a href="tg://user?id=42168291">Ririz</a>, please check the following status:'
        )
        for i in output:
            percentage = int(i[4].replace("%", ""))
            if percentage >= minimum_disk:
                mention = mention + f"\nDisk usage on {i[0]} is <b>{i[4]} {i[5]}</b>"
                message = {
                    "text": f"{mention}",
                    "disable_web_page_preview": True,
                    "disable_notification": False,
                    "parse_mode": "HTML",
                }
                telegram_hook = TelegramHook("telegram")
                result = telegram_hook.send_message(message)
            else:
                print("Partition disk is still available")

    start = EmptyOperator(task_id="start", dag=dag)

    hostname = ["ssh-cboss-mhg", "ssh-cboss-snt", "ssh-cboss-snl", "ssh-analysis"]
    SSHOperator = sshServer.expand(conn_id=hostname)
    sendTelegram = sendTelegram.expand(result=SSHOperator)

    start >> SSHOperator >> sendTelegram
