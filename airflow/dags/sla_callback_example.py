import datetime
import time
import os
import logging
import pendulum
import requests

from default.default_config import DefaultDAG
from airflow.operators.python import PythonOperator


def send_telegram_message(message):
    TELEGRAM_BOT_TOKEN = "1613558114:AAHSoUmYdeX2NMwGAIpJTrryVZ7YjbTSEIY"
    TELEGRAM_CHAT_ID = "-1002313477728"

    """Send a message to a Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "message_thread_id": 5,
        "parse_mode": "HTML",
    }
    response = requests.post(url, data=payload, timeout=10)
    if response.status_code != 200:
        logging.error("Failed to send Telegram message: %s", response.text)
    else:
        logging.info("Telegram notification sent successfully.")


def sla_miss_callback(*args):
    """Callback function that is called when an SLA is missed."""
    if len(args) >= 5:
        dag, task_list, blocking_task_list, slas, blocking_tis = args[:5]
        message = f"""
        <b>🚨 SLA Missed Alert (147) 🚨</b>
        <b>DAG:</b> {dag.dag_id}
        <b>Task(s):</b> {task_list}
        <b>Blocking Task(s):</b> {blocking_task_list}
        <b>SLA(s):</b> {slas}
        <b>Blocking Task Instances:</b> {blocking_tis}
        """
        logging.error("SLA miss callback triggered.")
        send_telegram_message(message)
    else:
        logging.error("SLA miss callback triggered with insufficient arguments.")


def slp():
    """Sleep for 1 minutes"""
    time.sleep(120)
    logging.info("Slept for 1 minutes")


def simple_print(**context):
    """Prints a message"""
    print("Hello World!")


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    sla_miss_callback=DefaultDAG.sla_miss_callback,
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2024, 11, 2, tz="UTC"),
    catchup=False,
) as dag:
    slep = PythonOperator(
        task_id="sleeep",
        python_callable=slp,
        sla=datetime.timedelta(seconds=10),
        dag=dag,
    )

    simple_task = PythonOperator(
        task_id="simpletask", python_callable=simple_print, dag=dag
    )

    slep >> simple_task
