from __future__ import annotations
import requests
import json
import logging
from airflow.hooks.base import BaseHook


class AlertHook(BaseHook):
    def __init__(self, conn_id="telegram-dba"):
        super().__init__()
        self._conn_id = conn_id

    def _get_conn(self):
        "Get telegram bot token and chat id"
        config = self.get_connection(self._conn_id)
        self._token = config.password
        self._chat_id = config.host
        self._topic_id = json.loads(config.extra).get("message_thread_id", None)
        return self._token, self._chat_id, self._topic_id

    def send_message(self, text):
        """Send telegram message using defined connection"""
        token, chat_id, topic_id = self._get_conn()
        base_url = f"https://api.telegram.org/bot{token}/sendMessage"
        if topic_id:
            params = {
                "chat_id": chat_id,
                "text": text,
                "message_thread_id": topic_id,
                "parse_mode": "HTML",
            }
        else:
            params = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        try:
            response = requests.post(base_url, params=params, timeout=10)
            response.raise_for_status()
            logging.info("Message sent successfully")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send message: {e}")

    def send_alert(self, status, kwargs):
        """Callback function that is called when an DAG is succeeded/failed."""
        text = f"""<b>🚨 AIRFLOW ALERT 🚨</b>
<b>STATUS</b>: <b>{status}</b>
<b>dag_id</b>: <b>{kwargs['dag_run'].dag_id}</b>
<b>Run</b>: <b>{kwargs['dag_run'].run_id}</b>
<b>Task</b>: <b>{kwargs['task_instance_key_str']}</b>
<b>Execution time</b>: <b>{kwargs['data_interval_end'].in_tz("Asia/Jakarta").strftime("%Y-%m-%d, %H:%M:%S")}</b>"""
        self.send_message(text)

    def exception_alert(self, err, **kwargs):
        """Callback function that is called when an exception
        is occured at DAG task."""
        text = f"""<b>⚠️ AIRFLOW ALERT⚠️</b>
<b>STATUS</b>: <b>Exception Notification</b>
<b>dag_id</b>: <b>{kwargs['dag_run'].dag_id}</b>
<b>Run</b>: <b>{kwargs['dag_run'].run_id}</b>
<b>Task</b>: <b>{kwargs['task_instance_key_str']}</b>
<b>Execution time</b>: <b>{(kwargs['data_interval_end']).in_tz("Asia/Jakarta").strftime("%Y-%m-%d, %H:%M:%S")}</b>"""
        if err is not None:
            text = text + f"""<b>Error Description</b>: <b>{err}</b>"""
        self.send_message(text)

    def sla_miss_callback(self, status, kwargs):
        """Callback function that is called when an DAG SLA is missed."""
        text = f"""<b>⚠️ AIRFLOW ALERT ⚠️</b>
        <b>STATUS</b>: <b>{status}</b>
        <b>DAG:</b> {kwargs['dag_run'].dag_id}
        <b>Task(s):</b> {kwargs['task_instance_key_str']}
        <b>Blocking Task(s):</b> {kwargs['blocking_task_list']}
        <b>SLA(s):</b> {kwargs['slas']}
        <b>Blocking Task Instances:</b> {kwargs['blocking_tis']}
        """
        self.send_message(text)
