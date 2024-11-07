from __future__ import annotations
from airflow import DAG
from default.custom_hook import AlertHook


class DefaultDAG(DAG):
    alert_manager = None

    def __init__(self, *args, conn_id=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_args = self.__build_args(kwargs)
        if conn_id:
            DefaultDAG.alert_manager = AlertHook(conn_id)

    def __build_args(self, kwargs):
        default_args = {
            "owner": "data-engineer dba",
            "start_date": kwargs["start_date"],
            "wait_for_downstream": False,
        }
        default_args.update(kwargs.get("default_args", {}))
        return default_args

    @classmethod
    def success_callback(cls, kwargs):
        "Send telegram notification when DAG task is succeeded"
        if cls.alert_manager:
            cls.alert_manager.send_alert("TASK IS SUCCEEDED", kwargs)

    @classmethod
    def failure_callback(cls, kwargs):
        "Send telegram notification when DAG task is failed"
        if cls.alert_manager:
            cls.alert_manager.send_alert("TASK IS FAILED", kwargs)

    @classmethod
    def sla_miss_callback(cls, *args):
        "Send telegram notification when DAG runtime duration is exceed defined SLA"
        if cls.alert_manager:
            cls.alert_manager.sla_miss_callback("DAG RUNTIME IS OVER SLA", *args)

    @classmethod
    def exception_alert(cls, err, **kwargs):
        """Send telegram notification when exception occured at
        try ... except .. logic at a DAG task"""
        if cls.alert_manager:
            cls.alert_manager.exception_alert(err, **kwargs)
