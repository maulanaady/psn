import os
import logging
from datetime import timedelta, datetime
import pendulum
from airflow.decorators import task
from default.default_config import DefaultDAG
from default.custom_connection import ConnectionHook


with DefaultDAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=pendulum.today("UTC").add(days=-1),
    conn_id="telegram-dba",
    on_failure_callback=DefaultDAG.failure_callback,
    schedule="@hourly",
    max_active_runs=1,
    catchup=False,
    tags=["udr"],
) as dag:

    @task.short_circuit()
    def checking_file(conn_id, **context):
        execution = context["data_interval_end"].date().strftime("%Y-%m-%d")
        command = f"""cd /tmpdir/mariabackup/zip && ls -alh | grep {execution}"""
        cmd = "sudo ps -ax | grep 'tar -zcvf /tmpdir/mariabackup/zip/'"
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            _, stdout, _ = ssh_client.exec_command(command)
            line = stdout.readlines()
            _, stdout, _ = ssh_client.exec_command(cmd)
            process = stdout.readlines()
            if len(line) > 0 and len(process) == 2:
                list_dev = []
                for _, item in enumerate(line):
                    x = item.split()
                    list_dev.append(x)
                current = datetime.now() + timedelta(hours=7)
                day = (
                    datetime.today().date().strftime("%Y-%m-%d") + " " + list_dev[0][7]
                )
                file_time = datetime.strptime(day, "%Y-%m-%d %H:%M")
                diff = (current - file_time).total_seconds() / 60
                if diff > 10:
                    filename = list_dev[0][8]
                    continues = True
                else:
                    filename = None
                    continues = False
            else:
                filename = None
                continues = False
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)
            filename = None
            continues = False
        finally:
            context["task_instance"].xcom_push(key="filename", value=filename)
            ssh_client.close()
        return continues

    @task.branch()
    def copy_file(conn_id, **context):
        file = context["task_instance"].xcom_pull(
            task_ids="checking_file", key="filename"
        )
        copy_command = f"""sudo scp ubuntu@192.168.41.7:/tmpdir/mariabackup/zip/{file} \
            /mnt/cephfs-nfs/bigdata/backup_test/zip"""
        check_command = (
            f"""cd /mnt/cephfs-nfs/bigdata/backup_test/zip && ls -alh | grep {file}"""
        )
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            _, stdout, _ = ssh_client.exec_command(copy_command)
            for line in iter(stdout.readline, ""):
                logging.info("%s", line.strip())
            _, stdout, _ = ssh_client.exec_command(check_command)
            line = stdout.readlines()
            ssh_client.close()
            if len(line) > 0:
                return ["delete_task"]
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)
        return ["notify_error_copy_file"]

    # @task.branch()
    # def check_copy_result(conn_id, **context):
    #     file = context["task_instance"].xcom_pull(task_ids="checking_file", key="filename")
    #     check_command = f"""cd /mnt/cephfs-nfs/bigdata/backup_test/zip && ls -alh | grep {file}"""
    #     try:
    #         ssh_client = ConnectionHook.get_ssh_connection(conn_id)
    #         _, stdout, _ = ssh_client.exec_command(check_command)
    #         line = stdout.readlines()
    #         ssh_client.close()
    #         if len(line) > 0:
    #             return ['delete_task']
    #     except RuntimeError as err:
    #         DefaultDAG.exception_alert(err, **context)
    #     return ['notify_error_copy_file']

    @task()
    def delete_task(conn_id, **context):
        file = context["task_instance"].xcom_pull(
            task_ids="checking_file", key="filename"
        )
        date = context["data_interval_end"].date().strftime("%Y-%m-%d")
        rm_zip_command = f"""cd /tmpdir/mariabackup/zip && sudo rm -f {file} """
        rm_dir_command = f"sudo rm -rf /tmpdir/mariabackup/base/{date}"
        try:
            ssh_client = ConnectionHook.get_ssh_connection(conn_id)
            ssh_client.exec_command(rm_zip_command)
            ssh_client.exec_command(rm_dir_command)
        except RuntimeError as err:
            DefaultDAG.exception_alert(err, **context)
        finally:
            ssh_client.close()

    @task()
    def notify_error_copy_file(**context):
        from default.custom_hook import AlertHook

        message = f"""<b>STATUS</b>: <b>TASK copy_file IS FAILED</b>
        <b>dag_id</b>: <b>{context["dag_run"].dag_id}</b>
        <b>Run</b>: <b>{context["dag_run"].run_id}</b>
        <b>Task</b>: <b>{context["task"].task_id}</b>
        <b>Execution time</b>: <b>{((context['data_interval_end'])
            .in_tz("Asia/Jakarta").strftime("%Y-%m-%d, %H:%M:%S"))}</b>"""
        AlertHook.send_message(text=message, **context)

    (
        checking_file("ssh-analysis")
        >> copy_file("ssh-nfs")
        >> [delete_task("ssh-analysis"), notify_error_copy_file()]
    )
