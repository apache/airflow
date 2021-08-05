# -*- coding:utf-8 -*-

import datetime as dt
import pendulum
from airflow import DAG
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.logger import generate_logger
from sqlalchemy import func
from airflow.utils.db import provide_session
from airflow.utils import timezone
_logger = generate_logger(__name__)

local_tz = pendulum.timezone("Asia/Shanghai")
desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz)
}

dag_id = 'report_retry'
task_report_retry = 'task_report_retry'

dag = DAG(dag_id, description=u'分析任务重试数量报警推送',
          schedule_interval='@hourly', default_args=desoutter_default_args, catchup=False)


@provide_session
def retried_tasks(delta=None, session=None):
    tasks = session.query(func.count(TaskInstance.task_id)).filter(TaskInstance._try_number > 1)
    if delta:
        return tasks.filter(
            TaskInstance.execution_date > (timezone.utcnow() + delta)).first()[0]
    return tasks.first()[0]


def retry_counts(period):
    delta = timedelta(**period)
    tasks = retried_tasks(delta=delta)
    _logger.debug('found {} retried tasks'.format(tasks))
    return tasks


def should_report(period, threshold):
    if not threshold or not period:
        return False
    count = retry_counts(period)
    if int(count) >= int(threshold):
        return count
    return False


def report_message(period, count):
    return u'在过去{}中，有{}任务进行了重试'.format(period, count)


def do_report(**kwargs):
    period = Variable.get('task_retry_period', {'hours': -1}, deserialize_json=True)
    threshold = Variable.get('task_retry_threshold', 1)
    count = should_report(period, threshold)
    if not count:
        _logger.info('未达到报警条件, period: {}, threshold: {}'.format(period, threshold))
        return
    mails_task_retry = Variable.get('mails_task_retry', [])
    if not mails_task_retry or not len(mails_task_retry) > 0:
        raise Exception('重试报警邮箱未配置')
    send_email(
        to=mails_task_retry,
        subject=u'分析任务重试数量报警',
        html_content=report_message(period, count)
    )


report_task = PythonOperator(
    provide_context=True,
    task_id=task_report_retry,
    dag=dag,
    python_callable=do_report
)
