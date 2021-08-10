# -*- coding:utf-8 -*-

import datetime as dt
import pendulum
from airflow import DAG
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator
from plugins.utils.logger import generate_logger
from airflow.utils.db import provide_session
from sqlalchemy import func
from datetime import timedelta
from airflow.utils import timezone

_logger = generate_logger(__name__)

local_tz = pendulum.timezone("Asia/Shanghai")
desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz)
}

mail_key = 'mails_analysis_timeout'


@provide_session
def timeout_count(period, threshold, session=None):
    delta = timedelta(**period)

    tasks = session.query(func.count(TaskInstance.task_id)) \
        .filter(TaskInstance.task_id.in_(('trigger_anay_task', 'curve_training_task'))) \
        .filter(TaskInstance.duration > threshold)
    if delta:
        return tasks.filter(TaskInstance.execution_date > (timezone.utcnow() + delta)).first()[0]
    return 0


def report_message(period, count, threshold):
    return u'过去{}中，有{}个分析任务执行时间超过{}。'.format(period, count, threshold)


def report_analysis_timeout(**kwargs):
    period = Variable.get('report_analysis_timeout_period', {'hours': -1}, deserialize_json=True)
    threshold = Variable.get('report_analysis_timeout_threshold', 10)
    count = timeout_count(period, threshold)
    if count == 0:
        _logger.info(u'未达到报警条件, period: {}, threshold: {}'.format(period, threshold))
        return
    mails_analysis_timeout = Variable.get(mail_key, [])
    if not mails_analysis_timeout or not len(mails_analysis_timeout) > 0:
        raise Exception(u'分析超时报警邮箱未配置:{}，请在变量中添加"{}"变量'.format(str(mails_analysis_timeout), mail_key))
    send_email(
        to=mails_analysis_timeout,
        subject=u'分析任务超时报警',
        html_content=report_message(period, count, threshold),
        mime_charset='utf-8'
    )


dag = DAG('report_analysis_timeout', description=u'分析任务超时报警',
          schedule_interval='@hourly', default_args=desoutter_default_args, catchup=False)

report_analysis_timeout_task = PythonOperator(
    provide_context=True,
    task_id='task_report_analysis_timeout',
    dag=dag,
    python_callable=report_analysis_timeout
)
