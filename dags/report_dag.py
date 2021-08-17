# -*- coding:utf-8 -*-

import datetime as dt
import requests
import os
import pendulum
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import errno

file_ok_report = "ok_report.pdf"
file_task = "task.pdf"
file_diff = "diff.pdf"
file_path = os.path.join(os.environ.get('AIRFLOW_USER_HOME', '/usr/local/airflow'))
if not os.path.exists(os.path.dirname(file_path)):
    try:
        os.makedirs(os.path.dirname(file_path))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

target_file_ok_report = os.path.join(file_path, file_ok_report)
target_file_task_report = os.path.join(file_path, file_task)
target_file_diff = os.path.join(file_path, file_diff)

local_tz = pendulum.timezone("Asia/Shanghai")
desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz)
}

# airflow params
dashboard_ok_rate = Variable.get('dashboard_ok_rate', 'H2XWxQMGk')
dashboard_task = Variable.get('dashboard_task', 'IoRG5XGMz')
dashboard_diff = Variable.get('dashboard_diff', '51JOPwGGk')
mails_ok_rate = Variable.get('mails_ok_rate', '')
mails_task = Variable.get('mails_task', '')
mails_diff = Variable.get('mails_diff', '')


def save_pdf_file(url, filename):
    resp = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(resp.content)


dag_id = 'report'
task_report_ok_rate = 'task_report_ok_rate'
task_load_file = 'task_load_file'
task_report_task = 'task_report_task'
task_report_diff = 'task_report_diff'
line_code = 'All'

dag = DAG(dag_id, description=u'日报表推送',
          schedule_interval='@daily', default_args=desoutter_default_args, catchup=False)


def do_load_file(**kwargs):
    from airflow.models.connection import Connection
    report = Connection.get_connection_from_secrets('qcos_report')
    reporter_url = '{}:{}'.format(report.host, report.port) if report else ''
    grafana_token = report.get_password() if report else ''

    report_ok_rate_url = 'http://{}/api/v5/report/{}?apitoken={}&var-line_code={}'.format(reporter_url, dashboard_ok_rate,
                                                                                      grafana_token, line_code)
    report_task_url = 'http://{}/api/v5/report/{}?apitoken={}&var-line_code={}'.format(reporter_url, dashboard_task,
                                                                                   grafana_token, line_code)
    report_diff_url = 'http://{}/api/v5/report/{}?apitoken={}&var-line_code={}'.format(reporter_url, dashboard_diff,
                                                                                   grafana_token, line_code)
    save_pdf_file(report_ok_rate_url, target_file_ok_report)
    save_pdf_file(report_task_url, target_file_task_report)
    save_pdf_file(report_diff_url, target_file_diff)


load_file_task = PythonOperator(provide_context=True,
                                task_id=task_load_file, dag=dag,
                                python_callable=do_load_file)

report_ok_rate = EmailOperator(
    task_id=task_report_ok_rate,
    to=mails_ok_rate,
    subject=u'日拧紧合格率报表',
    html_content=u"""<h3>SAIC OK Report</h3>""",
    files=[target_file_ok_report],
    dag=dag
)

report_task = EmailOperator(
    task_id=task_report_task,
    to=mails_task,
    subject=u'日分析任务报表',
    html_content=u"""<h3>SAIC Task Report</h3>""",
    files=[target_file_task_report],
    dag=dag
)

report_diff = EmailOperator(
    task_id=task_report_diff,
    to=mails_diff,
    subject=u'日分析差异报表',
    html_content=u"""<h3>SAIC Diff Report</h3>""",
    files=[target_file_diff],
    dag=dag
)

load_file_task >> [report_ok_rate, report_task, report_diff]
