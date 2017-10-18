# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta

from airflow import DAG, AirflowException
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.utils.slack_webhook import notify_task_success, notify_task_retry, \
    notify_task_failure
from airflow.operators.python_operator import PythonOperator

# For test you have to create a connection and put the conn_id in `params.slack_conn_id`
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # retry once to see the retry message
    'retry_delay': timedelta(seconds=10),
    # will be annoying if every success tasks send notification
    # You can set `on_failure_callback` here as common setting,
    # then set other two on Operator initialization
    'on_success_callback': notify_task_success,
    'on_retry_callback': notify_task_retry,
    'on_failure_callback': notify_task_failure,
    'params': {
        'slack_conn_id': 'slack_webhook_not_default'
    }
}

dag = DAG('test_slack_notification', default_args=default_args, schedule_interval='@once')

t1 = SlackWebhookOperator(
    dag=dag,
    task_id='notify_slack_channel_by_webhook'
)


def failed():
    raise AirflowException("Raise this exception intentionally.")


t2 = PythonOperator(
    dag=dag,
    task_id='notify_on_retry_and_failure',
    python_callable=failed
)
