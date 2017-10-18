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

import json
import logging

import requests
from airflow import configuration, AirflowException
from airflow.hooks.base_hook import BaseHook

SLACK_WEBHOOK_HOST = "https://hooks.slack.com"


def construct_attachment(text='',
                         dag_id=None,
                         task_id=None,
                         owner='airflow',
                         status=None,
                         execution_time=None,
                         duration=None,
                         base_url=configuration.get('webserver', 'base_url'),
                         color='',  # None will be invalid
                         context=None):
    if context:
        ti = context['ti']
        dag_id = ti.dag_id
        task_id = ti.task_id
        status = ti.state.upper()
        owner = context['dag'].owner
        execution_time = context['ts']  # 'ts' is string, 'execution_time' is datetime object
        duration = ti.duration
    # escape '{' '}' with '{{' '}}' since we apply str.format() to it
    template = '''
    {{
      "color": "{color}",
      "text": "{text}",
      "fields": [
        {{
          "title": "DAG",
          "value": "<{base_url}/admin/airflow/tree?dag_id={dag_id}|{dag_id}>",
          "short": true
        }},
        {{
          "title": "Owner",
          "value": "{owner}",
          "short": true
        }},
        {{
          "title": "Task",
          "value": "{task_id}",
          "short": false
        }},
        {{
          "title": "Status",
          "value": "{status}",
          "short": false
        }},
        {{
          "title": "Execution Time",
          "value": "{execution_time}",
          "short": true
        }},
        {{
          "title": "Duration",
          "value": "{duration}",
          "short": true
        }},
        {{
          "value": "<{base_url}/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_time}|View Task Log>",
          "short": false
        }}
      ]
    }}
    '''
    return json.loads(template.format(**locals()))


def get_webhook_urls(slack_conn_id='slack_webhook_default'):
    conn = BaseHook.get_connection(slack_conn_id)
    if not conn.host:
        raise AirflowException(
            "Connection {} may not have host specified.".format(slack_conn_id))
    webhook_urls = conn.host.split(',')
    # check url if it's valid slack webhook url
    for url in webhook_urls:
        if not url.startswith(SLACK_WEBHOOK_HOST):
            raise AirflowException(
                "Connection {} contains invalid Slack Webhook URL {}".format(slack_conn_id, url))
    return webhook_urls


def post_message_via_webhook(message_json, webhook_urls=get_webhook_urls()):
    logging.info("Message is prepared: \n{}".format(json.dumps(message_json)))
    session = requests.Session()
    for url in webhook_urls:
        logging.info("Sending message to slack webhook: {}".format(url))
        response = session.post(url, json=message_json)
        if not response.status_code == 200:
            logging.error("Failed to send message: {}".format(response.text))
            return response
    return None


def get_webhook_urls_from_context(context):
    """
    Try to retrieve slack_conn_id from params applied when DAG initialization.
    Use default if not found or not specified.
    :param context:
    :return:
    """
    try:
        webhook_urls = get_webhook_urls(context['params']['slack_conn_id'])
    except (KeyError, AttributeError):
        logging.warning(
            "Not found value of dag.params['slack_conn_id'], \
            use fallback conn_id='slack_webhook_default' instead.")
        webhook_urls = get_webhook_urls()
    return webhook_urls


def notify_task_failure(context):
    """
    Use as the callback function of task constructor, usually put this into DAG default_args
    and apply to all tasks for the DAG, but you still can specify it for particular task.

    :param context: passed from Task instance
    :return:
    """
    post_message_via_webhook(
        message_json={"attachments": [
            construct_attachment(status='FAILURE', color='danger', context=context)]},
        webhook_urls=get_webhook_urls_from_context(context))


def notify_task_success(context):
    post_message_via_webhook(
        message_json={
            "attachments": [construct_attachment(status='SUCCESS', color='good', context=context)]},
        webhook_urls=get_webhook_urls_from_context(context))


def notify_task_retry(context):
    post_message_via_webhook(
        message_json={"attachments": [
            construct_attachment(status='RETRY', color='warning', context=context)]},
        webhook_urls=get_webhook_urls_from_context(context))
