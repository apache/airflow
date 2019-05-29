# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta

import airflow
from airflow.contrib.operators.dingding_operator import DingdingOperator
from airflow.models import DAG

args = {
    'owner': 'airflow',
    'retries': 3,
    'start_date': airflow.utils.dates.days_ago(2),
}


# [START howto_operator_dingding_failure_callback]
def failure_callback(context):
    message = 'AIRFLOW TASK FAILURE TIPS:\n' \
              'DAG:    {}\n' \
              'TASKS:  {}\n' \
              'Reason: {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id,
                context['exception'])
    return DingdingOperator(
        task_id='dingding_success_callback',
        dingding_conn_id='dingding_default',
        message_type='text',
        message=message,
        at_all=True,
    ).execute(context)


args['on_failure_callback'] = failure_callback
# [END howto_operator_dingding_failure_callback]

dag = DAG(
    dag_id='example_dingding_operator',
    default_args=args,
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=60),
)

# [START howto_operator_dingding]
text_msg_remind_none = DingdingOperator(
    task_id='text_msg_remind_none',
    dingding_conn_id='dingding_default',
    message_type='text',
    message='Airflow dingding text message remind none',
    at_mobiles=None,
    at_all=False,
    dag=dag,
)
# [END howto_operator_dingding]

text_msg_remind_specific = DingdingOperator(
    task_id='text_msg_remind_specific',
    dingding_conn_id='dingding_default',
    message_type='text',
    message='Airflow dingding text message remind specific users',
    at_mobiles=['156XXXXXXXX', '130XXXXXXXX'],
    at_all=False,
    dag=dag,
)

text_msg_remind_include_invalid = DingdingOperator(
    task_id='text_msg_remind_include_invalid',
    dingding_conn_id='dingding_default',
    message_type='text',
    message='Airflow dingding text message remind users including invalid',
    # 123 is invalid user or user not in the group
    at_mobiles=['156XXXXXXXX', '123'],
    at_all=False,
    dag=dag,
)

# [START howto_operator_dingding_remind_users]
text_msg_remind_all = DingdingOperator(
    task_id='text_msg_remind_all',
    dingding_conn_id='dingding_default',
    message_type='text',
    message='Airflow dingding text message remind all users in group',
    # list of user phone/email here in the group
    # when at_all is specific will cover at_mobiles
    at_mobiles=['156XXXXXXXX', '130XXXXXXXX'],
    at_all=True,
    dag=dag,
)
# [END howto_operator_dingding_remind_users]

link_msg = DingdingOperator(
    task_id='link_msg',
    dingding_conn_id='dingding_default',
    message_type='link',
    message={
        'title': 'Airflow dingding link message',
        'text': 'Airflow official documentation link',
        'messageUrl': 'http://airflow.apache.org',
        'picURL': 'http://airflow.apache.org/_images/pin_large.png'
    },
    dag=dag,
)

# [START howto_operator_dingding_rich_text]
markdown_msg = DingdingOperator(
    task_id='markdown_msg',
    dingding_conn_id='dingding_default',
    message_type='markdown',
    message={
        'title': 'Airflow dingding markdown message',
        'text': '# Markdown message title\n'
                'content content .. \n'
                '### sub-title\n'
                '![logo](http://airflow.apache.org/_images/pin_large.png)'
    },
    at_mobiles=['156XXXXXXXX'],
    at_all=False,
    dag=dag,
)
# [END howto_operator_dingding_rich_text]

single_action_card_msg = DingdingOperator(
    task_id='single_action_card_msg',
    dingding_conn_id='dingding_default',
    message_type='actionCard',
    message={
        'title': 'Airflow dingding single actionCard message',
        'text': 'Airflow dingding single actionCard message\n'
                '![logo](http://airflow.apache.org/_images/pin_large.png)\n'
                'This is a official logo in Airflow website.',
        'hideAvatar': '0',
        'btnOrientation': '0',
        'singleTitle': 'read more',
        'singleURL': 'http://airflow.apache.org'
    },
    dag=dag,
)

multi_action_card_msg = DingdingOperator(
    task_id='multi_action_card_msg',
    dingding_conn_id='dingding_default',
    message_type='actionCard',
    message={
        'title': 'Airflow dingding multi actionCard message',
        'text': 'Airflow dingding multi actionCard message\n'
                '![logo](http://airflow.apache.org/_images/pin_large.png)\n'
                'Airflow documentation and github',
        'hideAvatar': '0',
        'btnOrientation': '0',
        'btns': [
            {
                'title': 'Airflow Documentation',
                'actionURL': 'http://airflow.apache.org'
            },
            {
                'title': 'Airflow Github',
                'actionURL': 'https://github.com/apache/airflow'
            }
        ]
    },
    dag=dag,
)

feed_card_msg = DingdingOperator(
    task_id='feed_card_msg',
    dingding_conn_id='dingding_default',
    message_type='feedCard',
    message={
        "links": [
            {
                "title": "Airflow DAG feed card",
                "messageURL": "https://airflow.readthedocs.io/en/latest/ui.html",
                "picURL": "http://airflow.apache.org/_images/dags.png"
            },
            {
                "title": "Airflow tree feed card",
                "messageURL": "https://airflow.readthedocs.io/en/latest/ui.html",
                "picURL": "http://airflow.apache.org/_images/tree.png"
            },
            {
                "title": "Airflow graph feed card",
                "messageURL": "https://airflow.readthedocs.io/en/latest/ui.html",
                "picURL": "http://airflow.apache.org/_images/graph.png"
            }
        ]
    },
    dag=dag,
)

msg_failure_callback = DingdingOperator(
    task_id='msg_failure_callback',
    dingding_conn_id='dingding_default',
    message_type='not_support_msg_type',
    message="",
    dag=dag,
)

[
    text_msg_remind_none,
    text_msg_remind_specific,
    text_msg_remind_include_invalid,
    text_msg_remind_all
] >> link_msg >> markdown_msg >> [
    single_action_card_msg,
    multi_action_card_msg
] >> feed_card_msg >> msg_failure_callback
