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


import json
from typing import Dict

from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

DEFAULT_ARGS = {
    "owner": "airflow",
}


# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, schedule_interval=None)
def send_server_ip(email: str = 'example@example.com'):
    """
    DAG to send server IP to email.

    :param email: Email to send IP to. Defaults to example@example.com.
    :type email: str
    """
    # Using default connection as it's set to httpbin.org by default
    get_ip = SimpleHttpOperator(
        task_id='get_ip', endpoint='get', method='GET', xcom_push=True
    )

    @task(multiple_outputs=True)
    def prepare_email(raw_json: str) -> Dict[str, str]:
        external_ip = json.loads(raw_json)['origin']
        return {
            'subject': f'Server connected from {external_ip}',
            'body': f'Seems like today your server executing Airflow is connected from IP {external_ip}<br>'
        }

    email_info = prepare_email(get_ip.output)

    EmailOperator(
        task_id='send_email',
        to=email,
        subject=email_info['subject'],
        html_content=email_info['body']
    )


DAG = send_server_ip()
# [END dag_decorator_usage]
