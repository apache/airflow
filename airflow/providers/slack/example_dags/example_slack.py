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

from airflow.models.dag import DAG
from airflow.providers.slack.operators.slack import SlackAPIFileOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='slack_example_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:
    # Send file with filename and filetype
    slack_operator_file = SlackAPIFileOperator(
        task_id="slack_file_upload_1",
        dag=dag,
        slack_conn_id="slack",
        channel="#general",
        initial_comment="Hello World!",
        filename="/files/dags/test.txt",
        filetype="txt",
    )

    # Send file content
    slack_operator_file_content = SlackAPIFileOperator(
        task_id="slack_file_upload_2",
        dag=dag,
        slack_conn_id="slack",
        channel="#general",
        initial_comment="Hello World!",
        content="file content in txt",
    )

    slack_operator_file >> slack_operator_file_content
