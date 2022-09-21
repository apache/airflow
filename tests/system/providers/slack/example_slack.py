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
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.slack.operators.slack import SlackAPIFileOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "slack_example_dag"

# [START slack_operator_howto_guide]
with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    default_args={'slack_conn_id': 'slack', 'channel': '#general', 'initial_comment': 'Hello World!'},
    max_active_runs=1,
    tags=['example'],
) as dag:

    # Send file with filename and filetype
    slack_operator_file = SlackAPIFileOperator(
        task_id="slack_file_upload_1",
        filename="/files/dags/test.txt",
        filetype="txt",
    )

    # Send file content
    slack_operator_file_content = SlackAPIFileOperator(
        task_id="slack_file_upload_2",
        content="file content in txt",
    )
    # [END slack_operator_howto_guide]

    slack_operator_file >> slack_operator_file_content


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
