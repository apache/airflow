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

"""
Example Airflow DAG that uses Bash operator together with Google services
"""
import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")

with models.DAG(
    "example_google_bash_operator",
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_bash_gcloud]
    gcloud_task = BashOperator(
        task_id='gcloud',
        bash_command=(
            'curl -H "Authorization: Bearer $(gcloud auth print-access-token)" '
            '"https://oauth2.googleapis.com/tokeninfo"'
        ),
        gcp_conn_id=GCP_PROJECT_ID
    )
    # [END howto_operator_bash_gcloud]

    # [START howto_operator_bash_custom]
    CURRENT_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
    SCRIPT_PATH = os.path.join(CURRENT_DAG_FOLDER, 'example_google_bash_operator_custom_script.py')
    gcloud_custom_app = BashOperator(
        task_id='custom_app',
        bash_command=(
            f'python {SCRIPT_PATH}'
        ),
        gcp_conn_id=GCP_PROJECT_ID
    )
    # [END howto_operator_bash_custom]
