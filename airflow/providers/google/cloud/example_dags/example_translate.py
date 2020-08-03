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
Example Airflow DAG that translates text in Google Cloud Translate
service in the Google Cloud Platform.

"""

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.translate import CloudTranslateTextOperator
from airflow.utils.dates import days_ago

with models.DAG(
    'example_gcp_translate',
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    # [START howto_operator_translate_text]
    product_set_create = CloudTranslateTextOperator(
        task_id='translate',
        values=['zażółć gęślą jaźń'],
        target_language='en',
        format_='text',
        source_language=None,
        model='base',
    )
    # [END howto_operator_translate_text]
    # [START howto_operator_translate_access]
    translation_access = BashOperator(
        task_id='access',
        bash_command="echo '{{ task_instance.xcom_pull(\"translate\")[0] }}'"
    )
    product_set_create >> translation_access
    # [END howto_operator_translate_access]
