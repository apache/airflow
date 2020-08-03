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
Example Airflow DAG that execute the following tasks using
Cloud DLP service in the Google Cloud Platform:
1) Creating a content inspect template;
2) Using the created template to inspect content;
3) Deleting the template from GCP .
"""

import os

from google.cloud.dlp_v2.types import ContentItem, InspectConfig, InspectTemplate

from airflow import models
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateInspectTemplateOperator, CloudDLPDeleteInspectTemplateOperator,
    CloudDLPInspectContentOperator,
)
from airflow.utils.dates import days_ago

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "example-project")
TEMPLATE_ID = "dlp-inspect-838746"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(
    info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}]
)
INSPECT_TEMPLATE = InspectTemplate(inspect_config=INSPECT_CONFIG)


with models.DAG(
    "example_gcp_dlp",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    create_template = CloudDLPCreateInspectTemplateOperator(
        project_id=GCP_PROJECT,
        inspect_template=INSPECT_TEMPLATE,
        template_id=TEMPLATE_ID,
        task_id="create_template",
        do_xcom_push=True,
        dag=dag,
    )

    inspect_content = CloudDLPInspectContentOperator(
        task_id="inpsect_content",
        project_id=GCP_PROJECT,
        item=ITEM,
        inspect_template_name="{{ task_instance.xcom_pull('create_template', key='return_value')['name'] }}",
        dag=dag,
    )

    delete_template = CloudDLPDeleteInspectTemplateOperator(
        task_id="delete_template",
        template_id=TEMPLATE_ID,
        project_id=GCP_PROJECT,
        dag=dag,
    )

    create_template >> inspect_content >> delete_template
