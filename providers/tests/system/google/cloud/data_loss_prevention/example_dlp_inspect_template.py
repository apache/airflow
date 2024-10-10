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
Example Airflow DAG that creates, updates, list and deletes Data Loss Prevention inspect templates.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.dlp_v2.types import ContentItem, InspectConfig, InspectTemplate

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateInspectTemplateOperator,
    CloudDLPDeleteInspectTemplateOperator,
    CloudDLPGetInspectTemplateOperator,
    CloudDLPInspectContentOperator,
    CloudDLPListInspectTemplatesOperator,
    CloudDLPUpdateInspectTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dlp_inspect_template"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

TEMPLATE_ID = f"dlp-inspect-{ENV_ID}"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}])
INSPECT_TEMPLATE = InspectTemplate(inspect_config=INSPECT_CONFIG)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["dlp", "example"],
) as dag:
    # [START howto_operator_dlp_create_inspect_template]
    create_template = CloudDLPCreateInspectTemplateOperator(
        task_id="create_template",
        project_id=PROJECT_ID,
        inspect_template=INSPECT_TEMPLATE,
        template_id=TEMPLATE_ID,
        do_xcom_push=True,
    )
    # [END howto_operator_dlp_create_inspect_template]

    list_templates = CloudDLPListInspectTemplatesOperator(
        task_id="list_templates",
        project_id=PROJECT_ID,
    )

    get_template = CloudDLPGetInspectTemplateOperator(
        task_id="get_template", project_id=PROJECT_ID, template_id=TEMPLATE_ID
    )

    update_template = CloudDLPUpdateInspectTemplateOperator(
        task_id="update_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        inspect_template=INSPECT_TEMPLATE,
    )

    # [START howto_operator_dlp_use_inspect_template]
    inspect_content = CloudDLPInspectContentOperator(
        task_id="inspect_content",
        project_id=PROJECT_ID,
        item=ITEM,
        inspect_template_name="{{ task_instance.xcom_pull('create_template', key='return_value')['name'] }}",
    )
    # [END howto_operator_dlp_use_inspect_template]

    # [START howto_operator_dlp_delete_inspect_template]
    delete_template = CloudDLPDeleteInspectTemplateOperator(
        task_id="delete_template",
        template_id=TEMPLATE_ID,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_dlp_delete_inspect_template]
    delete_template.trigger_rule = TriggerRule.ALL_DONE

    (
        create_template
        >> list_templates
        >> get_template
        >> update_template
        >> inspect_content
        >> delete_template
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
