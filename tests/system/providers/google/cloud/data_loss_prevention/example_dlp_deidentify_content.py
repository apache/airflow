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
Example Airflow DAG that de-identifies potentially sensitive info using Data Loss Prevention operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.dlp_v2.types import ContentItem, DeidentifyTemplate, InspectConfig

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateDeidentifyTemplateOperator,
    CloudDLPDeidentifyContentOperator,
    CloudDLPDeleteDeidentifyTemplateOperator,
    CloudDLPGetDeidentifyTemplateOperator,
    CloudDLPListDeidentifyTemplatesOperator,
    CloudDLPReidentifyContentOperator,
    CloudDLPUpdateDeidentifyTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dlp_deidentify_content"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
CRYPTO_KEY_NAME = f"{DAG_ID}_{ENV_ID}"

ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}])

# [START dlp_deidentify_config_example]
DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "replace_config": {"new_value": {"string_value": "[deidentified_number]"}}
                }
            }
        ]
    }
}
# [END dlp_deidentify_config_example]

REVERSIBLE_DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "crypto_deterministic_config": {
                        "crypto_key": {"transient": {"name": CRYPTO_KEY_NAME}},
                        "surrogate_info_type": {"name": "PHONE_NUMBER"},
                    }
                }
            }
        ]
    }
}

TEMPLATE_ID = f"template_{DAG_ID}_{ENV_ID}"
DEIDENTIFY_TEMPLATE = DeidentifyTemplate(deidentify_config=DEIDENTIFY_CONFIG)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["dlp", "example"],
) as dag:
    # [START _howto_operator_dlp_deidentify_content]
    deidentify_content = CloudDLPDeidentifyContentOperator(
        project_id=PROJECT_ID,
        item=ITEM,
        deidentify_config=DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content",
    )
    # [END _howto_operator_dlp_deidentify_content]

    reidentify_content = CloudDLPReidentifyContentOperator(
        task_id="reidentify_content",
        project_id=PROJECT_ID,
        item=ContentItem(value="{{ task_instance.xcom_pull('deidentify_content')['item'] }}"),
        reidentify_config=REVERSIBLE_DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
    )

    create_template = CloudDLPCreateDeidentifyTemplateOperator(
        task_id="create_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        deidentify_template=DEIDENTIFY_TEMPLATE,
    )

    list_templates = CloudDLPListDeidentifyTemplatesOperator(task_id="list_templates", project_id=PROJECT_ID)

    get_template = CloudDLPGetDeidentifyTemplateOperator(
        task_id="get_template", project_id=PROJECT_ID, template_id=TEMPLATE_ID
    )

    update_template = CloudDLPUpdateDeidentifyTemplateOperator(
        task_id="update_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        deidentify_template=DEIDENTIFY_TEMPLATE,
    )

    deidentify_content_with_template = CloudDLPDeidentifyContentOperator(
        project_id=PROJECT_ID,
        item=ITEM,
        deidentify_template_name="{{ task_instance.xcom_pull('create_template')['name'] }}",
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content_with_template",
    )

    delete_template = CloudDLPDeleteDeidentifyTemplateOperator(
        task_id="delete_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        deidentify_content
        >> reidentify_content
        >> create_template
        >> list_templates
        >> get_template
        >> update_template
        >> deidentify_content_with_template
        >> delete_template
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
