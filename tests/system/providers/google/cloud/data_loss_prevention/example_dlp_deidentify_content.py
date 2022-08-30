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
Example Airflow DAG that de-identifies potentially sensitive info using CloudDLPDeidentifyContentOperator.
"""

from __future__ import annotations
import os
from datetime import datetime

from google.cloud.dlp_v2.types import ContentItem, InspectConfig

from airflow import models
from airflow.providers.google.cloud.operators.dlp import CloudDLPDeidentifyContentOperator

DAG_ID = "dlp_deidentify_content"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

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

with models.DAG(
    DAG_ID,
    schedule='@once',
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
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
