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
Example Airflow DAG that translates text in Google Cloud Translate using V3 API version
service in the Google Cloud.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.translate import (
    TranslateCreateGlossaryOperator,
    TranslateDeleteGlossaryOperator,
    TranslateListGlossariesOperator,
    TranslateUpdateGlossaryOperator,
)

DAG_ID = "translate_glossary"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
REGION = "us-central1"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

GLOSSARY_FILE_INPUT = {
    "gcs_source": {"input_uri": f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/glossaries/glossary_sample.tsv"}
}
UPDATE_GLOSSARY_FILE_INPUT = {
    "gcs_source": {
        "input_uri": f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/glossaries/glossary_update_sample.tsv"
    }
}


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "glossary_translate", "translate_V3"],
) as dag:
    # [START howto_operator_translate_create_glossary]
    create_glossary = TranslateCreateGlossaryOperator(
        task_id="glossary_create",
        project_id=PROJECT_ID,
        location=REGION,
        input_config=GLOSSARY_FILE_INPUT,
        glossary_id=f"glossary_new_{PROJECT_ID}",
        language_pair={"source_language_code": "en", "target_language_code": "es"},
    )
    # [END howto_operator_translate_create_glossary]

    # [START howto_operator_translate_update_glossary]
    glossary_id = create_glossary.output["glossary_id"]
    update_glossary = TranslateUpdateGlossaryOperator(
        task_id="glossary_update",
        project_id=PROJECT_ID,
        location=REGION,
        new_input_config=UPDATE_GLOSSARY_FILE_INPUT,
        new_display_name=f"gl_{PROJECT_ID}_updated",
        glossary_id=glossary_id,
    )
    # [END howto_operator_translate_update_glossary]

    # [START howto_operator_translate_list_glossaries]
    list_glossaries = TranslateListGlossariesOperator(
        task_id="list_glossaries",
        page_size=100,
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_list_glossaries]

    # [START howto_operator_translate_delete_glossary]
    delete_glossary = TranslateDeleteGlossaryOperator(
        task_id="delete_glossary",
        glossary_id=glossary_id,
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_delete_glossary]

    (
        # TEST BODY
        create_glossary >> update_glossary >> list_glossaries >> delete_glossary
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
