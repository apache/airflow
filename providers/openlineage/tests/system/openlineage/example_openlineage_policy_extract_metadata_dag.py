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
DAG exercising the OpenLineage emission policy authoring API.

Per-task ``extract_operator_metadata=False`` gives empty inputs/outputs.
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import Dataset

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.openlineage.extractors.base import OperatorLineage

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class _PolicyCustomOp(BaseOperator):
    """Custom operator returning fixed inputs/outputs via the OL extractor API."""

    def execute(self, context):
        print("custom op ran")

    def get_openlineage_facets_on_complete(self, task_instance):
        return OperatorLineage(
            inputs=[Dataset("s3://ol-test-in", "in.csv")],
            outputs=[Dataset("s3://ol-test-out", "out.csv")],
        )


DAG_ID = "openlineage_policy_extract_metadata_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    t_with_extract = _PolicyCustomOp(task_id="t_with_extract")
    t_no_extract = extend_global_openlineage_emission_policy(
        _PolicyCustomOp(task_id="t_no_extract"),
        extract_operator_metadata=False,
    )
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    t_with_extract >> t_no_extract >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
