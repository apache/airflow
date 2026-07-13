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
DAG with a custom operator that intentionally fails and implements get_openlineage_facets_on_failure.

It checks:
    - START event carries inputs from get_openlineage_facets_on_start
    - FAIL event carries all expected facets + run facets from get_openlineage_facets_on_failure
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import InputDataset

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class FailingOLOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.my_test_attr = None

    def execute(self, context):
        self.my_test_attr = "123"
        raise ValueError("Intentional failure for OpenLineage on_failure testing")

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"custom_facet": {"random_facet_key": self.my_test_attr}},
            inputs=[InputDataset(namespace="s3://failure-test", name="before_fail.csv")],
        )

    def get_openlineage_facets_on_failure(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"custom_facet": {"random_facet_key": self.my_test_attr}},
        )


DAG_ID = "openlineage_custom_operator_failure_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    description="custom_description",
    tags=["first", "second"],
) as dag:
    failing_task = FailingOLOperator(task_id="failing_task", owner="some_owner1", doc_rst="RST doc")

    empty_task = EmptyOperator(task_id="empty_success", trigger_rule=TriggerRule.ONE_FAILED)

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
    )

    failing_task >> empty_task >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
