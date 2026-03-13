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
from __future__ import annotations

import datetime
import time

from airflow.models.dag import DAG
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.openlineage.extractors import OperatorLineage


class OpenLineageExecutionOperator(BaseOperator):
    def __init__(self, *, stall_amount=0, fail=False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.stall_amount = stall_amount
        self.fail = fail

    def execute(self, context):
        self.log.error("STALL AMOUNT %s", self.stall_amount)
        time.sleep(1)
        if self.fail:
            raise Exception("Failed")

    def get_openlineage_facets_on_start(self):
        return OperatorLineage(inputs=[Dataset(namespace="test", name="on-start")])

    def get_openlineage_facets_on_complete(self, task_instance):
        self.log.error("STALL AMOUNT %s", self.stall_amount)
        time.sleep(self.stall_amount)
        return OperatorLineage(inputs=[Dataset(namespace="test", name="on-complete")])

    def get_openlineage_facets_on_failure(self, task_instance):
        self.log.error("STALL AMOUNT %s", self.stall_amount)
        time.sleep(self.stall_amount)
        return OperatorLineage(inputs=[Dataset(namespace="test", name="on-failure")])


with DAG(
    dag_id="test_openlineage_execution",
    default_args={"owner": "airflow", "retries": 3, "start_date": datetime.datetime(2022, 1, 1)},
    schedule="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
):
    no_stall = OpenLineageExecutionOperator(task_id="execute_no_stall")

    short_stall = OpenLineageExecutionOperator(task_id="execute_short_stall", stall_amount=5)

    mid_stall = OpenLineageExecutionOperator(task_id="execute_mid_stall", stall_amount=15)

    long_stall = OpenLineageExecutionOperator(task_id="execute_long_stall", stall_amount=30)

    fail = OpenLineageExecutionOperator(task_id="execute_fail", fail=True)
