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

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator

from tests_common.test_utils.config import conf_vars


def _make_dag_tagged_ti(create_runtime_ti, tags):
    with DAG("tagged_dag", tags=tags):
        task = BaseOperator(task_id="t")
    return create_runtime_ti(task=task)


def test_stats_tags_dag_tags_disabled_by_default(create_runtime_ti):
    ti = _make_dag_tagged_ti(create_runtime_ti, ["env:prod", "validation"])

    assert ti.stats_tags == {"dag_id": "tagged_dag", "task_id": "t", "run_type": "manual"}


@conf_vars({("metrics", "dag_tags_in_metrics"): "True"})
def test_stats_tags_default_to_expanded_dag_tags(create_runtime_ti):
    ti = _make_dag_tagged_ti(create_runtime_ti, ["production", "team:data"])

    assert ti.stats_tags == {
        "production": "",
        "team": "data",
        "dag_id": "tagged_dag",
        "task_id": "t",
        "run_type": "manual",
    }
