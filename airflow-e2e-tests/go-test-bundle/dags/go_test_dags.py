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
Stub Dags of the Go test bundle (``handlers_a`` and ``handlers_b``) that bind and run.

The Dag ids of the first group are generated when the file is parsed, from ``E2E_GO_DYNAMIC_DAG_IDS``, and
``handlers_a`` registers its handler for the same ids from the same variable. ``go_split_artifacts`` has
one task in each bundle, so a task that ran another bundle's file than its own would fail.
"""

from __future__ import annotations

import os

from airflow.sdk import dag, task

_QUEUE = "golang-test"


@task.stub(queue=_QUEUE)
def greet(): ...


@task.stub(queue=_QUEUE)
def from_a(): ...


@task.stub(queue=_QUEUE)
def from_b(): ...


def _make_greeting_dag(dag_id: str):
    @dag(dag_id=dag_id)
    def greeting():
        greet()

    return greeting()


for _dag_id in filter(None, os.environ.get("E2E_GO_DYNAMIC_DAG_IDS", "").split(",")):
    _make_greeting_dag(_dag_id)


@dag(dag_id="go_split_artifacts")
def go_split_artifacts():
    from_a() >> from_b()


go_split_artifacts()
