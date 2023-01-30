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

import re

import pytest

from airflow.kubernetes.kubernetes_helper_functions import create_pod_id

pod_name_regex = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"


@pytest.mark.parametrize(
    "val, expected",
    [
        ("task-id", "task-id"),  # no problem
        ("task_id", "task-id"),  # underscores
        ("---task.id---", "task-id"),  # dots
        (".task.id", "task-id"),  # leading dot invalid
        ("**task.id", "task-id"),  # leading dot invalid
        ("-90Abc*&", "90abc"),  # invalid ends
        ("90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a"),  # weird unicode
    ],
)
def test_create_pod_id_task_only(val, expected):
    actual = create_pod_id(task_id=val)
    assert actual == expected
    assert re.match(pod_name_regex, actual)


@pytest.mark.parametrize(
    "val, expected",
    [
        ("dag-id", "dag-id"),  # no problem
        ("dag_id", "dag-id"),  # underscores
        ("---dag.id---", "dag-id"),  # dots
        (".dag.id", "dag-id"),  # leading dot invalid
        ("**dag.id", "dag-id"),  # leading dot invalid
        ("-90Abc*&", "90abc"),  # invalid ends
        ("90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a"),  # weird unicode
    ],
)
def test_create_pod_id_dag_only(val, expected):
    actual = create_pod_id(dag_id=val)
    assert actual == expected
    assert re.match(pod_name_regex, actual)


@pytest.mark.parametrize(
    "dag_id, task_id, expected",
    [
        ("dag-id", "task-id", "dag-id-task-id"),  # no problem
        ("dag_id", "task_id", "dag-id-task-id"),  # underscores
        ("dag.id", "task.id", "dag-id-task-id"),  # dots
        (".dag.id", ".---task.id", "dag-id-task-id"),  # leading dot invalid
        ("**dag.id", "**task.id", "dag-id-task-id"),  # leading dot invalid
        ("-90Abc*&", "-90Abc*&", "90abc-90abc"),  # invalid ends
        ("90AçLbˆˆç˙ßß˜˜˙c*a", "90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a-90aclb-c-ssss-c-a"),  # ugly
    ],
)
def test_create_pod_id_dag_and_task(dag_id, task_id, expected):
    actual = create_pod_id(dag_id=dag_id, task_id=task_id)
    assert actual == expected
    assert re.match(pod_name_regex, actual)


def test_create_pod_id_dag_too_long():
    actual = create_pod_id("0" * 254)
    assert actual == "0" * 253
    assert re.match(pod_name_regex, actual)
