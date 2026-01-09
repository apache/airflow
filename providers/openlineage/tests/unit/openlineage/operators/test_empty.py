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

from unittest.mock import Mock

import pytest

from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.operators.empty import EmptyOperator


def test_execute_returns_none_and_does_not_raise():
    op = EmptyOperator(task_id="empty")
    # Calling execute should do nothing and return None
    result = op.execute(Mock())
    assert result is None


@pytest.mark.parametrize(
    ("method_name", "use_task_instance"),
    [
        ("get_openlineage_facets_on_start", False),
        ("get_openlineage_facets_on_complete", True),
        ("get_openlineage_facets_on_failure", True),
    ],
)
def test_openlineage_facets_methods_return_operator_lineage(method_name, use_task_instance):
    op = EmptyOperator(task_id="empty")
    method = getattr(op, method_name)
    # Invoke with or without a mock for task_instance
    if use_task_instance:
        facets = method(Mock())
    else:
        facets = method()
    # Should return an OperatorLineage instance
    assert isinstance(facets, OperatorLineage)
    # Each call returns a fresh instance
    second_call = method(Mock()) if use_task_instance else method()
    assert facets is not second_call
