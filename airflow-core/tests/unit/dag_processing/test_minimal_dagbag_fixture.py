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
"""Tests demonstrating and validating the shared ``minimal_dagbag`` fixture.

The ``minimal_dagbag`` fixture (defined in ``airflow-core/tests/conftest.py``)
provides a lightweight DagBag with a single no-op DAG. Any test that needs a
populated DagBag without complex setup can request this fixture instead of
manually creating temporary DAG files.
"""
from __future__ import annotations

import pytest


pytestmark = pytest.mark.db_test


class TestMinimalDagBagFixture:
    """Validate the shared minimal_dagbag fixture behaves as expected."""

    def test_dagbag_contains_one_dag(self, minimal_dagbag):
        assert minimal_dagbag.size() == 1

    def test_dagbag_has_minimal_test_dag(self, minimal_dagbag):
        assert "minimal_test_dag" in minimal_dagbag.dags

    def test_dag_has_expected_task(self, minimal_dagbag):
        dag = minimal_dagbag.dags["minimal_test_dag"]
        task_ids = [t.task_id for t in dag.tasks]
        assert "noop" in task_ids

    def test_dagbag_has_no_import_errors(self, minimal_dagbag):
        assert len(minimal_dagbag.import_errors) == 0

    def test_dag_folder_is_set(self, minimal_dagbag):
        assert minimal_dagbag.dag_folder is not None

    def test_get_dag_returns_dag(self, minimal_dagbag):
        dag = minimal_dagbag.dags.get("minimal_test_dag")
        assert dag is not None
        assert dag.dag_id == "minimal_test_dag"

    def test_get_non_existing_dag_returns_none(self, minimal_dagbag):
        assert minimal_dagbag.dags.get("does_not_exist") is None
