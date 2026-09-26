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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from airflow.models import DagModel
from airflow.models.dagwarning import DagWarning, DagWarningType, get_warning_type_value

from tests_common.test_utils.db import clear_db_dags


class TestGetWarningTypeValue:
    @pytest.mark.parametrize(
        ("warning_type", "expected"),
        [
            pytest.param(DagWarningType.NONEXISTENT_POOL, "non-existent pool", id="enum-member"),
            pytest.param("non-existent pool", "non-existent pool", id="built-in-string"),
            pytest.param("yaml:schema_violation", "yaml:schema_violation", id="importer-type"),
            pytest.param(f"yaml:{'x' * 45}", f"yaml:{'x' * 45}", id="importer-type-at-column-length"),
        ],
    )
    def test_valid_type_returns_stored_value(self, warning_type, expected):
        assert get_warning_type_value(warning_type) == expected

    @pytest.mark.parametrize(
        "warning_type",
        [
            pytest.param(1, id="non-string"),
            pytest.param("yaml schema violation", id="no-namespace"),
            pytest.param("non existent pool", id="mistyped-built-in-type"),
            pytest.param("YAML:Schema_Violation", id="uppercase"),
            pytest.param(":schema_violation", id="empty-namespace"),
            pytest.param("yaml:", id="empty-type"),
            pytest.param(f"yaml:{'x' * 46}", id="longer-than-column"),
        ],
    )
    def test_invalid_type_is_rejected(self, warning_type):
        with pytest.raises(ValueError, match="validation error"):
            get_warning_type_value(warning_type)

    def test_dag_warning_stores_importer_type(self):
        assert DagWarning("dag_1", "yaml:schema_violation", "message").warning_type == "yaml:schema_violation"


@pytest.mark.db_test
class TestDagWarning:
    def setup_method(self):
        clear_db_dags()

    def test_purge_inactive_dag_warnings(self, session, testing_dag_bundle):
        """
        Test that the purge_inactive_dag_warnings method deletes inactive dag warnings
        """
        dags = [
            DagModel(dag_id="dag_1", bundle_name="testing", is_stale=True),
            DagModel(dag_id="dag_2", bundle_name="testing", is_stale=False),
        ]
        session.add_all(dags)
        session.commit()

        dag_warnings = [
            DagWarning("dag_1", "non-existent pool", "non-existent pool"),
            DagWarning("dag_2", "non-existent pool", "non-existent pool"),
        ]
        session.add_all(dag_warnings)
        session.commit()

        DagWarning.purge_inactive_dag_warnings(session=session)

        remaining_dag_warnings = session.scalars(select(DagWarning)).all()
        assert len(remaining_dag_warnings) == 1
        assert remaining_dag_warnings[0].dag_id == "dag_2"

    @mock.patch("airflow.models.dagwarning.delete")
    def test_retry_purge_inactive_dag_warnings(self, delete_mock):
        """
        Test that the purge_inactive_dag_warnings method calls the delete method twice
        if the query throws an operationalError on the first call and works on the second attempt
        """
        self.session_mock = MagicMock()

        self.session_mock.execute.side_effect = [OperationalError(None, None, "database timeout"), None]

        DagWarning.purge_inactive_dag_warnings(session=self.session_mock)

        # Assert that the delete method was called twice
        assert delete_mock.call_count == 2
        assert self.session_mock.execute.call_count == 2
