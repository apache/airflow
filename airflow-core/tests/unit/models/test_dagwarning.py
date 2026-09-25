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
from airflow.models.dagwarning import DagWarning, DagWarningType

from tests_common.test_utils.db import clear_db_dags


class TestDagWarningType:
    def test_known_value_resolves_to_member(self):
        assert DagWarningType("non-existent pool") is DagWarningType.NONEXISTENT_POOL

    def test_namespaced_importer_value_is_accepted(self):
        warning_type = DagWarningType("yaml:schema_violation")

        assert isinstance(warning_type, DagWarningType)
        assert warning_type.value == "yaml:schema_violation"
        assert warning_type == "yaml:schema_violation"

    def test_importer_value_is_not_registered_as_member(self):
        DagWarningType("yaml:schema_violation")

        assert "yaml:schema_violation" not in DagWarningType._value2member_map_
        assert len(DagWarningType) == 4

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(1, id="non-string"),
            pytest.param("yaml schema violation", id="no-namespace"),
            pytest.param("non existent pool", id="mistyped-core-type"),
            pytest.param("YAML:Schema_Violation", id="uppercase"),
            pytest.param(":schema_violation", id="empty-namespace"),
            pytest.param("yaml:", id="empty-type"),
            pytest.param(f"yaml:{'x' * 46}", id="longer-than-column"),
        ],
    )
    def test_invalid_value_is_rejected(self, value):
        with pytest.raises(ValueError, match="is not a valid DagWarningType"):
            DagWarningType(value)

    def test_value_at_column_length_is_accepted(self):
        value = f"yaml:{'x' * 45}"

        assert DagWarningType(value).value == value

    @pytest.mark.parametrize(
        ("warning_type", "expected"),
        [
            pytest.param(DagWarningType.NONEXISTENT_POOL, "non-existent pool", id="enum-member"),
            pytest.param("non-existent pool", "non-existent pool", id="known-string"),
            pytest.param("yaml:schema_violation", "yaml:schema_violation", id="importer-defined-string"),
        ],
    )
    def test_dag_warning_stores_type_value(self, warning_type, expected):
        assert DagWarning("dag_1", warning_type, "message").warning_type == expected


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
