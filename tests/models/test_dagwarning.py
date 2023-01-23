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

from unittest.mock import MagicMock

from sqlalchemy.exc import OperationalError

from airflow.models import DagModel
from airflow.models.dagwarning import DagWarning
from tests.test_utils.db import clear_db_dags


class TestDagWarning:
    def setup_method(self):
        clear_db_dags()

    def test_purge_inactive_dag_warnings(self, session):
        """
        Test that the purge_inactive_dag_warnings method deletes inactive dag warnings
        """

        dags = [DagModel(dag_id="dag_1", is_active=False), DagModel(dag_id="dag_2", is_active=True)]
        session.add_all(dags)
        session.commit()

        dag_warnings = [
            DagWarning("dag_1", "non-existent pool", "non-existent pool"),
            DagWarning("dag_2", "non-existent pool", "non-existent pool"),
        ]
        session.add_all(dag_warnings)
        session.commit()

        DagWarning.purge_inactive_dag_warnings(session)

        remaining_dag_warnings = session.query(DagWarning).all()
        assert len(remaining_dag_warnings) == 1
        assert remaining_dag_warnings[0].dag_id == "dag_2"

    def test_retry_purge_inactive_dag_warnings(self):
        """
        Test that the purge_inactive_dag_warnings method calls the delete method twice
        if the query throws an operationalError on the first call and works on the second attempt
        """
        self.session_mock = MagicMock()
        self.delete_mock = MagicMock()
        self.session_mock.query.return_value.filter.return_value.delete = self.delete_mock

        self.delete_mock.side_effect = [OperationalError(None, None, "database timeout"), None]

        DagWarning.purge_inactive_dag_warnings(self.session_mock)

        # Assert that the delete method was called twice
        assert self.delete_mock.call_count == 2
