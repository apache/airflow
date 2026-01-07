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

from unittest.mock import patch

import pendulum
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

import airflow.example_dags as example_dags_module
from airflow.dag_processing.dagbag import DagBag
from airflow.models.dag_version import DagVersion
from airflow.models.dagcode import DagCode
from airflow.sdk import task as task_decorator
from airflow.serialization.definitions.dag import SerializedDAG

# To move it to a shared module.
from airflow.utils.file import open_maybe_zipped
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_code, clear_db_dags

pytestmark = pytest.mark.db_test


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    # TODO: AIP-66 dedup with tests/models/test_serdag
    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    with create_session() as session:
        if (
            session.scalar(
                select(func.count()).select_from(DagBundleModel).where(DagBundleModel.name == "testing")
            )
            == 0
        ):
            testing = DagBundleModel(name="testing")
            session.add(testing)

    dagbag = DagBag(module.__path__[0])
    SerializedDAG.bulk_write_to_db("testing", None, dagbag.dags.values())
    return dagbag.dags


class TestDagCode:
    """Unit tests for DagCode."""

    def setup_method(self):
        clear_db_dags()
        clear_db_dag_code()

    def teardown_method(self):
        clear_db_dags()
        clear_db_dag_code()

    def _write_two_example_dags(self, session):
        example_dags = make_example_dags(example_dags_module)
        bash_dag = example_dags["example_bash_operator"]
        sync_dag_to_db(bash_dag, session=session)
        dag_version = DagVersion.get_latest_version("example_bash_operator")
        x = DagCode(dag_version, bash_dag.fileloc)
        session.add(x)
        session.commit()
        xcom_dag = example_dags["example_xcom"]
        sync_dag_to_db(xcom_dag, session=session)
        dag_version = DagVersion.get_latest_version("example_xcom")
        x = DagCode(dag_version, xcom_dag.fileloc)
        session.add(x)
        session.commit()
        return [bash_dag, xcom_dag]

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        with create_session() as session:
            for dag in example_dags.values():
                sync_dag_to_db(dag, session=session)
        return example_dags

    def test_write_to_db(self, testing_dag_bundle):
        """Dg code can be written into database."""
        example_dags = self._write_example_dags()

        self._compare_example_dags(example_dags)

    @patch.object(DagCode, "dag_source_hash")
    def test_detecting_duplicate_key(self, mock_hash, session):
        """Dag code detects duplicate key."""
        mock_hash.return_value = 0

        with pytest.raises(IntegrityError):
            self._write_two_example_dags(session)

    def _compare_example_dags(self, example_dags):
        with create_session() as session:
            for dag in example_dags.values():
                assert DagCode.has_dag(dag.dag_id)
                result = session.execute(
                    select(DagCode.fileloc, DagCode.dag_id, DagCode.source_code)
                    .where(DagCode.dag_id == dag.dag_id)
                    .order_by(DagCode.last_updated.desc())
                    .limit(1)
                ).one()

                assert result.fileloc == dag.fileloc
                with open_maybe_zipped(dag.fileloc, "r") as source:
                    source_code = source.read()
                assert result.source_code == source_code

    def test_code_can_be_read_when_no_access_to_file(self, testing_dag_bundle):
        """
        Test that code can be retrieved from DB when you do not have access to Code file.
        Source Code should at least exist in one of DB or File.
        """
        example_dag = make_example_dags(example_dags_module).get("example_bash_operator")
        sync_dag_to_db(example_dag)

        # Mock that there is no access to the Dag File
        with patch("airflow.models.dagcode.open_maybe_zipped") as mock_open:
            mock_open.side_effect = FileNotFoundError
            dag_code = DagCode.code(example_dag.dag_id)

            for test_string in ["example_bash_operator", "also_run_this", "run_this_last"]:
                assert test_string in dag_code

    def test_db_code_created_on_serdag_change(self, session, testing_dag_bundle):
        """Test new DagCode is created in DB when ser dag is changed"""
        example_dag = make_example_dags(example_dags_module).get("example_bash_operator")
        sync_dag_to_db(example_dag, session=session).create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )
        result = session.scalars(
            select(DagCode)
            .where(DagCode.fileloc == example_dag.fileloc)
            .order_by(DagCode.last_updated.desc())
            .limit(1)
        ).one()

        assert result.source_code is not None

        example_dag.doc_md = "new doc"
        with patch("airflow.models.dagcode.DagCode.get_code_from_file") as mock_code:
            mock_code.return_value = "# dummy code"
            sync_dag_to_db(example_dag, session=session)

        new_result = session.scalars(
            select(DagCode)
            .where(DagCode.fileloc == example_dag.fileloc)
            .order_by(DagCode.last_updated.desc())
            .limit(1)
        ).one()

        assert new_result.source_code != result.source_code
        assert new_result.last_updated > result.last_updated
        assert session.scalar(select(func.count()).select_from(DagCode)) == 2

    def test_has_dag(self, dag_maker):
        """Test has_dag method."""
        with dag_maker("test_has_dag") as dag:
            pass
        sync_dag_to_db(dag)

        with dag_maker() as dag2:
            pass
        sync_dag_to_db(dag2)

        assert DagCode.has_dag(dag.dag_id)

    def test_update_source_code(self, dag_maker, session):
        """Test that dag code can be updated."""
        with dag_maker("dag1") as dag:

            @task_decorator
            def mytask():
                print("task4")

            mytask()
        sync_dag_to_db(dag)
        dag_code = DagCode.get_latest_dagcode(dag.dag_id)
        dag_code.source_code_hash = 2
        session.add(dag_code)
        session.commit()
        dagcode2 = DagCode.get_latest_dagcode(dag.dag_id)
        assert dagcode2.source_code_hash == 2
        DagCode.update_source_code(dag.dag_id, dag.fileloc)
        dag_code3 = DagCode.get_latest_dagcode(dag.dag_id)
        assert dag_code3.source_code_hash != 2
