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
import pickle
from copy import deepcopy
from unittest import mock

import pytest
from kubernetes.client import models as k8s
from sqlalchemy import text
from sqlalchemy.exc import StatementError

from airflow import settings
from airflow.sdk import DAG
from airflow.sdk.timezone import utcnow
from airflow.serialization.enums import DagAttributeTypes, Encoding
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.settings import Session
from airflow.utils.sqlalchemy import (
    ExecutorConfigType,
    ensure_pod_is_valid_after_unpickling,
    get_dialect_name,
    is_sqlalchemy_v1,
    prohibit_commit,
    with_row_locks,
)
from airflow.utils.state import State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import sync_dag_to_db

pytestmark = pytest.mark.db_test


TEST_POD = k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))


class TestGetDialectName:
    def test_returns_dialect_name_when_present(self, mocker):
        mock_session = mocker.Mock()
        mock_bind = mocker.Mock()
        mock_bind.dialect.name = "postgresql"
        mock_session.get_bind.return_value = mock_bind

        assert get_dialect_name(mock_session) == "postgresql"

    def test_raises_when_no_bind(self, mocker):
        mock_session = mocker.Mock()
        mock_session.get_bind.return_value = None

        with pytest.raises(ValueError, match="No bind/engine is associated"):
            get_dialect_name(mock_session)

    def test_returns_none_when_dialect_has_no_name(self, mocker):
        mock_session = mocker.Mock()
        mock_bind = mocker.Mock()
        # simulate dialect object without `name` attribute
        mock_bind.dialect = mock.Mock()
        delattr(mock_bind.dialect, "name") if hasattr(mock_bind.dialect, "name") else None
        mock_session.get_bind.return_value = mock_bind

        assert get_dialect_name(mock_session) is None


class TestSqlAlchemyUtils:
    def setup_method(self):
        session = Session()

        # make sure NOT to run in UTC. Only postgres supports storing
        # timezone information in the datetime field
        if get_dialect_name(session) == "postgresql":
            session.execute(text("SET timezone='Europe/Amsterdam'"))

        self.session = session

    def test_utc_transformations(self, testing_dag_bundle):
        """
        Test whether what we are storing is what we are retrieving
        for datetimes
        """
        dag_id = "test_utc_transformations"
        start_date = utcnow()
        iso_date = start_date.isoformat()
        logical_date = start_date + datetime.timedelta(hours=1, days=1)

        dag = sync_dag_to_db(DAG(dag_id=dag_id, schedule=datetime.timedelta(days=1), start_date=start_date))
        dag.clear()
        run = dag.create_dagrun(
            run_id=iso_date,
            run_type=DagRunType.MANUAL,
            state=State.NONE,
            logical_date=logical_date,
            start_date=start_date,
            session=self.session,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        assert logical_date == run.logical_date
        assert start_date == run.start_date

        assert logical_date.utcoffset().total_seconds() == 0.0
        assert start_date.utcoffset().total_seconds() == 0.0

        assert iso_date == run.run_id
        assert run.start_date.isoformat() == run.run_id

        dag.clear()

    def test_process_bind_param_naive(self):
        """
        Check if naive datetimes are prevented from saving to the db
        """
        dag_id = "test_process_bind_param_naive"

        # naive
        start_date = datetime.datetime.now()
        dag = sync_dag_to_db(DAG(dag_id=dag_id, start_date=start_date, schedule=datetime.timedelta(days=1)))
        dag.clear()

        with pytest.raises((ValueError, StatementError)):
            dag.create_dagrun(
                run_id=start_date.isoformat,
                run_type=DagRunType.MANUAL,
                state=State.NONE,
                logical_date=start_date,
                start_date=start_date,
                session=self.session,
                data_interval=dag.timetable.infer_manual_data_interval(run_after=start_date),
                run_after=start_date,
                triggered_by=DagRunTriggeredByType.TEST,
            )
        dag.clear()

    @pytest.mark.parametrize(
        ("dialect", "supports_for_update_of", "use_row_level_lock_conf", "expected_use_row_level_lock"),
        [
            ("postgresql", True, True, True),
            ("postgresql", True, False, False),
            ("mysql", False, True, False),
            ("mysql", False, False, False),
            ("mysql", True, True, True),
            ("mysql", True, False, False),
            ("sqlite", False, True, True),
        ],
    )
    def test_with_row_locks(
        self, dialect, supports_for_update_of, use_row_level_lock_conf, expected_use_row_level_lock
    ):
        query = mock.Mock()
        session = mock.Mock()
        session.bind.dialect.name = dialect
        session.bind.dialect.supports_for_update_of = supports_for_update_of
        session.get_bind.return_value = session.bind
        with mock.patch("airflow.utils.sqlalchemy.USE_ROW_LEVEL_LOCKING", use_row_level_lock_conf):
            returned_value = with_row_locks(query=query, session=session, nowait=True)

        if expected_use_row_level_lock:
            query.with_for_update.assert_called_once_with(nowait=True, key_share=True)
        else:
            assert returned_value == query
            query.with_for_update.assert_not_called()

    def test_prohibit_commit(self):
        with prohibit_commit(self.session) as guard:
            self.session.execute(text("SELECT 1"))
            with pytest.raises(RuntimeError):
                self.session.commit()
            self.session.rollback()

            self.session.execute(text("SELECT 1"))
            guard.commit()

            # Check the expected_commit is reset
            self.session.execute(text("SELECT 1"))
            with pytest.raises(RuntimeError, match="UNEXPECTED COMMIT"):
                self.session.commit()

    def test_prohibit_commit_specific_session_only(self):
        """
        Test that "prohibit_commit" applies only to the given session object,
        not any other session objects that may be used
        """
        # We _want_ another session. By default this would be the _same_
        # session we already had
        other_session = Session.session_factory()
        assert other_session is not self.session

        with prohibit_commit(self.session):
            self.session.execute(text("SELECT 1"))
            with pytest.raises(RuntimeError):
                self.session.commit()
            self.session.rollback()

            other_session.execute(text("SELECT 1"))
            other_session.commit()

    def teardown_method(self):
        self.session.close()
        settings.engine.dispose()


class TestExecutorConfigType:
    @pytest.mark.parametrize(
        ("input", "expected"),
        [
            ("anything", "anything"),
            (
                {"pod_override": TEST_POD},
                {
                    "pod_override": {
                        "__var": {"spec": {"containers": [{"name": "base"}]}},
                        "__type": DagAttributeTypes.POD,
                    }
                },
            ),
        ],
    )
    def test_bind_processor(self, input, expected, mocker):
        """
        The returned bind processor should pickle the object as is, unless it is a dictionary with
        a pod_override node, in which case it should run it through BaseSerialization.
        """
        config_type = ExecutorConfigType()
        mock_dialect = mocker.MagicMock()
        mock_dialect.dbapi = None
        process = config_type.bind_processor(mock_dialect)
        assert pickle.loads(process(input)) == expected
        assert pickle.loads(process(input)) == expected, "should not mutate variable"

    @pytest.mark.parametrize(
        "input",
        [
            pytest.param(
                pickle.dumps("anything"),
                id="anything",
            ),
            pytest.param(
                pickle.dumps({"pod_override": BaseSerialization.serialize(TEST_POD)}),
                id="serialized_pod",
            ),
            pytest.param(
                pickle.dumps({"pod_override": TEST_POD}),
                id="old_pickled_raw_pod",
            ),
            pytest.param(
                pickle.dumps({"pod_override": {"name": "hi"}}),
                id="arbitrary_dict",
            ),
        ],
    )
    def test_result_processor(self, input, mocker):
        """
        The returned bind processor should pickle the object as is, unless it is a dictionary with
        a pod_override node whose value was serialized with BaseSerialization.
        """
        config_type = ExecutorConfigType()
        mock_dialect = mocker.MagicMock()
        mock_dialect.dbapi = None
        process = config_type.result_processor(mock_dialect, None)
        result = process(input)
        expected = pickle.loads(input)
        pod_override = isinstance(expected, dict) and expected.get("pod_override")
        if pod_override and isinstance(pod_override, dict) and pod_override.get(Encoding.TYPE):
            # We should only deserialize a pod_override with BaseSerialization if
            # it was serialized with BaseSerialization (which is the behavior added in #24356
            expected["pod_override"] = BaseSerialization.deserialize(expected["pod_override"])
        assert result == expected

    def test_compare_values(self):
        """
        When comparison raises AttributeError, return False.
        This can happen when executor config contains kubernetes objects pickled
        under older kubernetes library version.
        """

        class MockAttrError:  # noqa: PLW1641
            def __eq__(self, other):
                raise AttributeError("hello")

        a = MockAttrError()
        with pytest.raises(AttributeError):
            # just verify for ourselves that comparing directly will throw AttributeError
            assert a == a

        instance = ExecutorConfigType()
        assert instance.compare_values(a, a) is False
        assert instance.compare_values("a", "a") is True

    def test_result_processor_bad_pickled_obj(self, mocker):
        """
        If unpickled obj is missing attrs that curr lib expects
        """
        test_container = k8s.V1Container(name="base")
        test_pod = k8s.V1Pod(spec=k8s.V1PodSpec(containers=[test_container]))
        copy_of_test_pod = deepcopy(test_pod)
        # curr api expects attr `tty`
        assert "tty" in test_container.openapi_types
        # it lives in protected attr _tty
        assert hasattr(test_container, "_tty")
        # so, let's remove it before pickling, to simulate what happens in real life
        del test_container._tty
        # now let's prove that this blows up when calling to_dict
        with pytest.raises(AttributeError):
            test_pod.to_dict()
        # no such problem with the copy
        assert copy_of_test_pod.to_dict()
        # so we need to roundtrip it through json
        fixed_pod = ensure_pod_is_valid_after_unpickling(test_pod)
        # and, since the missing attr was None anyway, we actually have the same pod
        assert fixed_pod.to_dict() == copy_of_test_pod.to_dict()

        # now, let's verify that result processor makes this all work
        # first, check that bad pod is still bad
        with pytest.raises(AttributeError):
            test_pod.to_dict()
        # define what will be retrieved from db
        input = pickle.dumps({"pod_override": TEST_POD})

        # get the result processor method
        config_type = ExecutorConfigType()
        mock_dialect = mocker.MagicMock()
        mock_dialect.dbapi = None
        process = config_type.result_processor(mock_dialect, None)

        # apply the result processor
        result = process(input)

        # show that the pickled (bad) pod is now a good pod, and same as the copy made
        # before making it bad
        assert result["pod_override"].to_dict() == copy_of_test_pod.to_dict()


@pytest.mark.parametrize(
    ("mock_version", "expected_result"),
    [
        ("1.0.0", True),  # Test 1: v1 identified as v1
        ("2.3.4", False),  # Test 2: v2 not identified as v1
    ],
)
def test_is_sqlalchemy_v1(mock_version, expected_result, mocker):
    mock_metadata = mocker.patch("airflow.utils.sqlalchemy.metadata")
    mock_metadata.version.return_value = mock_version
    assert is_sqlalchemy_v1() == expected_result
