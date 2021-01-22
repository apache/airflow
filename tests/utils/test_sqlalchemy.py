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
#
import datetime
import unittest
from unittest import mock

import pytest
from parameterized import parameterized
from sqlalchemy.exc import StatementError

from airflow import settings
from airflow.models import DAG
from airflow.settings import Session
from airflow.utils.sqlalchemy import nowait, prohibit_commit, skip_locked
from airflow.utils.state import State
from airflow.utils.timezone import utcnow


class TestSqlAlchemyUtils(unittest.TestCase):
    def setUp(self):
        session = Session()

        # make sure NOT to run in UTC. Only postgres supports storing
        # timezone information in the datetime field
        if session.bind.dialect.name == "postgresql":
            session.execute("SET timezone='Europe/Amsterdam'")

        self.session = session

    def test_utc_transformations(self):
        """
        Test whether what we are storing is what we are retrieving
        for datetimes
        """
        dag_id = 'test_utc_transformations'
        start_date = utcnow()
        iso_date = start_date.isoformat()
        execution_date = start_date + datetime.timedelta(hours=1, days=1)

        dag = DAG(
            dag_id=dag_id,
            start_date=start_date,
        )
        dag.clear()

        run = dag.create_dagrun(
            run_id=iso_date,
            state=State.NONE,
            execution_date=execution_date,
            start_date=start_date,
            session=self.session,
        )

        assert execution_date == run.execution_date
        assert start_date == run.start_date

        assert execution_date.utcoffset().total_seconds() == 0.0
        assert start_date.utcoffset().total_seconds() == 0.0

        assert iso_date == run.run_id
        assert run.start_date.isoformat() == run.run_id

        dag.clear()

    def test_process_bind_param_naive(self):
        """
        Check if naive datetimes are prevented from saving to the db
        """
        dag_id = 'test_process_bind_param_naive'

        # naive
        start_date = datetime.datetime.now()
        dag = DAG(dag_id=dag_id, start_date=start_date)
        dag.clear()

        with pytest.raises((ValueError, StatementError)):
            dag.create_dagrun(
                run_id=start_date.isoformat,
                state=State.NONE,
                execution_date=start_date,
                start_date=start_date,
                session=self.session,
            )
        dag.clear()

    @parameterized.expand(
        [
            (
                "postgresql",
                True,
                {'skip_locked': True},
            ),
            (
                "mysql",
                False,
                {},
            ),
            (
                "mysql",
                True,
                {'skip_locked': True},
            ),
            (
                "sqlite",
                False,
                {'skip_locked': True},
            ),
        ]
    )
    def test_skip_locked(self, dialect, supports_for_update_of, expected_return_value):
        session = mock.Mock()
        session.bind.dialect.name = dialect
        session.bind.dialect.supports_for_update_of = supports_for_update_of
        assert skip_locked(session=session) == expected_return_value

    @parameterized.expand(
        [
            (
                "postgresql",
                True,
                {'nowait': True},
            ),
            (
                "mysql",
                False,
                {},
            ),
            (
                "mysql",
                True,
                {'nowait': True},
            ),
            (
                "sqlite",
                False,
                {
                    'nowait': True,
                },
            ),
        ]
    )
    def test_nowait(self, dialect, supports_for_update_of, expected_return_value):
        session = mock.Mock()
        session.bind.dialect.name = dialect
        session.bind.dialect.supports_for_update_of = supports_for_update_of
        assert nowait(session=session) == expected_return_value

    def test_prohibit_commit(self):
        with prohibit_commit(self.session) as guard:
            self.session.execute('SELECT 1')
            with pytest.raises(RuntimeError):
                self.session.commit()
            self.session.rollback()

            self.session.execute('SELECT 1')
            guard.commit()

            # Check the expected_commit is reset
            with pytest.raises(RuntimeError):
                self.session.execute('SELECT 1')
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
            self.session.execute('SELECT 1')
            with pytest.raises(RuntimeError):
                self.session.commit()
            self.session.rollback()

            other_session.execute('SELECT 1')
            other_session.commit()

    def tearDown(self):
        self.session.close()
        settings.engine.dispose()
