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
import importlib
from unittest import mock

import pytest
import time_machine
from sentry_sdk import configure_scope
from sentry_sdk.transport import Transport

from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.module_loading import import_string
from airflow.utils.state import State
from tests.test_utils.config import conf_vars

EXECUTION_DATE = timezone.utcnow()
SCHEDULE_INTERVAL = datetime.timedelta(days=1)
DATA_INTERVAL = (EXECUTION_DATE, EXECUTION_DATE + SCHEDULE_INTERVAL)
DAG_ID = "test_dag"
TASK_ID = "test_task"
OPERATOR = "PythonOperator"
TRY_NUMBER = 0
STATE = State.SUCCESS
TEST_SCOPE = {
    "dag_id": DAG_ID,
    "task_id": TASK_ID,
    "data_interval_start": DATA_INTERVAL[0],
    "data_interval_end": DATA_INTERVAL[1],
    "execution_date": EXECUTION_DATE,
    "operator": OPERATOR,
    "try_number": TRY_NUMBER,
}
TASK_DATA = {
    "task_id": TASK_ID,
    "state": STATE,
    "operator": OPERATOR,
    "duration": None,
}

CRUMB_DATE = datetime.datetime(2019, 5, 15, tzinfo=datetime.timezone.utc)
CRUMB = {
    "timestamp": CRUMB_DATE,
    "type": "default",
    "category": "completed_tasks",
    "data": TASK_DATA,
    "level": "info",
}


def before_send(_):
    pass


class CustomTransport(Transport):
    pass


class TestSentryHook:
    @pytest.fixture
    def task_instance(self, dag_maker):
        # Mock the Dag
        with dag_maker(DAG_ID, schedule=SCHEDULE_INTERVAL, serialized=True):
            task = PythonOperator(task_id=TASK_ID, python_callable=int)

        dr = dag_maker.create_dagrun(data_interval=DATA_INTERVAL, execution_date=EXECUTION_DATE)
        ti = dr.task_instances[0]
        ti.state = STATE
        ti.task = task
        dag_maker.session.commit()

        yield ti

        dag_maker.session.rollback()

    @pytest.fixture
    def sentry_sdk(self):
        with mock.patch("sentry_sdk.init") as sentry_sdk:
            yield sentry_sdk

    @pytest.fixture
    def sentry(self):
        with conf_vars(
            {
                ("sentry", "sentry_on"): "True",
                ("sentry", "default_integrations"): "False",
                ("sentry", "before_send"): "tests.core.test_sentry.before_send",
            },
        ):
            from airflow import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    @pytest.fixture
    def sentry_custom_transport(self):
        with conf_vars(
            {
                ("sentry", "sentry_on"): "True",
                ("sentry", "default_integrations"): "False",
                ("sentry", "transport"): "tests.core.test_sentry.CustomTransport",
            },
        ):
            from airflow import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    @pytest.fixture
    def sentry_minimum(self):
        """
        Minimum sentry config
        """
        with conf_vars({("sentry", "sentry_on"): "True"}):
            from airflow import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_add_tagging(self, sentry, task_instance):
        """
        Test adding tags.
        """
        sentry.add_tagging(task_instance=task_instance)
        with configure_scope() as scope:
            for key, value in scope._tags.items():
                assert value == TEST_SCOPE[key]

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    @time_machine.travel(CRUMB_DATE)
    def test_add_breadcrumbs(self, sentry, task_instance):
        """
        Test adding breadcrumbs.
        """
        sentry.add_tagging(task_instance=task_instance)
        sentry.add_breadcrumbs(task_instance=task_instance)

        with configure_scope() as scope:
            test_crumb = scope._breadcrumbs.pop()
            for item in CRUMB:
                if item == "timestamp":
                    pass
                elif item == "state":
                    assert str(CRUMB[item]) == str(test_crumb[item])
                else:
                    assert CRUMB[item] == test_crumb[item]

    def test_before_send(self, sentry_sdk, sentry):
        """
        Test before send callable gets passed to the sentry SDK.
        """
        assert sentry
        called = sentry_sdk.call_args.kwargs["before_send"]
        expected = import_string("tests.core.test_sentry.before_send")
        assert called == expected

    def test_custom_transport(self, sentry_sdk, sentry_custom_transport):
        """
        Test transport gets passed to the sentry SDK
        """
        assert sentry_custom_transport
        called = sentry_sdk.call_args.kwargs["transport"]
        expected = import_string("tests.core.test_sentry.CustomTransport")
        assert called == expected

    def test_minimum_config(self, sentry_sdk, sentry_minimum):
        """
        Test before_send doesn't raise an exception when not set
        """
        assert sentry_minimum
        sentry_sdk.assert_called_once()
