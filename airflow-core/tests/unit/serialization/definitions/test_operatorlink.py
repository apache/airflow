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

import json

import pytest

from airflow._shared.state import TaskScope, attempt_link_state_key
from airflow.models.xcom import XComModel
from airflow.serialization.definitions.operatorlink import XComOperatorLink
from airflow.state import get_state_backend

pytestmark = pytest.mark.db_test

XCOM_KEY = "_link_MyLink"


@pytest.fixture
def link():
    return XComOperatorLink(name="My Link", xcom_key=XCOM_KEY)


@pytest.fixture
def store_link(session):
    def write(ti, try_number, value):
        get_state_backend().set(
            TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index),
            attempt_link_state_key(XCOM_KEY, try_number),
            json.dumps(value),
            session=session,
        )

    return write


@pytest.fixture
def xcom_link(session):
    def write(ti, value):
        XComModel.set(
            key=XCOM_KEY,
            value=value,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            session=session,
        )

    return write


class TestXComOperatorLinkPerAttempt:
    def test_returns_the_requested_attempts_link(
        self, session, create_task_instance, link, store_link, xcom_link
    ):
        ti = create_task_instance(task_id="link_per_attempt")
        store_link(ti, 1, "https://logs/attempt-1")
        store_link(ti, 2, "https://logs/attempt-2")
        xcom_link(ti, "https://logs/attempt-2")
        session.commit()

        assert link.get_link(None, ti_key=ti.key._replace(try_number=1)) == "https://logs/attempt-1"
        assert link.get_link(None, ti_key=ti.key._replace(try_number=2)) == "https://logs/attempt-2"

    def test_falls_back_to_xcom(self, session, create_task_instance, link, xcom_link):
        """Links written before per-attempt rows existed only have the XCom row."""
        ti = create_task_instance(task_id="link_fallback")
        xcom_link(ti, "https://logs/only-one")
        session.commit()

        assert link.get_link(None, ti_key=ti.key._replace(try_number=1)) == "https://logs/only-one"

    def test_returns_empty_when_nothing_stored(self, session, create_task_instance, link):
        ti = create_task_instance(task_id="link_missing")
        session.commit()

        assert link.get_link(None, ti_key=ti.key) == ""

    def test_state_store_wins_over_xcom(self, session, create_task_instance, link, store_link, xcom_link):
        """The XCom row is the latest attempt, so it must not answer for an earlier one."""
        ti = create_task_instance(task_id="link_precedence")
        store_link(ti, 1, "https://logs/attempt-1")
        xcom_link(ti, "https://logs/attempt-2")
        session.commit()

        assert link.get_link(None, ti_key=ti.key._replace(try_number=1)) == "https://logs/attempt-1"
