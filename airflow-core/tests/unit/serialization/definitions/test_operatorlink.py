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

import pytest

from airflow.models.xcom import XComModel
from airflow.sdk.bases.operatorlink import attempt_link_xcom_key
from airflow.serialization.definitions.operatorlink import XComOperatorLink

pytestmark = pytest.mark.db_test

XCOM_KEY = "_link_MyLink"


@pytest.fixture
def link():
    return XComOperatorLink(name="My Link", xcom_key=XCOM_KEY)


def _set(session, ti, key, value):
    XComModel.set(
        key=key,
        value=value,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        session=session,
    )


class TestXComOperatorLinkPerAttempt:
    def test_returns_the_requested_attempts_link(self, session, create_task_instance, link):
        ti = create_task_instance(task_id="test_link_per_attempt")
        _set(session, ti, attempt_link_xcom_key(XCOM_KEY, 1), "https://logs/attempt-1")
        _set(session, ti, attempt_link_xcom_key(XCOM_KEY, 2), "https://logs/attempt-2")
        _set(session, ti, XCOM_KEY, "https://logs/attempt-2")
        session.commit()

        assert link.get_link(ti.task, ti_key=ti.key._replace(try_number=1)) == "https://logs/attempt-1"
        assert link.get_link(ti.task, ti_key=ti.key._replace(try_number=2)) == "https://logs/attempt-2"

    def test_falls_back_to_the_bare_key(self, session, create_task_instance, link):
        """Links written before per-attempt rows existed only have the bare key."""
        ti = create_task_instance(task_id="test_link_fallback")
        _set(session, ti, XCOM_KEY, "https://logs/only-one")
        session.commit()

        assert link.get_link(ti.task, ti_key=ti.key._replace(try_number=1)) == "https://logs/only-one"

    def test_returns_empty_when_nothing_stored(self, session, create_task_instance, link):
        ti = create_task_instance(task_id="test_link_missing")
        session.commit()

        assert link.get_link(ti.task, ti_key=ti.key) == ""
