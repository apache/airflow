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

import pytest

from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCom
from airflow.providers.yandex.links.yq import YQLink

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.mock_operators import MockOperator

yandexcloud = pytest.importorskip("yandexcloud")


def test_persist():
    mock_context = mock.MagicMock()

    YQLink.persist(
        context=mock_context,
        task_instance=MockOperator(task_id="test_task_id"),
        web_link="g.com",
    )

    ti = mock_context["ti"]
    if AIRFLOW_V_3_0_PLUS:
        ti.xcom_push.assert_called_once_with(
            key="web_link",
            value="g.com",
        )
    else:
        ti.xcom_push.assert_called_once_with(
            key="web_link", value="g.com", execution_date=None
        )


def test_default_link():
    with mock.patch.object(XCom, "get_value") as m:
        m.return_value = None
        link = YQLink()

        op = MockOperator(task_id="test_task_id")
        ti = TaskInstance(task=op, run_id="run_id1")
        assert link.get_link(op, ti_key=ti.key) == "https://yq.cloud.yandex.ru"


def test_link():
    with mock.patch.object(XCom, "get_value") as m:
        m.return_value = "https://g.com"
        link = YQLink()

        op = MockOperator(task_id="test_task_id")
        ti = TaskInstance(task=op, run_id="run_id1")
        assert link.get_link(op, ti_key=ti.key) == "https://g.com"
