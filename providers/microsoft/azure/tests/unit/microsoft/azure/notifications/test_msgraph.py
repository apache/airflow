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

from airflow.providers.microsoft.azure.notifications.msgraph import (
    MSGraphNotifier,
    send_msgraph_notification,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

TEST_DAG_ID = "test_dag"
FROM_EMAIL = "airflow@example.com"
TO_EMAIL = "user@example.com"

EXPECTED_EMAIL = {
    "from_email": FROM_EMAIL,
    "to": TO_EMAIL,
    "subject": "Airflow alert",
    "html_content": "Something happened",
    "files": None,
    "cc": None,
    "bcc": None,
    "custom_headers": None,
    "save_to_sent_items": True,
}


@mock.patch("airflow.providers.microsoft.azure.notifications.msgraph.MSGraphMailHook", autospec=True)
class TestMSGraphNotifier:
    @pytest.mark.parametrize(
        ("given", "expected_conn_id"),
        (
            pytest.param({}, "msgraph_default", id="default-connection"),
            pytest.param({"conn_id": "msgraph_api"}, "msgraph_api", id="explicit-connection"),
        ),
    )
    def test_notifier(self, mock_hook, create_dag_without_db, given, expected_conn_id):
        notifier = send_msgraph_notification(
            from_email=FROM_EMAIL,
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            **given,
        )

        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})

        mock_hook.assert_called_once_with(conn_id=expected_conn_id)
        mock_hook.return_value.send_email.assert_called_once_with(**EXPECTED_EMAIL)

    def test_notifier_templated(self, mock_hook, create_dag_without_db):
        notifier = MSGraphNotifier(
            from_email=FROM_EMAIL,
            to=TO_EMAIL,
            subject="Dag {{ dag.dag_id }} failed",
            html_content="Dag {{ dag.dag_id }} needs attention",
        )

        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})

        mock_hook.return_value.send_email.assert_called_once_with(
            **{
                **EXPECTED_EMAIL,
                "subject": f"Dag {TEST_DAG_ID} failed",
                "html_content": f"Dag {TEST_DAG_ID} needs attention",
            }
        )

    def test_notifier_falls_back_to_the_configured_from_email(self, mock_hook, create_dag_without_db):
        with conf_vars({("email", "from_email"): FROM_EMAIL}):
            notifier = MSGraphNotifier(
                to=TO_EMAIL, subject="Airflow alert", html_content="Something happened"
            )

        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})

        mock_hook.return_value.send_email.assert_called_once_with(**EXPECTED_EMAIL)

    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Async support was added to BaseNotifier in 3.1.0")
    @pytest.mark.asyncio
    async def test_async_notifier(self, mock_hook, create_dag_without_db):
        notifier = MSGraphNotifier(
            from_email=FROM_EMAIL,
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            context={"dag": create_dag_without_db(TEST_DAG_ID)},
        )

        await notifier

        mock_hook.return_value.asend_email.assert_awaited_once_with(**EXPECTED_EMAIL)
