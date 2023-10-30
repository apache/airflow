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

from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.chime import ChimeWebhookHook
from airflow.providers.amazon.aws.notifications.chime import ChimeNotifier, send_chime_notification
from airflow.utils import db

pytestmark = pytest.mark.db_test


class TestChimeNotifier:
    # Chime webhooks can't really have a default connection, so we need to create one for tests.
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="default-chime-webhook",
                conn_type="chime",
                host="hooks.chime.aws/incomingwebhooks/",
                password="abcd-1134-ZeDA?token=somechimetoken111",
                schema="https",
            )
        )

    @mock.patch.object(ChimeWebhookHook, "send_message")
    def test_chime_notifier(self, mock_chime_hook, dag_maker):
        with dag_maker("test_chime_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = send_chime_notification(
            chime_conn_id="default-chime-webhook", message="Chime Test Message"
        )
        notifier({"dag": dag})
        mock_chime_hook.assert_called_once_with(message="Chime Test Message")

    @mock.patch.object(ChimeWebhookHook, "send_message")
    def test_chime_notifier_with_notifier_class(self, mock_chime_hook, dag_maker):
        with dag_maker("test_chime_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = ChimeNotifier(
            chime_conn_id="default-chime-webhook", message="Test Chime Message for Class"
        )
        notifier({"dag": dag})
        mock_chime_hook.assert_called_once_with(message="Test Chime Message for Class")

    @mock.patch.object(ChimeWebhookHook, "send_message")
    def test_chime_notifier_templated(self, mock_chime_hook, dag_maker):
        with dag_maker("test_chime_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = send_chime_notification(
            chime_conn_id="default-chime-webhook", message="Test Chime Message. Dag is {{ dag.dag_id }}."
        )
        notifier({"dag": dag})
        mock_chime_hook.assert_called_once_with(message="Test Chime Message. Dag is test_chime_notifier.")
