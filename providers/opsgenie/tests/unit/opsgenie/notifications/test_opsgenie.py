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

from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.opsgenie.hooks.opsgenie import OpsgenieAlertHook
from airflow.providers.opsgenie.notifications.opsgenie import OpsgenieNotifier, send_opsgenie_notification


class TestOpsgenieNotifier:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="opsgenie_default",
                conn_type="opsgenie",
                host="https://api.opsgenie.com/",
                password="eb243592-faa2-4ba2-a551q-1afdf565c889",
            )
        )

    _config = {
        "message": "An example alert message",
        "alias": "Life is too short for no alias",
        "description": "Every alert needs a description",
        "responders": [
            {"id": "4513b7ea-3b91-438f-b7e4-e3e54af9147c", "type": "team"},
            {"name": "NOC", "type": "team"},
            {"id": "bb4d9938-c3c2-455d-aaab-727aa701c0d8", "type": "user"},
            {"username": "trinity@opsgenie.com", "type": "user"},
            {"id": "aee8a0de-c80f-4515-a232-501c0bc9d715", "type": "escalation"},
            {"name": "Nightwatch Escalation", "type": "escalation"},
            {"id": "80564037-1984-4f38-b98e-8a1f662df552", "type": "schedule"},
            {"name": "First Responders Schedule", "type": "schedule"},
        ],
        "visible_to": [
            {"id": "4513b7ea-3b91-438f-b7e4-e3e54af9147c", "type": "team"},
            {"name": "rocket_team", "type": "team"},
            {"id": "bb4d9938-c3c2-455d-aaab-727aa701c0d8", "type": "user"},
            {"username": "trinity@opsgenie.com", "type": "user"},
        ],
        "actions": ["Restart", "AnExampleAction"],
        "tags": ["OverwriteQuietHours", "Critical"],
        "details": {"key1": "value1", "key2": "value2"},
        "entity": "An example entity",
        "source": "Airflow",
        "priority": "P1",
        "user": "Jesse",
        "note": "Write this down",
    }

    expected_payload_dict = {
        "message": _config["message"],
        "alias": _config["alias"],
        "description": _config["description"],
        "responders": _config["responders"],
        "visible_to": _config["visible_to"],
        "actions": _config["actions"],
        "tags": _config["tags"],
        "details": _config["details"],
        "entity": _config["entity"],
        "source": _config["source"],
        "priority": _config["priority"],
        "user": _config["user"],
        "note": _config["note"],
    }

    @mock.patch.object(OpsgenieAlertHook, "get_conn")
    def test_notifier(self, mock_opsgenie_alert_hook):
        notifier = send_opsgenie_notification(payload=self._config)
        notifier.notify({})
        args, _ = mock_opsgenie_alert_hook.return_value.create_alert.call_args
        assert args[0] == self.expected_payload_dict

    @mock.patch.object(OpsgenieAlertHook, "get_conn")
    def test_notifier_with_notifier_class(self, mock_opsgenie_alert_hook):
        notifier = OpsgenieNotifier(payload=self._config)
        notifier.notify({})
        args, _ = mock_opsgenie_alert_hook.return_value.create_alert.call_args
        assert args[0] == self.expected_payload_dict

    @mock.patch.object(OpsgenieAlertHook, "get_conn")
    def test_notifier_templated(self, mock_opsgenie_alert_hook, create_dag_without_db):
        dag_id = "test_notifier"

        template_fields = ("message", "alias", "description", "entity", "priority", "note")
        templated_config = {}
        for key, value in self._config.items():
            if key in template_fields:
                templated_config[key] = value + " {{dag.dag_id}}"
            else:
                templated_config[key] = value

        templated_expected_payload_dict = {}
        for key, value in self.expected_payload_dict.items():
            if key in template_fields:
                templated_expected_payload_dict[key] = value + f" {dag_id}"
            else:
                templated_expected_payload_dict[key] = value

        notifier = OpsgenieNotifier(payload=templated_config)

        notifier({"dag": create_dag_without_db(dag_id=dag_id)})
        args, _ = mock_opsgenie_alert_hook.return_value.create_alert.call_args
        assert args[0] == templated_expected_payload_dict
