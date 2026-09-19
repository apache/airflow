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

from airflow.models.variable import Variable
from airflow.serialization.decoders import resolve_deadline_alert_interval
from airflow.serialization.definitions.deadline import (
    SerializedDeadlineAlert,
    SerializedReferenceModels,
    SerializedVariableInterval,
)


def _alert(interval):
    return SerializedDeadlineAlert(
        reference=SerializedReferenceModels.DagRunQueuedAtDeadline(),
        interval=interval,
        callback=None,
    )


class TestResolveDeadlineAlertInterval:
    def test_forwards_session_to_variable_lookup(self, mocker):
        """Both callers resolve intervals while holding an open transaction, so the session they
        pass has to reach ``Variable.get``. Without it ``provide_session`` hands back the same
        scoped session and rolls it back under the scheduler's ``prohibit_commit`` guard."""
        mock_get = mocker.patch.object(Variable, "get", return_value="42")
        session = mocker.MagicMock()

        resolved = resolve_deadline_alert_interval(
            _alert(SerializedVariableInterval(key="test_interval")), session=session
        )

        assert resolved == datetime.timedelta(seconds=42)
        mock_get.assert_called_once_with("test_interval", session=session)

    def test_fixed_interval_is_returned_without_a_variable_lookup(self, mocker):
        mock_get = mocker.patch.object(Variable, "get")

        interval = datetime.timedelta(hours=1)

        assert resolve_deadline_alert_interval(_alert(interval), session=mocker.MagicMock()) == interval
        mock_get.assert_not_called()
