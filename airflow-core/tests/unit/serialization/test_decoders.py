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

from airflow.models.deadline_alert import DeadlineAlert as DeadlineAlertModel
from airflow.models.variable import Variable
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineReference
from airflow.sdk.serde import serialize
from airflow.serialization.decoders import decode_deadline_alert_model, resolve_deadline_alert_interval
from airflow.serialization.definitions.deadline import (
    SerializedDeadlineAlert,
    SerializedReferenceModels,
    SerializedVariableInterval,
)


async def empty_callback_for_deadline():
    pass


def _alert(interval):
    return SerializedDeadlineAlert(
        reference=SerializedReferenceModels.DagRunQueuedAtDeadline(),
        interval=interval,
        callback=None,
    )


class TestResolveDeadlineAlertInterval:
    def test_forwards_session_to_variable_lookup(self, mocker):
        """Both callers resolve intervals while holding an open transaction, so the session they
        pass has to reach ``Variable.get``. See ``resolve_deadline_alert_interval()``."""
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


class TestDecodeDeadlineAlertModel:
    def test_row_name_is_carried_into_the_serialized_alert(self):
        """The ORM row has a ``name`` column, so a general "decode this row" helper has to read it.

        Neither caller looks at ``.name`` today, which is exactly why dropping it here would go
        unnoticed until a third caller trusts the helper's name.
        """
        row = DeadlineAlertModel(
            name="my_deadline",
            reference=DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference(),
            interval=serialize(datetime.timedelta(hours=1)),
            callback_def=serialize(AsyncCallback(empty_callback_for_deadline)),
        )

        assert decode_deadline_alert_model(row).name == "my_deadline"
