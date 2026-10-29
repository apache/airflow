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

from unittest.mock import MagicMock, patch

from airflow.providers.apache.ozone.hooks.ozone import OzoneFsHook
from airflow.providers.apache.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.apache.ozone.utils.errors import OzoneCliError


class TestOzoneKeySensor:
    def test_template_fields_cover_runtime_params(self):
        assert OzoneKeySensor.template_fields == ("path", "ozone_conn_id")

    @patch("airflow.providers.apache.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_key_exists(self, mock_ozone_hook: MagicMock):
        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.key_exists = MagicMock(return_value=True)
        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1", timeout=42)
        result = sensor.poke(context={})
        assert result is True
        mock_ozone_hook.assert_called_once()
        assert mock_ozone_hook.call_args.kwargs["ozone_conn_id"] == OzoneFsHook.default_conn_name
        mock_hook_instance.key_exists.assert_called_once_with("ofs://vol1/bucket1/key1", timeout=42)
        assert mock_hook_instance.key_exists.call_args.args[0] == "ofs://vol1/bucket1/key1"
        assert sensor.cli_timeout == 42
        assert sensor.timeout != 42

    @patch("airflow.providers.apache.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_retryable_error_returns_false(self, mock_ozone_hook: MagicMock):
        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.key_exists.side_effect = OzoneCliError("Connection reset", retryable=True)
        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")
        result = sensor.poke(context={})
        assert result is False
