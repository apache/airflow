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

from tests_common.test_utils.config import conf_vars

_FAKE_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "loggers": {},
}

_FALLBACK_CLASS_PATH = "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"


@pytest.mark.parametrize(
    ("json_format_value", "expected_json_output"),
    [
        ("True", True),
        ("False", False),
    ],
)
def test_json_format_passed_to_shared_configure_logging(json_format_value, expected_json_output):
    """json_output kwarg forwarded to shared configure_logging based on json_format config."""
    mock_shared_configure = mock.MagicMock()

    with (
        conf_vars({("logging", "json_format"): json_format_value}),
        mock.patch(
            "airflow.logging_config.load_logging_config",
            return_value=(_FAKE_LOGGING_CONFIG, _FALLBACK_CLASS_PATH),
        ),
        mock.patch("airflow.logging_config.validate_logging_config"),
        # Patch the shared module that configure_logging() imports from locally
        mock.patch("airflow._shared.logging.configure_logging", mock_shared_configure),
        mock.patch("airflow._shared.logging.init_log_folder", return_value="/tmp/airflow/logs"),
        mock.patch("airflow._shared.logging.translate_config_values", return_value=("", [])),
    ):
        from airflow.logging_config import configure_logging

        configure_logging()

    mock_shared_configure.assert_called_once()
    assert mock_shared_configure.call_args.kwargs.get("json_output") is expected_json_output
