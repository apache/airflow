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

from airflow.dag_processing.health import check_processor_health, write_processor_health

from tests_common.test_utils.config import conf_vars


@pytest.mark.parametrize(
    ("loop_age", "api_age", "readiness", "healthy"),
    [
        (0, 0, False, True),
        (31, 0, False, False),
        (0, 31, False, True),
        (0, 31, True, False),
        (0, 0, True, True),
        (-1, 0, False, False),
    ],
)
@mock.patch("airflow.dag_processing.health.monotonic", autospec=True)
def test_local_health_requires_a_fresh_loop(clock, tmp_path, loop_age, api_age, readiness, healthy):
    with conf_vars(
        {
            ("dag_processor", "health_check_file"): str(tmp_path / "health.json"),
            ("dag_processor", "health_check_threshold"): "30",
        }
    ):
        clock.return_value = 100 - loop_age
        write_processor_health(100 - api_age)
        clock.return_value = 100
        if healthy:
            check_processor_health(readiness=readiness)
        else:
            with pytest.raises(SystemExit):
                check_processor_health(readiness=readiness)


@pytest.mark.parametrize("contents", [None, "broken", "{}"])
def test_missing_or_invalid_health_file_fails_closed(tmp_path, contents):
    path = tmp_path / "health.json"
    if contents is not None:
        path.write_text(contents)
    with conf_vars({("dag_processor", "health_check_file"): str(path)}), pytest.raises(SystemExit):
        check_processor_health()
