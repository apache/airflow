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

import logging
from unittest.mock import patch

import pytest

from airflow.utils.log.colored_log import CustomTTYColoredFormatter

pytestmark = pytest.mark.db_test


@patch("airflow.utils.log.timezone_aware.TimezoneAware.formatTime")
def test_format_time_uses_tz_aware(mock_fmt):
    # get a logger that uses CustomTTYColoredFormatter
    logger = logging.getLogger("test_format_time")
    h = logging.StreamHandler()
    h.setFormatter(CustomTTYColoredFormatter())
    logger.addHandler(h)

    # verify that it uses TimezoneAware.formatTime
    logger.info("hi")
    mock_fmt.assert_called()
