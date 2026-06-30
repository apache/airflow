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

import sys
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(AIRFLOW_ROOT / "scripts" / "ci"))
import fetch_theme_assets  # noqa: E402


@mock.patch("fetch_theme_assets.time.sleep")
@mock.patch("fetch_theme_assets.urlopen")
def test_retries_on_transient_failure(mock_urlopen, mock_sleep):
    mock_urlopen.side_effect = [URLError("timeout"), URLError("reset"), URLError("refused")]
    with pytest.raises(SystemExit, match="1"):
        fetch_theme_assets.fetch_and_extract("0.0.1")
    assert mock_urlopen.call_count == 3
    assert mock_sleep.call_count == 2
