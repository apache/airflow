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

import pytest

from airflow.providers.influxdb.utils import _convert_dataframe_to_records


def test_convert_dataframe_to_records_serializes_rows_and_timestamps():
    pd = pytest.importorskip("pandas")

    dataframe = pd.DataFrame(
        {
            "col1": [1, 2],
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-02T03:04:05Z"], utc=True),
        }
    )

    assert _convert_dataframe_to_records(dataframe) == [
        {"col1": 1, "timestamp": "2024-01-01T00:00:00.000Z"},
        {"col1": 2, "timestamp": "2024-01-02T03:04:05.000Z"},
    ]
