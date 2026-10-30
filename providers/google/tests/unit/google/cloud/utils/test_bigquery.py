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

from airflow.providers.google.cloud.utils.bigquery import bq_cast, convert_job_id


class TestBqCast:
    @pytest.mark.parametrize(
        ("value", "bq_type", "expected"),
        [
            (None, "INTEGER", None),
            ("123", "INTEGER", 123),
            ("123.45", "FLOAT", 123.45),
            ("123.45", "TIMESTAMP", 123.45),
            ("true", "BOOLEAN", True),
            ("false", "BOOLEAN", False),
            ("raw_string", "UNKNOWN_TYPE", "raw_string"),
        ],
    )
    def test_bq_cast(self, value, bq_type, expected):
        result = bq_cast(value, bq_type)
        assert result == expected
        assert type(result) is type(expected)

    def test_bq_cast_boolean_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="invalid must have value 'true' or 'false'"):
            bq_cast("invalid", "BOOLEAN")


class TestConvertJobId:
    @pytest.mark.parametrize(
        ("job_id", "location", "expected"),
        [
            ("job123", "EU", "proj:EU:job123"),
            ("job123", None, "proj:US:job123"),
            (["job1", "job2"], "EU", ["proj:EU:job1", "proj:EU:job2"]),
        ],
    )
    def test_convert_job_id(self, job_id, location, expected):
        assert convert_job_id(job_id=job_id, project_id="proj", location=location) == expected
