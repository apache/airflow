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

from unittest.mock import patch

import pytest

from airflow.models.dag import DAG
<<<<<<<< HEAD:tests/providers/standard/sensors/test_date_time.py
from airflow.providers.standard.sensors.date_time import DateTimeSensor
========
from airflow.providers.core.time.sensors.date_time import DateTimeSensor
>>>>>>>> 6f86e128e4 (add core.time provider):tests/providers/core/time/sensors/test_date_time.py
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)


class TestDateTimeSensor:
    @classmethod
    def setup_class(cls):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        cls.dag = DAG("test_dag", schedule=None, default_args=args)

    @pytest.mark.parametrize(
        "task_id, target_time, expected",
        [
            (
                "valid_datetime",
                timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                "2020-07-06T13:00:00+00:00",
            ),
            (
                "valid_str",
                "20200706T210000+8",
                "20200706T210000+8",
            ),
            (
                "jinja_str_is_accepted",
                "{{ ds }}",
                "{{ ds }}",
            ),
        ],
    )
    def test_valid_input(self, task_id, target_time, expected):
        """target_time should be a string as it is a template field"""
        op = DateTimeSensor(
            task_id=task_id,
            target_time=target_time,
            dag=self.dag,
        )
        assert op.target_time == expected

    def test_invalid_input(self):
        with pytest.raises(TypeError):
            DateTimeSensor(
                task_id="test",
                target_time=timezone.utcnow().time(),
                dag=self.dag,
            )

    @pytest.mark.parametrize(
        "task_id, target_time, expected",
        [
            (
                "poke_datetime",
                timezone.datetime(2020, 1, 1, 22, 59, tzinfo=timezone.utc),
                True,
            ),
            ("poke_str_extended", "2020-01-01T23:00:00.001+00:00", False),
            ("poke_str_basic_with_tz", "20200102T065959+8", True),
        ],
    )
    @patch(
<<<<<<<< HEAD:tests/providers/standard/sensors/test_date_time.py
        "airflow.providers.standard.sensors.date_time.timezone.utcnow",
========
        "airflow.providers.core.time.sensors.date_time.timezone.utcnow",
>>>>>>>> 6f86e128e4 (add core.time provider):tests/providers/core/time/sensors/test_date_time.py
        return_value=timezone.datetime(2020, 1, 1, 23, 0, tzinfo=timezone.utc),
    )
    def test_poke(self, mock_utcnow, task_id, target_time, expected):
        op = DateTimeSensor(task_id=task_id, target_time=target_time, dag=self.dag)
        assert op.poke(None) == expected
