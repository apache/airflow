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

import datetime
from unittest.mock import patch

import pendulum
import pytest

from airflow import macros
from airflow.models.dag import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensor, DateTimeSensorAsync

from tests_common.test_utils.version_compat import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)


class TestDateTimeSensor:
    @classmethod
    def setup_class(cls):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        cls.dag = DAG("test_dag", schedule=None, default_args=args)

    @pytest.mark.parametrize(
        ("task_id", "target_time"),
        [
            ("valid_datetime", timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc)),
            ("valid_str", "20200706T210000+8"),
            ("jinja_str_is_accepted", "{{ ds }}"),
        ],
    )
    def test_target_time_stored_verbatim(self, task_id, target_time):
        """target_time is a template field, so __init__ must store it as-is without transformation."""
        op = DateTimeSensor(
            task_id=task_id,
            target_time=target_time,
            dag=self.dag,
        )
        assert op.target_time == target_time

    def test_invalid_input_rejected_after_rendering(self):
        # __init__ no longer validates the template field; the TypeError surfaces when the
        # un-renderable value is used at poke time.
        op = DateTimeSensor(
            task_id="test",
            target_time=timezone.utcnow().time(),
            dag=self.dag,
        )
        with pytest.raises(TypeError):
            op.poke(None)

    @pytest.mark.parametrize(
        ("task_id", "target_time", "expected"),
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
        "airflow.providers.standard.sensors.date_time.timezone.utcnow",
        return_value=timezone.datetime(2020, 1, 1, 23, 0, tzinfo=timezone.utc),
    )
    def test_poke(self, mock_utcnow, task_id, target_time, expected):
        op = DateTimeSensor(task_id=task_id, target_time=target_time, dag=self.dag)
        assert op.poke(None) == expected

    @pytest.mark.parametrize(
        ("native", "target_time", "expected_type"),
        [
            (False, "2025-01-01T00:00:00+00:00", pendulum.DateTime),
            (True, "{{ data_interval_end }}", pendulum.DateTime),
            (False, pendulum.datetime(2025, 1, 1, tz="UTC"), pendulum.DateTime),
        ],
    )
    def test_moment(self, native, target_time, expected_type):
        dag = DAG(
            dag_id="moment_dag",
            start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
            schedule=None,
            render_template_as_native_obj=native,
        )

        sensor = DateTimeSensor(
            task_id="moment",
            target_time=target_time,
            dag=dag,
        )

        ctx = {
            "data_interval_end": pendulum.datetime(2025, 1, 1, tz="UTC"),
            "macros": macros,
            "dag": dag,
        }
        sensor.render_template_fields(ctx)

        assert isinstance(sensor._moment, expected_type)

    @patch(
        "airflow.providers.standard.sensors.date_time.timezone.utcnow",
        return_value=pendulum.datetime(2020, 1, 2, tz="UTC"),
    )
    def test_poke_with_natively_rendered_datetime(self, mock_utcnow):
        """poke must handle a target_time rendered to a datetime (render_template_as_native_obj=True)."""
        dag = DAG(
            dag_id="native_poke_dag",
            start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
            schedule=None,
            render_template_as_native_obj=True,
        )
        op = DateTimeSensor(task_id="native_poke", target_time="{{ data_interval_end }}", dag=dag)
        op.render_template_fields(
            {"data_interval_end": pendulum.datetime(2020, 1, 1, tz="UTC"), "macros": macros, "dag": dag}
        )
        assert isinstance(op.target_time, datetime.datetime)
        assert op.poke(None) is True

    def test_async_start_from_trigger_moment(self):
        op = DateTimeSensorAsync(
            task_id="async",
            target_time="2020-01-01T00:00:00+00:00",
            start_from_trigger=True,
            dag=self.dag,
        )
        assert op.start_trigger_args.trigger_kwargs["moment"] == pendulum.parse("2020-01-01T00:00:00+00:00")
