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

import datetime
import unittest

from dateutil import relativedelta

from airflow.api_connexion.schemas.common_schema import (
    CronExpression,
    CronExpressionSchema,
    RelativeDeltaSchema,
    ScheduleIntervalSchema,
    TimeDeltaSchema,
)


class TestTimeDeltaSchema(unittest.TestCase):
    def test_should_serialize(self):
        instance = datetime.timedelta(days=12)
        schema_instance = TimeDeltaSchema()
        result = schema_instance.dump(instance)
        self.assertEqual({"__type": "TimeDelta", "days": 12, "seconds": 0, "microseconds": 0}, result)

    def test_should_deserialize(self):
        instance = {"__type": "TimeDelta", "days": 12, "seconds": 0, "microseconds": 0}
        schema_instance = TimeDeltaSchema()
        result = schema_instance.load(instance)
        expected_instance = datetime.timedelta(days=12)
        self.assertEqual(expected_instance, result)


class TestRelativeDeltaSchema(unittest.TestCase):
    def test_should_serialize(self):
        instance = relativedelta.relativedelta(days=+12)
        schema_instance = RelativeDeltaSchema()
        result = schema_instance.dump(instance)
        self.assertEqual(
            {
                '__type': 'RelativeDelta',
                "day": None,
                "days": 12,
                "hour": None,
                "hours": 0,
                "leapdays": 0,
                "microsecond": None,
                "microseconds": 0,
                "minute": None,
                "minutes": 0,
                "month": None,
                "months": 0,
                "second": None,
                "seconds": 0,
                "year": None,
                "years": 0,
            },
            result,
        )

    def test_should_deserialize(self):
        instance = {"__type": "RelativeDelta", "days": 12, "seconds": 0}
        schema_instance = RelativeDeltaSchema()
        result = schema_instance.load(instance)
        expected_instance = relativedelta.relativedelta(days=+12)
        self.assertEqual(expected_instance, result)


class TestCronExpressionSchema(unittest.TestCase):
    def test_should_deserialize(self):
        instance = {"__type": "CronExpression", "value": "5 4 * * *"}
        schema_instance = CronExpressionSchema()
        result = schema_instance.load(instance)
        expected_instance = CronExpression("5 4 * * *")
        self.assertEqual(expected_instance, result)


class TestScheduleIntervalSchema(unittest.TestCase):
    def test_should_serialize_timedelta(self):
        instance = datetime.timedelta(days=12)
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        self.assertEqual({"__type": "TimeDelta", "days": 12, "seconds": 0, "microseconds": 0}, result)

    def test_should_deserialize_timedelta(self):
        instance = {"__type": "TimeDelta", "days": 12, "seconds": 0, "microseconds": 0}
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.load(instance)
        expected_instance = datetime.timedelta(days=12)
        self.assertEqual(expected_instance, result)

    def test_should_serialize_relative_delta(self):
        instance = relativedelta.relativedelta(days=+12)
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        self.assertEqual(
            {
                "__type": "RelativeDelta",
                "day": None,
                "days": 12,
                "hour": None,
                "hours": 0,
                "leapdays": 0,
                "microsecond": None,
                "microseconds": 0,
                "minute": None,
                "minutes": 0,
                "month": None,
                "months": 0,
                "second": None,
                "seconds": 0,
                "year": None,
                "years": 0,
            },
            result,
        )

    def test_should_deserialize_relative_delta(self):
        instance = {"__type": "RelativeDelta", "days": 12, "seconds": 0}
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.load(instance)
        expected_instance = relativedelta.relativedelta(days=+12)
        self.assertEqual(expected_instance, result)

    def test_should_serialize_cron_expression(self):
        instance = "5 4 * * *"
        schema_instance = ScheduleIntervalSchema()
        result = schema_instance.dump(instance)
        expected_instance = {"__type": "CronExpression", "value": "5 4 * * *"}
        self.assertEqual(expected_instance, result)

    def test_should_error_unknown_obj_type(self):
        instance = 342
        schema_instance = ScheduleIntervalSchema()
        with self.assertRaisesRegex(Exception, "Unknown object type: int"):
            schema_instance.dump(instance)
