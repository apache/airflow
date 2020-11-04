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
import inspect
import typing

import marshmallow
from dateutil import relativedelta
from marshmallow import Schema, fields, validate
from marshmallow_oneofschema import OneOfSchema

from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.weight_rule import WeightRule


class CronExpression(typing.NamedTuple):
    """Cron expression schema"""

    value: str


class TimeDeltaSchema(Schema):
    """Time delta schema"""

    objectType = fields.Constant("TimeDelta", data_key="__type")
    days = fields.Integer()
    seconds = fields.Integer()
    microseconds = fields.Integer()

    @marshmallow.post_load
    def make_time_delta(self, data, **kwargs):
        """Create time delta based on data"""
        if "objectType" in data:
            del data["objectType"]
        return datetime.timedelta(**data)


class RelativeDeltaSchema(Schema):
    """Relative delta schema"""

    objectType = fields.Constant("RelativeDelta", data_key="__type")
    years = fields.Integer()
    months = fields.Integer()
    days = fields.Integer()
    leapdays = fields.Integer()
    hours = fields.Integer()
    minutes = fields.Integer()
    seconds = fields.Integer()
    microseconds = fields.Integer()
    year = fields.Integer()
    month = fields.Integer()
    day = fields.Integer()
    hour = fields.Integer()
    minute = fields.Integer()
    second = fields.Integer()
    microsecond = fields.Integer()

    @marshmallow.post_load
    def make_relative_delta(self, data, **kwargs):
        """Create relative delta based on data"""
        if "objectType" in data:
            del data["objectType"]

        return relativedelta.relativedelta(**data)


class CronExpressionSchema(Schema):
    """Cron expression schema"""

    objectType = fields.Constant("CronExpression", data_key="__type")
    value = fields.String(required=True)

    @marshmallow.post_load
    def make_cron_expression(self, data, **kwargs):
        """Create cron expression based on data"""
        return CronExpression(data["value"])


class ScheduleIntervalSchema(OneOfSchema):
    """
    Schedule interval.

    It supports the following types:

    * TimeDelta
    * RelativeDelta
    * CronExpression
    """

    type_field = "__type"
    type_schemas = {
        "TimeDelta": TimeDeltaSchema,
        "RelativeDelta": RelativeDeltaSchema,
        "CronExpression": CronExpressionSchema,
    }

    def _dump(self, obj, update_fields=True, **kwargs):
        if isinstance(obj, str):
            obj = CronExpression(obj)

        return super()._dump(obj, update_fields=update_fields, **kwargs)

    def get_obj_type(self, obj):
        """Select schema based on object type"""
        if isinstance(obj, datetime.timedelta):
            return "TimeDelta"
        elif isinstance(obj, relativedelta.relativedelta):
            return "RelativeDelta"
        elif isinstance(obj, CronExpression):
            return "CronExpression"
        else:
            raise Exception(f"Unknown object type: {obj.__class__.__name__}")


class ColorField(fields.String):
    """Schema for color property"""

    def __init__(self, **metadata):
        super().__init__(**metadata)
        self.validators = [validate.Regexp("^#[a-fA-F0-9]{3,6}$")] + list(self.validators)


class WeightRuleField(fields.String):
    """Schema for WeightRule"""

    def __init__(self, **metadata):
        super().__init__(**metadata)
        self.validators = [validate.OneOf(WeightRule.all_weight_rules())] + list(self.validators)


class TimezoneField(fields.String):
    """Schema for timezone"""


class ClassReferenceSchema(Schema):
    """Class reference schema."""

    module_path = fields.Method("_get_module", required=True)
    class_name = fields.Method("_get_class_name", required=True)

    def _get_module(self, obj):
        if isinstance(obj, SerializedBaseOperator):
            return obj._task_module  # pylint: disable=protected-access
        return inspect.getmodule(obj).__name__

    def _get_class_name(self, obj):
        if isinstance(obj, SerializedBaseOperator):
            return obj._task_type  # pylint: disable=protected-access
        if isinstance(obj, type):
            return obj.__name__
        return type(obj).__name__
