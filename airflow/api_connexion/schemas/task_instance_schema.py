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

from typing import NamedTuple

from marshmallow import Schema, ValidationError, fields, validate, validates_schema
from marshmallow.utils import get_value
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.parameters import validate_istimezone
from airflow.api_connexion.schemas.common_schema import JsonObjectField
from airflow.api_connexion.schemas.enum_schemas import TaskInstanceStateField
from airflow.api_connexion.schemas.sla_miss_schema import SlaMissSchema
from airflow.models import SlaMiss, TaskInstance
from airflow.utils.helpers import exactly_one
from airflow.utils.state import State


class TaskInstanceSchema(SQLAlchemySchema):
    """Task instance schema"""

    class Meta:
        """Meta"""

        model = TaskInstance

    task_id = auto_field()
    dag_id = auto_field()
    run_id = auto_field(data_key="dag_run_id")
    map_index = auto_field()
    execution_date = auto_field()
    start_date = auto_field()
    end_date = auto_field()
    duration = auto_field()
    state = TaskInstanceStateField()
    _try_number = auto_field(data_key="try_number")
    max_tries = auto_field()
    hostname = auto_field()
    unixname = auto_field()
    pool = auto_field()
    pool_slots = auto_field()
    queue = auto_field()
    priority_weight = auto_field()
    operator = auto_field()
    queued_dttm = auto_field(data_key="queued_when")
    pid = auto_field()
    executor_config = auto_field()
    sla_miss = fields.Nested(SlaMissSchema, dump_default=None)
    rendered_fields = JsonObjectField(dump_default={})

    def get_attribute(self, obj, attr, default):
        if attr == "sla_miss":
            # Object is a tuple of task_instance and slamiss
            # and the get_value expects a dict with key, value
            # corresponding to the attr.
            slamiss_instance = {"sla_miss": obj[1]}
            return get_value(slamiss_instance, attr, default)
        elif attr == "rendered_fields":
            return get_value(obj[0], "rendered_task_instance_fields.rendered_fields", default)
        return get_value(obj[0], attr, default)


class TaskInstanceCollection(NamedTuple):
    """List of task instances with metadata"""

    task_instances: list[tuple[TaskInstance, SlaMiss | None]]
    total_entries: int


class TaskInstanceCollectionSchema(Schema):
    """Task instance collection schema"""

    task_instances = fields.List(fields.Nested(TaskInstanceSchema))
    total_entries = fields.Int()


class TaskInstanceBatchFormSchema(Schema):
    """Schema for the request form passed to Task Instance Batch endpoint"""

    page_offset = fields.Int(load_default=0, validate=validate.Range(min=0))
    page_limit = fields.Int(load_default=100, validate=validate.Range(min=1))
    dag_ids = fields.List(fields.Str(), load_default=None)
    execution_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    execution_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    start_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    start_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    end_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    end_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    duration_gte = fields.Int(load_default=None)
    duration_lte = fields.Int(load_default=None)
    state = fields.List(fields.Str(), load_default=None)
    pool = fields.List(fields.Str(), load_default=None)
    queue = fields.List(fields.Str(), load_default=None)


class ClearTaskInstanceFormSchema(Schema):
    """Schema for handling the request of clearing task instance of a Dag"""

    dry_run = fields.Boolean(load_default=True)
    start_date = fields.DateTime(load_default=None, validate=validate_istimezone)
    end_date = fields.DateTime(load_default=None, validate=validate_istimezone)
    only_failed = fields.Boolean(load_default=True)
    only_running = fields.Boolean(load_default=False)
    include_subdags = fields.Boolean(load_default=False)
    include_parentdag = fields.Boolean(load_default=False)
    reset_dag_runs = fields.Boolean(load_default=False)
    task_ids = fields.List(fields.String(), validate=validate.Length(min=1))
    dag_run_id = fields.Str(load_default=None)
    include_upstream = fields.Boolean(load_default=False)
    include_downstream = fields.Boolean(load_default=False)
    include_future = fields.Boolean(load_default=False)
    include_past = fields.Boolean(load_default=False)

    @validates_schema
    def validate_form(self, data, **kwargs):
        """Validates clear task instance form"""
        if data["only_failed"] and data["only_running"]:
            raise ValidationError("only_failed and only_running both are set to True")
        if data["start_date"] and data["end_date"]:
            if data["start_date"] > data["end_date"]:
                raise ValidationError("end_date is sooner than start_date")
        if data["start_date"] and data["end_date"] and data["dag_run_id"]:
            raise ValidationError("Exactly one of dag_run_id or (start_date and end_date) must be provided")
        if data["start_date"] and data["dag_run_id"]:
            raise ValidationError("Exactly one of dag_run_id or start_date must be provided")
        if data["end_date"] and data["dag_run_id"]:
            raise ValidationError("Exactly one of dag_run_id or end_date must be provided")


class SetTaskInstanceStateFormSchema(Schema):
    """Schema for handling the request of setting state of task instance of a DAG"""

    dry_run = fields.Boolean(dump_default=True)
    task_id = fields.Str(required=True)
    execution_date = fields.DateTime(validate=validate_istimezone)
    dag_run_id = fields.Str()
    include_upstream = fields.Boolean(required=True)
    include_downstream = fields.Boolean(required=True)
    include_future = fields.Boolean(required=True)
    include_past = fields.Boolean(required=True)
    new_state = TaskInstanceStateField(required=True, validate=validate.OneOf([State.SUCCESS, State.FAILED]))

    @validates_schema
    def validate_form(self, data, **kwargs):
        """Validates set task instance state form"""
        if not exactly_one(data.get("execution_date"), data.get("dag_run_id")):
            raise ValidationError("Exactly one of execution_date or dag_run_id must be provided")


class TaskInstanceReferenceSchema(Schema):
    """Schema for the task instance reference schema"""

    task_id = fields.Str()
    run_id = fields.Str(data_key="dag_run_id")
    dag_id = fields.Str()
    execution_date = fields.DateTime()


class TaskInstanceReferenceCollection(NamedTuple):
    """List of objects with metadata about taskinstance and dag_run_id"""

    task_instances: list[tuple[TaskInstance, str]]


class TaskInstanceReferenceCollectionSchema(Schema):
    """Collection schema for task reference"""

    task_instances = fields.List(fields.Nested(TaskInstanceReferenceSchema))


task_instance_schema = TaskInstanceSchema()
task_instance_collection_schema = TaskInstanceCollectionSchema()
task_instance_batch_form = TaskInstanceBatchFormSchema()
clear_task_instance_form = ClearTaskInstanceFormSchema()
set_task_instance_state_form = SetTaskInstanceStateFormSchema()
task_instance_reference_schema = TaskInstanceReferenceSchema()
task_instance_reference_collection_schema = TaskInstanceReferenceCollectionSchema()
