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

from marshmallow import Schema, fields

from airflow.api_connexion.schemas.common_schema import (
    ClassReferenceSchema,
    ColorField,
    TimeDeltaSchema,
    WeightRuleField,
)
from airflow.api_connexion.schemas.dag_schema import DAGSchema
from airflow.models.operator import Operator


class TaskSchema(Schema):
    """Task schema"""

    class_ref = fields.Method("_get_class_reference", dump_only=True)
    operator_name = fields.Method("_get_operator_name", dump_only=True)
    task_id = fields.String(dump_only=True)
    owner = fields.String(dump_only=True)
    start_date = fields.DateTime(dump_only=True)
    end_date = fields.DateTime(dump_only=True)
    trigger_rule = fields.String(dump_only=True)
    extra_links = fields.List(
        fields.Nested(ClassReferenceSchema), dump_only=True, attribute="operator_extra_links"
    )
    depends_on_past = fields.Boolean(dump_only=True)
    wait_for_downstream = fields.Boolean(dump_only=True)
    retries = fields.Number(dump_only=True)
    queue = fields.String(dump_only=True)
    pool = fields.String(dump_only=True)
    pool_slots = fields.Number(dump_only=True)
    execution_timeout = fields.Nested(TimeDeltaSchema, dump_only=True)
    retry_delay = fields.Nested(TimeDeltaSchema, dump_only=True)
    retry_exponential_backoff = fields.Boolean(dump_only=True)
    priority_weight = fields.Number(dump_only=True)
    weight_rule = WeightRuleField(dump_only=True)
    ui_color = ColorField(dump_only=True)
    ui_fgcolor = ColorField(dump_only=True)
    template_fields = fields.List(fields.String(), dump_only=True)
    sub_dag = fields.Nested(DAGSchema, dump_only=True)
    downstream_task_ids = fields.List(fields.String(), dump_only=True)
    params = fields.Method('get_params', dump_only=True)
    is_mapped = fields.Boolean(dump_only=True)

    def _get_class_reference(self, obj):
        result = ClassReferenceSchema().dump(obj)
        return result.data if hasattr(result, "data") else result

    def _get_operator_name(self, obj):
        return obj.operator_name

    @staticmethod
    def get_params(obj):
        """Get the Params defined in a Task"""
        params = obj.params
        return {k: v.dump() for k, v in params.items()}


class TaskCollection(NamedTuple):
    """List of Tasks with metadata"""

    tasks: list[Operator]
    total_entries: int


class TaskCollectionSchema(Schema):
    """Schema for TaskCollection"""

    tasks = fields.List(fields.Nested(TaskSchema))
    total_entries = fields.Int()


task_schema = TaskSchema()
task_collection_schema = TaskCollectionSchema()
