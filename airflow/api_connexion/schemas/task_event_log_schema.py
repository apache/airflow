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
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models.taskinstance import TaskEventLog


class TaskEventLogSchema(SQLAlchemySchema):
    """Event log schema."""

    class Meta:
        """Meta."""

        model = TaskEventLog

    id = auto_field(dump_only=True)
    dag_id = auto_field(dump_only=True)
    task_id = auto_field(dump_only=True)
    run_id = auto_field(dump_only=True)
    map_index = auto_field(dump_only=True)
    try_number = auto_field(dump_only=True)
    description = auto_field(dump_only=True)


class TaskEventLogCollection(NamedTuple):
    """List of import errors with metadata."""

    data: list[TaskEventLog]
    total_entries: int


class TaskEventLogCollectionSchema(Schema):
    """EventLog Collection Schema."""

    data = fields.List(fields.Nested(TaskEventLogSchema))
    total_entries = fields.Int()


task_event_log_schema = TaskEventLogSchema()
task_event_log_collection_schema = TaskEventLogCollectionSchema()
