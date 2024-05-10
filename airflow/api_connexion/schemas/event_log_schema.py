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

from airflow.models.log import Log


class EventLogSchema(SQLAlchemySchema):
    """Event log schema."""

    class Meta:
        """Meta."""

        model = Log

    id = auto_field(data_key="event_log_id", dump_only=True)
    dttm = auto_field(data_key="when", dump_only=True)
    dag_id = auto_field(dump_only=True)
    task_id = auto_field(dump_only=True)
    run_id = auto_field(dump_only=True)
    event = auto_field(dump_only=True)
    execution_date = auto_field(dump_only=True)
    owner = auto_field(dump_only=True)
    extra = auto_field(dump_only=True)


class EventLogCollection(NamedTuple):
    """List of import errors with metadata."""

    event_logs: list[Log]
    total_entries: int


class EventLogCollectionSchema(Schema):
    """EventLog Collection Schema."""

    event_logs = fields.List(fields.Nested(EventLogSchema))
    total_entries = fields.Int()


event_log_schema = EventLogSchema()
event_log_collection_schema = EventLogCollectionSchema()
