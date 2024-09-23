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

from typing import NamedTuple

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models.backfill import Backfill, BackfillDagRun


class BackfillSchema(SQLAlchemySchema):
    """Backfill Schema."""

    class Meta:
        """Meta."""

        model = Backfill

    id = auto_field(dump_only=True)
    dag_id = auto_field(dump_only=True)
    from_date = auto_field(dump_only=True)
    to_date = auto_field(dump_only=True)
    dag_run_conf = fields.Dict(allow_none=True)
    is_paused = auto_field(dump_only=True)
    max_active_runs = auto_field(dump_only=True)
    created_at = auto_field(dump_only=True)
    completed_at = auto_field(dump_only=True)
    updated_at = auto_field(dump_only=True)


class BackfillDagRunSchema(SQLAlchemySchema):
    """Trigger Schema."""

    class Meta:
        """Meta."""

        model = BackfillDagRun

    id = auto_field(dump_only=True)
    backfill_id = auto_field(dump_only=True)
    dag_run_id = auto_field(dump_only=True)
    sort_ordinal = auto_field(dump_only=True)


class BackfillCollection(NamedTuple):
    """List of Backfills with meta."""

    backfills: list[Backfill]
    total_entries: int


class BackfillCollectionSchema(Schema):
    """Backfill Collection Schema."""

    backfills = fields.List(fields.Nested(BackfillSchema))
    total_entries = fields.Int()


backfill_schema = BackfillSchema()
backfill_dag_run_schema = BackfillDagRunSchema()
backfill_collection_schema = BackfillCollectionSchema()
