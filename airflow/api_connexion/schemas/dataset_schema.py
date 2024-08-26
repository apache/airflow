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

from datetime import datetime
from typing import NamedTuple

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.common_schema import JsonObjectField
from airflow.models.asset import (
    AssetAliasModel,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dagrun import DagRun


class TaskOutletAssetReferenceSchema(SQLAlchemySchema):
    """TaskOutletAssetReference DB schema."""

    class Meta:
        """Meta."""

        model = TaskOutletAssetReference

    dag_id = auto_field()
    task_id = auto_field()
    created_at = auto_field()
    updated_at = auto_field()


class DagScheduleAssetReferenceSchema(SQLAlchemySchema):
    """DagScheduleAssetReference DB schema."""

    class Meta:
        """Meta."""

        model = DagScheduleAssetReference

    dag_id = auto_field()
    created_at = auto_field()
    updated_at = auto_field()


class AssetAliasSchema(SQLAlchemySchema):
    """AssetAlias DB schema."""

    class Meta:
        """Meta."""

        model = AssetAliasModel

    id = auto_field()
    name = auto_field()


class AssetSchema(SQLAlchemySchema):
    """Asset DB schema."""

    class Meta:
        """Meta."""

        model = AssetModel

    id = auto_field()
    uri = auto_field()
    extra = JsonObjectField()
    created_at = auto_field()
    updated_at = auto_field()
    producing_tasks = fields.List(fields.Nested(TaskOutletAssetReferenceSchema))
    consuming_dags = fields.List(fields.Nested(DagScheduleAssetReferenceSchema))
    aliases = fields.List(fields.Nested(AssetAliasSchema))


class AssetCollection(NamedTuple):
    """List of Assets with meta."""

    datasets: list[AssetModel]
    total_entries: int


class AssetCollectionSchema(Schema):
    """Asset Collection Schema."""

    datasets = fields.List(fields.Nested(AssetSchema))
    total_entries = fields.Int()


dataset_schema = AssetSchema()
asset_collection_schema = AssetCollectionSchema()


class BasicDAGRunSchema(SQLAlchemySchema):
    """Basic Schema for DAGRun."""

    class Meta:
        """Meta."""

        model = DagRun
        dateformat = "iso"

    run_id = auto_field(data_key="dag_run_id")
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field(data_key="logical_date", dump_only=True)
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = auto_field(dump_only=True)
    data_interval_start = auto_field(dump_only=True)
    data_interval_end = auto_field(dump_only=True)


class AssetEventSchema(SQLAlchemySchema):
    """Asset Event DB schema."""

    class Meta:
        """Meta."""

        model = AssetEvent

    id = auto_field()
    dataset_id = auto_field()
    dataset_uri = fields.String(attribute="dataset.uri", dump_only=True)
    extra = JsonObjectField()
    source_task_id = auto_field()
    source_dag_id = auto_field()
    source_run_id = auto_field()
    source_map_index = auto_field()
    created_dagruns = fields.List(fields.Nested(BasicDAGRunSchema))
    timestamp = auto_field()


class AssetEventCollection(NamedTuple):
    """List of Asset events with meta."""

    dataset_events: list[AssetEvent]
    total_entries: int


class AssetEventCollectionSchema(Schema):
    """Asset Event Collection Schema."""

    dataset_events = fields.List(fields.Nested(AssetEventSchema))
    total_entries = fields.Int()


class CreateAssetEventSchema(Schema):
    """Create Asset Event Schema."""

    dataset_uri = fields.String()
    extra = JsonObjectField()


dataset_event_schema = AssetEventSchema()
asset_event_collection_schema = AssetEventCollectionSchema()
create_asset_event_schema = CreateAssetEventSchema()


class QueuedEvent(NamedTuple):
    """QueuedEvent."""

    uri: str
    dag_id: str
    created_at: datetime


class QueuedEventSchema(Schema):
    """QueuedEvent schema."""

    uri = fields.String()
    dag_id = fields.String()
    created_at = fields.DateTime()


class QueuedEventCollection(NamedTuple):
    """List of QueuedEvent with meta."""

    queued_events: list[QueuedEvent]
    total_entries: int


class QueuedEventCollectionSchema(Schema):
    """QueuedEvent Collection Schema."""

    queued_events = fields.List(fields.Nested(QueuedEventSchema))
    total_entries = fields.Int()


queued_event_schema = QueuedEventSchema()
queued_event_collection_schema = QueuedEventCollectionSchema()
