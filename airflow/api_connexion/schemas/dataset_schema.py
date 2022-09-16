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

from airflow.api_connexion.schemas.common_schema import JsonObjectField
from airflow.models.dagrun import DagRun
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)


class TaskOutletDatasetReferenceSchema(SQLAlchemySchema):
    """TaskOutletDatasetReference DB schema"""

    class Meta:
        """Meta"""

        model = TaskOutletDatasetReference

    dag_id = auto_field()
    task_id = auto_field()
    created_at = auto_field()
    updated_at = auto_field()


class DagScheduleDatasetReferenceSchema(SQLAlchemySchema):
    """DagScheduleDatasetReference DB schema"""

    class Meta:
        """Meta"""

        model = DagScheduleDatasetReference

    dag_id = auto_field()
    created_at = auto_field()
    updated_at = auto_field()


class DatasetSchema(SQLAlchemySchema):
    """Dataset DB schema"""

    class Meta:
        """Meta"""

        model = DatasetModel

    id = auto_field()
    uri = auto_field()
    extra = JsonObjectField()
    created_at = auto_field()
    updated_at = auto_field()
    producing_tasks = fields.List(fields.Nested(TaskOutletDatasetReferenceSchema))
    consuming_dags = fields.List(fields.Nested(DagScheduleDatasetReferenceSchema))


class DatasetCollection(NamedTuple):
    """List of Datasets with meta"""

    datasets: list[DatasetModel]
    total_entries: int


class DatasetCollectionSchema(Schema):
    """Dataset Collection Schema"""

    datasets = fields.List(fields.Nested(DatasetSchema))
    total_entries = fields.Int()


dataset_schema = DatasetSchema()
dataset_collection_schema = DatasetCollectionSchema()


class BasicDAGRunSchema(SQLAlchemySchema):
    """Basic Schema for DAGRun"""

    class Meta:
        """Meta"""

        model = DagRun
        dateformat = "iso"

    run_id = auto_field(data_key='dag_run_id')
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field(data_key="logical_date", dump_only=True)
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = auto_field(dump_only=True)
    data_interval_start = auto_field(dump_only=True)
    data_interval_end = auto_field(dump_only=True)


class DatasetEventSchema(SQLAlchemySchema):
    """Dataset Event DB schema"""

    class Meta:
        """Meta"""

        model = DatasetEvent

    id = auto_field()
    dataset_id = auto_field()
    dataset_uri = fields.String(attribute='dataset.uri', dump_only=True)
    extra = JsonObjectField()
    source_task_id = auto_field()
    source_dag_id = auto_field()
    source_run_id = auto_field()
    source_map_index = auto_field()
    created_dagruns = fields.List(fields.Nested(BasicDAGRunSchema))
    timestamp = auto_field()


class DatasetEventCollection(NamedTuple):
    """List of Dataset events with meta"""

    dataset_events: list[DatasetEvent]
    total_entries: int


class DatasetEventCollectionSchema(Schema):
    """Dataset Event Collection Schema"""

    dataset_events = fields.List(fields.Nested(DatasetEventSchema))
    total_entries = fields.Int()


dataset_event_schema = DatasetEventSchema()
dataset_event_collection_schema = DatasetEventCollectionSchema()
