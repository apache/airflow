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

from typing import List, NamedTuple

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.common_schema import JsonObjectField
from airflow.models.dataset import DatasetDagRef, DatasetEvent, DatasetModel, DatasetTaskRef


class DatasetTaskRefSchema(SQLAlchemySchema):
    """DatasetTaskRef DB schema"""

    class Meta:
        """Meta"""

        model = DatasetTaskRef

    dag_id = auto_field()
    task_id = auto_field()
    created_at = auto_field()
    updated_at = auto_field()


class DatasetDagRefSchema(SQLAlchemySchema):
    """DatasetDagRef DB schema"""

    class Meta:
        """Meta"""

        model = DatasetDagRef

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
    producing_tasks = fields.List(fields.Nested(DatasetTaskRefSchema))
    consuming_dags = fields.List(fields.Nested(DatasetDagRefSchema))


class DatasetCollection(NamedTuple):
    """List of Datasets with meta"""

    datasets: List[DatasetModel]
    total_entries: int


class DatasetCollectionSchema(Schema):
    """Dataset Collection Schema"""

    datasets = fields.List(fields.Nested(DatasetSchema))
    total_entries = fields.Int()


dataset_schema = DatasetSchema()
dataset_collection_schema = DatasetCollectionSchema()


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
    timestamp = auto_field()


class DatasetEventCollection(NamedTuple):
    """List of Dataset events with meta"""

    dataset_events: List[DatasetEvent]
    total_entries: int


class DatasetEventCollectionSchema(Schema):
    """Dataset Event Collection Schema"""

    dataset_events = fields.List(fields.Nested(DatasetEventSchema))
    total_entries = fields.Int()


dataset_event_schema = DatasetEventSchema()
dataset_event_collection_schema = DatasetEventCollectionSchema()
