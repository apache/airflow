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

from airflow.api_connexion.schemas.common_schema import ScheduleIntervalSchema, TimeDeltaSchema, TimezoneField
from airflow.models.dag import DagModel, DagTag


class DagTagSchema(SQLAlchemySchema):
    """Dag Tag schema"""
    class Meta:
        """Meta"""

        model = DagTag

    name = auto_field()


class DAGSchema(SQLAlchemySchema):
    """DAG schema"""

    class Meta:
        """Meta"""
        model = DagModel

    dag_id = auto_field(dump_only=True)
    root_dag_id = auto_field(dump_only=True)
    is_paused = auto_field(dump_only=True)
    is_subdag = auto_field(dump_only=True)
    fileloc = auto_field(dump_only=True)
    owners = fields.Method("get_owners", dump_only=True)
    description = auto_field(dump_only=True)
    schedule_interval = fields.Nested(ScheduleIntervalSchema, dump_only=True)
    tags = fields.List(fields.Nested(DagTagSchema), dump_only=True)

    @staticmethod
    def get_owners(obj: DagModel):
        """Convert owners attribute to DAG representation"""

        if not getattr(obj, 'owners', None):
            return []
        return obj.owners.split(",")


class DAGDetailSchema(DAGSchema):
    """DAG details"""

    timezone = TimezoneField(dump_only=True)
    catchup = fields.Boolean(dump_only=True)
    orientation = fields.String(dump_only=True)
    concurrency = fields.Integer(dump_only=True)
    start_date = fields.DateTime(dump_only=True)
    dag_run_timeout = fields.Nested(TimeDeltaSchema, dump_only=True, attribute="dagrun_timeout")
    doc_md = fields.String(dump_only=True)
    default_view = fields.String(dump_only=True)


class DAGCollection(NamedTuple):
    """List of DAGs with metadata"""

    dags: List[DagModel]
    total_entries: int


class DAGCollectionSchema(Schema):
    """DAG Collection schema"""

    dags = fields.List(fields.Nested(DAGSchema))
    total_entries = fields.Int()


dags_collection_schema = DAGCollectionSchema()
dag_schema = DAGSchema()
dag_detail_schema = DAGDetailSchema()
