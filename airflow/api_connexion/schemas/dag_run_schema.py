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
import json
from typing import List, NamedTuple

from marshmallow import fields
from marshmallow.schema import Schema
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.enum_schemas import DagStateField
from airflow.models.dagrun import DagRun


class ConfObject(fields.Field):
    """ The conf field"""
    def _serialize(self, value, attr, obj, **kwargs):
        if not value:
            return {}
        return json.loads(value) if isinstance(value, str) else value

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, str):
            return json.loads(value)
        return value


class DAGRunSchema(SQLAlchemySchema):
    """
    Schema for DAGRun
    """

    class Meta:
        """ Meta """
        model = DagRun
        dateformat = 'iso'

    run_id = auto_field(data_key='dag_run_id')
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field()
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = DagStateField()
    external_trigger = auto_field(default=True, dump_only=True)
    conf = ConfObject()


class DAGRunCollection(NamedTuple):
    """List of DAGRuns with metadata"""

    dag_runs: List[DagRun]
    total_entries: int


class DAGRunCollectionSchema(Schema):
    """DAGRun Collection schema"""
    dag_runs = fields.List(fields.Nested(DAGRunSchema))
    total_entries = fields.Int()


dagrun_schema = DAGRunSchema()
dagrun_collection_schema = DAGRunCollectionSchema()
