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

from typing import List, NamedTuple

from marshmallow import ValidationError, fields
from marshmallow.schema import Schema
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.enum_schemas import DagState
from airflow.models.dagrun import DagRun


class DAGRunSchema(SQLAlchemySchema):
    """
    Schema for DAGRun
    """

    class Meta:
        """ Meta """
        model = DagRun

    run_id = auto_field(dump_to='dag_run_id', load_from='dag_run_id')
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field()
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = fields.Method('get_state', deserialize='load_state')
    external_trigger = auto_field(default=True, dump_only=True)
    conf = fields.Method('get_conf')

    @staticmethod
    def get_conf(obj: DagRun):
        """Convert conf to object if None after serialization"""

        if obj.conf is None:
            return {}
        return obj.conf

    @staticmethod
    def get_state(obj: DagRun):
        """ Return a DagState value of state field """
        if obj.state:
            return DagState().dump(dict(state=obj.state)).data['state']
        return obj.state

    @classmethod
    def load_state(cls, value):
        """ Validate state field and return None if it fails """
        try:
            DagState(strict=True).load(dict(state=value))
        except ValidationError:
            return None
        return value


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
