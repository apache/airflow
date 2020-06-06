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
from marshmallow import fields
from marshmallow_sqlalchemy import auto_field

from airflow.api_connexion.schemas.base_schema import BaseSchema
from airflow.api_connexion.schemas.enum_schemas import DagState
from airflow.models.dagrun import DagRun


class DAGRunSchema(BaseSchema):
    """
    Schema for DAGRun
    """
    COLLECTION_NAME = 'dag_runs'

    class Meta:
        """ Meta """
        model = DagRun

    run_id = auto_field(dump_to='dag_run_id', load_from='dag_run_id')
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field()
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = fields.Str(DagState())
    external_trigger = auto_field(default=True, dump_only=True)
    conf = fields.Method('get_conf')

    @staticmethod
    def get_conf(obj: DagRun):
        """Convert conf to object if None after serialization"""

        if obj.conf is None:
            return {}
        return obj.conf


dagrun_schema = DAGRunSchema()
dagrun_collection_schema = DAGRunSchema(many=True)
