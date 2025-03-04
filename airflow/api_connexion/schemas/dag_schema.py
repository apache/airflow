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

from itsdangerous import URLSafeSerializer
from marshmallow import fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.configuration import conf
from airflow.models.dag import DagModel, DagTag


class DagTagSchema(SQLAlchemySchema):
    """Dag Tag schema."""

    class Meta:
        """Meta."""

        model = DagTag

    name = auto_field()


class DAGSchema(SQLAlchemySchema):
    """DAG schema."""

    class Meta:
        """Meta."""

        model = DagModel

    dag_id = auto_field(dump_only=True)
    dag_display_name = fields.String(attribute="dag_display_name", dump_only=True)
    bundle_name = auto_field(dump_only=True)
    bundle_version = auto_field(dump_only=True)
    is_paused = auto_field()
    is_active = auto_field(dump_only=True)
    last_parsed_time = auto_field(dump_only=True)
    last_expired = auto_field(dump_only=True)
    default_view = auto_field(dump_only=True)
    fileloc = auto_field(dump_only=True)
    file_token = fields.Method("get_token", dump_only=True)
    owners = fields.Method("get_owners", dump_only=True)
    description = auto_field(dump_only=True)
    timetable_summary = auto_field(dump_only=True)
    timetable_description = auto_field(dump_only=True)
    tags = fields.List(fields.Nested(DagTagSchema), dump_only=True)
    max_active_tasks = auto_field(dump_only=True)
    max_active_runs = auto_field(dump_only=True)
    max_consecutive_failed_dag_runs = auto_field(dump_only=True)
    has_task_concurrency_limits = auto_field(dump_only=True)
    has_import_errors = auto_field(dump_only=True)
    next_dagrun = auto_field(dump_only=True)
    next_dagrun_data_interval_start = auto_field(dump_only=True)
    next_dagrun_data_interval_end = auto_field(dump_only=True)
    next_dagrun_create_after = auto_field(dump_only=True)

    @staticmethod
    def get_owners(obj: DagModel):
        """Convert owners attribute to DAG representation."""
        if not getattr(obj, "owners", None):
            return []
        return obj.owners.split(",")

    @staticmethod
    def get_token(obj: DagModel):
        """Return file token."""
        serializer = URLSafeSerializer(conf.get_mandatory_value("webserver", "secret_key"))
        return serializer.dumps(obj.fileloc)


dag_schema = DAGSchema()
