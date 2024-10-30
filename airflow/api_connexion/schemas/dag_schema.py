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

from typing import TYPE_CHECKING, NamedTuple

from itsdangerous import URLSafeSerializer
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.schemas.common_schema import TimeDeltaSchema, TimezoneField
from airflow.configuration import conf
from airflow.models.dag import DagModel, DagTag

if TYPE_CHECKING:
    from airflow import DAG


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
    is_paused = auto_field()
    is_active = auto_field(dump_only=True)
    last_parsed_time = auto_field(dump_only=True)
    last_pickled = auto_field(dump_only=True)
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


class DAGDetailSchema(DAGSchema):
    """DAG details."""

    owners = fields.Method("get_owners", dump_only=True)
    timezone = TimezoneField(dump_only=True)
    catchup = fields.Boolean(dump_only=True)
    orientation = fields.String(dump_only=True)
    max_active_tasks = fields.Integer(dump_only=True)
    asset_expression = fields.Dict(allow_none=True)
    start_date = fields.DateTime(dump_only=True)
    dag_run_timeout = fields.Nested(TimeDeltaSchema, attribute="dagrun_timeout", dump_only=True)
    doc_md = fields.String(dump_only=True)
    default_view = fields.String(dump_only=True)
    params = fields.Method("get_params", dump_only=True)
    tags = fields.Method("get_tags", dump_only=True)  # type: ignore
    is_paused = fields.Method("get_is_paused", dump_only=True)
    is_active = fields.Method("get_is_active", dump_only=True)
    is_paused_upon_creation = fields.Boolean(dump_only=True)
    end_date = fields.DateTime(dump_only=True)
    template_searchpath = fields.String(dump_only=True)
    render_template_as_native_obj = fields.Boolean(dump_only=True)
    last_loaded = fields.DateTime(dump_only=True, data_key="last_parsed")

    @staticmethod
    def get_tags(obj: DAG):
        """Dump tags as objects."""
        tags = obj.tags
        if tags:
            return [DagTagSchema().dump({"name": tag}) for tag in tags]
        return []

    @staticmethod
    def get_owners(obj: DAG):
        """Convert owners attribute to DAG representation."""
        if not getattr(obj, "owner", None):
            return []
        return obj.owner.split(",")

    @staticmethod
    def get_is_paused(obj: DAG):
        """Check entry in DAG table to see if this DAG is paused."""
        return obj.get_is_paused()

    @staticmethod
    def get_is_active(obj: DAG):
        """Check entry in DAG table to see if this DAG is active."""
        return obj.get_is_active()

    @staticmethod
    def get_params(obj: DAG):
        """Get the Params defined in a DAG."""
        params = obj.params
        return {k: v.dump() for k, v in params.items()}


class DAGCollection(NamedTuple):
    """List of DAGs with metadata."""

    dags: list[DagModel]
    total_entries: int


class DAGCollectionSchema(Schema):
    """DAG Collection schema."""

    dags = fields.List(fields.Nested(DAGSchema))
    total_entries = fields.Int()


dags_collection_schema = DAGCollectionSchema()
dag_schema = DAGSchema()

dag_detail_schema = DAGDetailSchema()
