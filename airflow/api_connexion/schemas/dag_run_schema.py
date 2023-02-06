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

import json
from typing import NamedTuple

from marshmallow import fields, post_dump, pre_load, validate
from marshmallow.schema import Schema
from marshmallow.validate import Range
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
from pendulum.parsing import ParserError

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.parameters import validate_istimezone
from airflow.api_connexion.schemas.enum_schemas import DagStateField
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


class ConfObject(fields.Field):
    """The conf field."""

    def _serialize(self, value, attr, obj, **kwargs):
        if not value:
            return {}
        return json.loads(value) if isinstance(value, str) else value

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, str):
            return json.loads(value)
        return value


_MISSING = object()


class DAGRunSchema(SQLAlchemySchema):
    """Schema for DAGRun."""

    class Meta:
        """Meta."""

        model = DagRun
        dateformat = "iso"

    run_id = auto_field(data_key="dag_run_id")
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field(data_key="logical_date", validate=validate_istimezone)
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = DagStateField(dump_only=True)
    external_trigger = auto_field(dump_default=True, dump_only=True)
    conf = ConfObject()
    data_interval_start = auto_field(dump_only=True)
    data_interval_end = auto_field(dump_only=True)
    last_scheduling_decision = auto_field(dump_only=True)
    run_type = auto_field(dump_only=True)
    note = auto_field(dump_only=False)

    @pre_load
    def autogenerate(self, data, **kwargs):
        """Auto generate run_id and logical_date if they are not provided.

        For compatibility, if `execution_date` is submitted, it is converted
        to `logical_date`.
        """
        logical_date = data.get("logical_date", _MISSING)
        execution_date = data.pop("execution_date", _MISSING)
        if logical_date is execution_date is _MISSING:  # Both missing.
            data["logical_date"] = str(timezone.utcnow())
        elif logical_date is _MISSING:  # Only logical_date missing.
            data["logical_date"] = execution_date
        elif execution_date is _MISSING:  # Only execution_date missing.
            pass
        elif logical_date != execution_date:  # Both provided but don't match.
            raise BadRequest(
                "logical_date conflicts with execution_date",
                detail=f"{logical_date!r} != {execution_date!r}",
            )

        if "dag_run_id" not in data:
            try:
                data["dag_run_id"] = DagRun.generate_run_id(
                    DagRunType.MANUAL, timezone.parse(data["logical_date"])
                )
            except (ParserError, TypeError) as err:
                raise BadRequest("Incorrect datetime argument", detail=str(err))
        return data

    @post_dump
    def autofill(self, data, **kwargs):
        """Populate execution_date from logical_date for compatibility."""
        data["execution_date"] = data["logical_date"]
        return data


class SetDagRunStateFormSchema(Schema):
    """Schema for handling the request of setting state of DAG run."""

    state = DagStateField(
        validate=validate.OneOf(
            [DagRunState.SUCCESS.value, DagRunState.FAILED.value, DagRunState.QUEUED.value]
        )
    )


class ClearDagRunStateFormSchema(Schema):
    """Schema for handling the request of clearing a DAG run."""

    dry_run = fields.Boolean(load_default=True)


class DAGRunCollection(NamedTuple):
    """List of DAGRuns with metadata."""

    dag_runs: list[DagRun]
    total_entries: int


class DAGRunCollectionSchema(Schema):
    """DAGRun Collection schema."""

    dag_runs = fields.List(fields.Nested(DAGRunSchema))
    total_entries = fields.Int()


class DagRunsBatchFormSchema(Schema):
    """Schema to validate and deserialize the Form(request payload) submitted to DagRun Batch endpoint."""

    class Meta:
        """Meta."""

        datetimeformat = "iso"
        strict = True

    order_by = fields.String()
    page_offset = fields.Int(load_default=0, validate=Range(min=0))
    page_limit = fields.Int(load_default=100, validate=Range(min=1))
    dag_ids = fields.List(fields.Str(), load_default=None)
    states = fields.List(fields.Str(), load_default=None)
    execution_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    execution_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    start_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    start_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    end_date_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    end_date_lte = fields.DateTime(load_default=None, validate=validate_istimezone)
    updated_at_gte = fields.DateTime(load_default=None, validate=validate_istimezone)
    updated_at_lte = fields.DateTime(load_default=None, validate=validate_istimezone)


class SetDagRunNoteFormSchema(Schema):
    """Schema for handling the request of clearing a DAG run."""

    note = fields.String(allow_none=True, validate=validate.Length(max=1000))


dagrun_schema = DAGRunSchema()
dagrun_collection_schema = DAGRunCollectionSchema()
set_dagrun_state_form_schema = SetDagRunStateFormSchema()
clear_dagrun_form_schema = ClearDagRunStateFormSchema()
dagruns_batch_form_schema = DagRunsBatchFormSchema()
set_dagrun_note_form_schema = SetDagRunNoteFormSchema()
