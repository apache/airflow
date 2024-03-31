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

from airflow.models.dagwarning import DagWarning


class DagWarningSchema(SQLAlchemySchema):
    """Import error schema."""

    class Meta:
        """Meta."""

        model = DagWarning

    dag_id = auto_field(data_key="dag_id", dump_only=True)
    warning_type = auto_field(dump_only=True)
    message = auto_field(dump_only=True)
    timestamp = auto_field(format="iso", dump_only=True)


class DagWarningCollection(NamedTuple):
    """List of dag warnings with metadata."""

    dag_warnings: list[DagWarning]
    total_entries: int


class DagWarningCollectionSchema(Schema):
    """Import error collection schema."""

    dag_warnings = fields.List(fields.Nested(DagWarningSchema))
    total_entries = fields.Int()


dag_warning_schema = DagWarningSchema()
dag_warning_collection_schema = DagWarningCollectionSchema()
