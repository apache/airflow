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

from airflow.models.errors import ParseImportError


class ImportErrorSchema(SQLAlchemySchema):
    """Import error schema."""

    class Meta:
        """Meta."""

        model = ParseImportError

    import_error_id = auto_field("id", dump_only=True)
    timestamp = auto_field(format="iso", dump_only=True)
    filename = auto_field(dump_only=True)
    stack_trace = auto_field("stacktrace", dump_only=True)


class ImportErrorCollection(NamedTuple):
    """List of import errors with metadata."""

    import_errors: list[ParseImportError]
    total_entries: int


class ImportErrorCollectionSchema(Schema):
    """Import error collection schema."""

    import_errors = fields.List(fields.Nested(ImportErrorSchema))
    total_entries = fields.Int()


import_error_schema = ImportErrorSchema()
import_error_collection_schema = ImportErrorCollectionSchema()
