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

from airflow.providers.airbyte.utils.utils import resolve_table_schema


class TestResolveTableSchema:
    def test_source_based(self):
        schema = resolve_table_schema(
            name_source="source",
            source={"namespace": "source_namespace"},
            destination=None,
            connection=None,
        )

        assert "source_namespace" == schema

        # missing key
        schema = resolve_table_schema(
            name_source="source",
            source={},
            destination=None,
            connection=None,
        )

        assert schema is None

    def test_destination_based(self):
        schema = resolve_table_schema(
            name_source="destination",
            source={"namespace": "source_namespace"},
            destination=None,
            connection=None,
        )

        assert schema is None

        schema = resolve_table_schema(
            name_source="destination",
            source={"namespace": "source_namespace"},
            destination={"connectionConfiguration": {"schema": "destination_schema"}},
            connection=None,
        )

        assert schema == "destination_schema"

        schema = resolve_table_schema(
            name_source="destination",
            source={"namespace": "source_namespace"},
            destination={"connectionConfiguration": {}},
            connection=None,
        )

        assert schema is None

    def test_invalid_name_source(self):
        name_source = "invalid"
        schema = resolve_table_schema(
            name_source=name_source,
            source={"namespace": "source_namespace"},
            destination=None,
            connection=None,
        )

        assert schema is None

    def test_custom_format(self):
        schema = resolve_table_schema(
            name_source="customformat",
            source={"namespace": "source_namespace"},
            destination=None,
            connection={"namespaceFormat": "custom_format"},
        )

        assert schema == "custom_format"

        schema = resolve_table_schema(
            name_source="customformat",
            source={"namespace": "source_namespace"},
            destination=None,
            connection={},
        )

        assert schema is None
