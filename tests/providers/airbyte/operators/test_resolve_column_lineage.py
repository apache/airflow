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

from openlineage.client.generated.column_lineage_dataset import Fields, InputField

from airflow.providers.airbyte.operators.airbyte import _ColumnLineageResolver


class TestResolveColumnLineage:
    def test_resolve_column_lineage(self):
        column_lineage = _ColumnLineageResolver.resolve_column_lineage(
            namespace="test",
            schema="test_schema",
            stream={
                "field1": {"type": "string"},
                "field2": {"type": "string"},
            },
        )

        assert column_lineage.fields["field1"] == Fields(
            transformationDescription="",
            inputFields=[
                InputField(namespace="test", name="test_schema", field="field1", transformations=[])
            ],
            transformationType="IDENTITY",
        )

        assert column_lineage.fields["field2"] == Fields(
            inputFields=[
                InputField(namespace="test", name="test_schema", field="field2", transformations=[])
            ],
            transformationDescription="",
            transformationType="IDENTITY",
        )
