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

from openlineage.client.generated.schema_dataset import SchemaDatasetFacetFields

from airflow.providers.airbyte.operators.airbyte import _AirbyteOlSchemaResolver


def test_map_airbyte_types_to_ol():
    schema_resolver = _AirbyteOlSchemaResolver()

    result = schema_resolver.map_airbyte_type_to_ol_schema_field(
        {
            # primitive types
            "field1": {"type": "string", "format": "date-time"},
            "field2": {
                "type": "integer",
            },
            "field3": {
                "type": "number",
            },
            "field4": {
                "type": "boolean",
            },
            "field5": {
                "type": "string",
            },
            "field6": {"type": "string", "format": "date"},
            # complex types
            "field7": {
                "type": "object",
                "properties": {
                    "field3": {
                        "type": "string",
                    }
                },
            },
            # array of primitive types
            "field8": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
            # array of complex types
            "field9": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field1": {
                            "type": "string",
                        }
                    },
                },
            },
            # array of array of primitive types
            "field10": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
            },
            # array of array of complex types
            "field11": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field1": {
                                "type": "string",
                            },
                            "field2": {
                                "type": "integer",
                            },
                            "field3": {
                                "type": "boolean",
                            },
                        },
                    },
                },
            },
        }
    )

    assert [
        SchemaDatasetFacetFields(name="field2", type="int", description=None, fields=[]),
        SchemaDatasetFacetFields(name="field3", type="float", description=None, fields=[]),
        SchemaDatasetFacetFields(name="field4", type="boolean", description=None, fields=[]),
        SchemaDatasetFacetFields(name="field5", type="string", description=None, fields=[]),
        SchemaDatasetFacetFields(name="field6", type="date", description=None, fields=[]),
        SchemaDatasetFacetFields(
            name="field7",
            type="struct",
            description=None,
            fields=[SchemaDatasetFacetFields(name="field3", type="string", description=None, fields=[])],
        ),
        SchemaDatasetFacetFields(
            name="field8",
            type="array",
            description=None,
            fields=[SchemaDatasetFacetFields(name="_element", type="string", description=None, fields=[])],
        ),
        SchemaDatasetFacetFields(
            name="field9",
            type="array",
            description=None,
            fields=[
                SchemaDatasetFacetFields(
                    name="_element",
                    type="struct",
                    description=None,
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="string", description=None, fields=[])
                    ],
                )
            ],
        ),
        SchemaDatasetFacetFields(
            name="field10",
            type="array",
            description=None,
            fields=[
                SchemaDatasetFacetFields(
                    name="_element",
                    type="array",
                    description=None,
                    fields=[
                        SchemaDatasetFacetFields(name="_element", type="string", description=None, fields=[])
                    ],
                )
            ],
        ),
        SchemaDatasetFacetFields(
            name="field11",
            type="array",
            description=None,
            fields=[
                SchemaDatasetFacetFields(
                    name="_element",
                    type="array",
                    description=None,
                    fields=[
                        SchemaDatasetFacetFields(
                            name="_element",
                            type="struct",
                            description=None,
                            fields=[
                                SchemaDatasetFacetFields(
                                    name="field1", type="string", description=None, fields=[]
                                ),
                                SchemaDatasetFacetFields(
                                    name="field2", type="int", description=None, fields=[]
                                ),
                                SchemaDatasetFacetFields(
                                    name="field3", type="boolean", description=None, fields=[]
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
    ] == result
