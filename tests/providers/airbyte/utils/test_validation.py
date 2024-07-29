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

from airflow.providers.airbyte.utils.validation import is_connection_valid


def test_schema_validation_invalid():
    tests = {
        "missing syncCatalog": {
            "connection_response": {
                "destinationId": "test",
                "prefix": "test",
                "namespaceDefinition": "test",
            }
        },
        "missing destinationId": {
            "connection_response": {
                "prefix": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams": []},
            }
        },
        "missing namespaceDefinition": {
            "connection_response": {"destinationId": "test", "syncCatalog": {"streams": []}}
        },
        "missing streams": {
            "connection_response": {
                "destinationId": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams1": []},
            }
        },
        "missing stream": {
            "connection_response": {
                "destinationId": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams": [{"stream1": {}}]},
            }
        },
        "missing name": {
            "connection_response": {
                "destinationId": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams": [{"stream": {"name1": "test", "jsonSchema": {}}}]},
            }
        },
        "missing jsonSchema": {
            "connection_response": {
                "destinationId": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams": [{"stream": {"name": "test", "jsonSchema1": {}}}]},
            }
        },
        "missing properties for field": {
            "connection_response": {
                "destinationId": "test",
                "namespaceDefinition": "test",
                "syncCatalog": {"streams": [{"stream": {"name": "test", "jsonSchema": {"properties1": {}}}}]},
            }
        },
    }

    for test_name, test in tests.items():
        is_valid = is_connection_valid(test["connection_response"])

        assert is_valid is False, f"Test {test_name} failed"
