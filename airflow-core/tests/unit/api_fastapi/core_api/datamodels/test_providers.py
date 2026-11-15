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
from importlib.resources import files as resource_files

from airflow.api_fastapi.core_api.datamodels.providers import ProviderInfoResponse

# Added to the provider metadata when the distribution is built, so provider_info.schema.json
# declares no property for it.
BUILD_TIME_KEYS = {"documentation-url"}


def get_declared_provider_info_keys() -> set[str]:
    """Return the provider metadata keys ``ProviderInfoResponse`` reads."""
    keys: set[str] = set()
    for name, field in ProviderInfoResponse.model_fields.items():
        alias = field.validation_alias
        keys.add(alias if isinstance(alias, str) else name)
    return keys


def get_supported_schema_keys() -> set[str]:
    """Return the non-deprecated top-level properties of ``provider_info.schema.json``."""
    schema = json.loads((resource_files("airflow") / "provider_info.schema.json").read_text())
    return {key for key, spec in schema["properties"].items() if "deprecated" not in spec}


def test_provider_info_response_declares_every_schema_key():
    """
    Keys ``ProviderInfoResponse`` does not declare are dropped from the API response without
    raising, so a key added to ``provider_info.schema.json`` has to fail here rather than go
    missing from ``GET /providers/{package_name}`` unnoticed.
    """
    assert get_declared_provider_info_keys() - BUILD_TIME_KEYS == get_supported_schema_keys()
