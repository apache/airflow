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

from unittest.mock import patch

from airflow.providers.common.ai.utils import tool_definition
from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs


def test_returns_kwarg_when_supported():
    with patch.object(tool_definition, "_SUPPORTS_RETURN_SCHEMA", True):
        assert return_schema_kwargs({"type": "string"}) == {"return_schema": {"type": "string"}}


def test_returns_empty_when_unsupported():
    with patch.object(tool_definition, "_SUPPORTS_RETURN_SCHEMA", False):
        assert return_schema_kwargs({"type": "string"}) == {}
