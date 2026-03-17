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

from airflow.api_fastapi.core_api.services.ui.connections import HookMetaService


class TestMockOptional:
    def test_mock_optional_is_callable(self):
        """MockOptional instances must be callable to satisfy WTForms validator checks."""
        validator = HookMetaService.MockOptional()
        assert callable(validator)

    def test_mock_optional_call_is_noop(self):
        """Calling MockOptional should be a no-op (returns None)."""
        validator = HookMetaService.MockOptional()
        result = validator(None, None)
        assert result is None
