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

import pytest
from pydantic import ValidationError

from airflow.api_fastapi.execution_api.datamodels.dagrun import ClearDagRunPayload


class TestClearDagRunPayload:
    def test_only_failed_defaults_to_false(self):
        """Omitting only_failed yields False so the route performs a whole-run clear."""
        payload = ClearDagRunPayload()
        assert payload.only_failed is False

    @pytest.mark.parametrize("value", [True, False])
    def test_only_failed_accepts_bool(self, value):
        payload = ClearDagRunPayload(only_failed=value)
        assert payload.only_failed is value

    def test_only_failed_rejects_non_bool(self):
        with pytest.raises(ValidationError):
            ClearDagRunPayload(only_failed="not-a-bool")
