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

from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType


class TestDataFusionPipelineType:
    @pytest.mark.parametrize(
        ("str_value", "expected_item"),
        [
            ("batch", DataFusionPipelineType.BATCH),
            ("stream", DataFusionPipelineType.STREAM),
        ],
    )
    def test_from_str(self, str_value, expected_item):
        assert DataFusionPipelineType.from_str(str_value) == expected_item

    def test_from_str_error(self):
        with pytest.raises(ValueError, match="Invalid value 'non-existing value'."):
            DataFusionPipelineType.from_str("non-existing value")
