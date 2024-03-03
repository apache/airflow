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

from airflow.api_connexion.endpoints.update_mask import extract_update_mask_data
from airflow.api_connexion.exceptions import BadRequest


class TestUpdateMask:
    def test_should_extract_data(self):
        non_update_fields = ["field_1"]
        update_mask = ["field_2"]
        data = {
            "field_1": "value_1",
            "field_2": "value_2",
            "field_3": "value_3",
        }
        data = extract_update_mask_data(update_mask, non_update_fields, data)
        assert data == {"field_2": "value_2"}

    def test_update_forbid_field_should_raise_exception(self):
        non_update_fields = ["field_1"]
        update_mask = ["field_1", "field_2"]
        data = {
            "field_1": "value_1",
            "field_2": "value_2",
            "field_3": "value_3",
        }
        with pytest.raises(BadRequest):
            extract_update_mask_data(update_mask, non_update_fields, data)

    def test_update_unknown_field_should_raise_exception(self):
        non_update_fields = ["field_1"]
        update_mask = ["field_2", "field_3"]
        data = {
            "field_1": "value_1",
            "field_2": "value_2",
        }
        with pytest.raises(BadRequest):
            extract_update_mask_data(update_mask, non_update_fields, data)
