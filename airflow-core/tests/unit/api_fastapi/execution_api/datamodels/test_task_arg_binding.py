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

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import get_arg_bindings_adapter


@pytest.mark.parametrize(
    "binding",
    [
        pytest.param({"kind": "xcom", "task_id": "extract", "name": "row"}, id="named"),
        pytest.param({"kind": "xcom", "task_id": "extract", "element_index": 0, "spread": True}, id="spread"),
    ],
)
def test_binding_names_its_parameter_unless_spread(binding):
    get_arg_bindings_adapter().validate_python([binding])


@pytest.mark.parametrize(
    "binding",
    [
        pytest.param({"kind": "xcom", "task_id": "extract"}, id="unnamed"),
        pytest.param(
            {"kind": "xcom", "task_id": "extract", "name": "row", "spread": True}, id="named-spread"
        ),
    ],
)
def test_binding_rejects_name_and_spread_mismatch(binding):
    with pytest.raises(ValidationError, match="has a name unless it sets spread"):
        get_arg_bindings_adapter().validate_python([binding])
