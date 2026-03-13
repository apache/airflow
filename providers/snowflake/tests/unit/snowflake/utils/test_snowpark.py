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

pytest.importorskip("snowflake-snowpark-python")

from airflow.providers.snowflake.utils.snowpark import inject_session_into_op_kwargs


@pytest.mark.parametrize(
    ("func", "expected_injected"),
    [
        (lambda x: x, False),
        (lambda: 1, False),
        (lambda session: 1, True),
        (lambda session, x: x, True),
        (lambda x, session: 2 * x, True),
    ],
)
def test_inject_session_into_op_kwargs(func, expected_injected):
    result = inject_session_into_op_kwargs(func, {}, None)
    assert ("session" in result) == expected_injected
