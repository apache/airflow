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

from collections.abc import Callable
from typing import Any

import pytest
from tenacity import stop_after_attempt, wait_incrementing

INVALID_RETRY_ARGS_PATTERN = "does not support non-serializable databricks_retry_args when deferrable=True"
UNSUPPORTED_RETRY_ARGS = [
    pytest.param({"wait": wait_incrementing(start=1, increment=1, max=3)}, id="wait_incrementing"),
    pytest.param({"stop": stop_after_attempt(3)}, id="stop_after_attempt"),
]


def assert_invalid_retry_args_raises(invoke: Callable[[], Any]) -> None:
    with pytest.raises(ValueError, match=INVALID_RETRY_ARGS_PATTERN):
        invoke()
