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

from airflow.providers.anthropic.exceptions import (
    AnthropicAgentSessionError,
    AnthropicAgentSessionTimeout,
    AnthropicBatchJobError,
    AnthropicBatchTimeout,
    AnthropicError,
)


@pytest.mark.parametrize(
    "exc",
    [
        AnthropicError,
        AnthropicBatchJobError,
        AnthropicBatchTimeout,
        AnthropicAgentSessionError,
        AnthropicAgentSessionTimeout,
    ],
)
def test_provider_errors_share_base_and_are_not_airflow_exceptions(exc):
    # Every provider error is catchable via the AnthropicError base and is deliberately
    # NOT an AirflowException subclass (the no-new-AirflowException direction).
    assert issubclass(exc, AnthropicError)
    assert not any(base.__name__ == "AirflowException" for base in exc.__mro__)
