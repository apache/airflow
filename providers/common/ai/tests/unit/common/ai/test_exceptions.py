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

from airflow.providers.common.ai.exceptions import (
    HITLMaxIterationsError,
    LLMFileAnalysisError,
    LLMFileAnalysisLimitExceededError,
    LLMFileAnalysisMultimodalRequiredError,
    LLMFileAnalysisUnsupportedFormatError,
    ManagedAgentInvocationError,
)
from airflow.providers.common.compat.sdk import AirflowException

EXCEPTION_CASES = [
    (HITLMaxIterationsError, AirflowException),
    (LLMFileAnalysisError, ValueError),
    (LLMFileAnalysisUnsupportedFormatError, LLMFileAnalysisError),
    (LLMFileAnalysisLimitExceededError, LLMFileAnalysisError),
    (LLMFileAnalysisMultimodalRequiredError, LLMFileAnalysisUnsupportedFormatError),
    (ManagedAgentInvocationError, RuntimeError),
]


@pytest.mark.parametrize(("exc_cls", "expected_base"), EXCEPTION_CASES)
def test_exception_bases_and_message(exc_cls, expected_base):
    message = "test-message"
    exc = exc_cls(message)

    assert isinstance(exc, expected_base)
    assert str(exc) == message


def test_llm_file_analysis_exception_chain():
    exc = LLMFileAnalysisMultimodalRequiredError("need multimodal")

    assert isinstance(exc, LLMFileAnalysisUnsupportedFormatError)
    assert isinstance(exc, LLMFileAnalysisError)
    assert isinstance(exc, ValueError)
