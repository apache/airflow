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

from pydantic_ai.exceptions import FallbackExceptionGroup

from airflow.providers.common.ai.fallback_exceptions import FallbackChainExhaustedError


class TestFromGroup:
    def test_keeps_message_members_and_journal_progress(self):
        members = [ValueError("first"), RuntimeError("second")]
        group = FallbackExceptionGroup("All models from FallbackModel failed", members)

        error = FallbackChainExhaustedError.from_group(group, replayed_steps=2, recorded_steps=1)

        assert error.message == group.message
        assert error.member_errors == tuple(members)
        assert error.replayed_steps == 2
        assert error.recorded_steps == 1

    def test_group_without_attempts_yields_empty_attempts(self):
        # Stands in for the group pydantic-ai-slim raised before 2.55.0, which had no ``attempts``.
        group = ExceptionGroup("failed", [ValueError("x")])

        error = FallbackChainExhaustedError.from_group(group)  # type: ignore[arg-type]

        assert error.attempts == ()
