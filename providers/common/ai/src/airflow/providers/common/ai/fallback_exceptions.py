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
"""
The error a failed ``fallback_conn_ids`` chain raises.

Kept apart from :mod:`airflow.providers.common.ai.exceptions`, which must stay importable without
loading pydantic-ai: this class subclasses pydantic-ai's ``FallbackExceptionGroup``.
"""

from __future__ import annotations

from pydantic_ai.exceptions import FallbackExceptionGroup


class FallbackChainExhaustedError(FallbackExceptionGroup):
    """
    Raised when every connection of a ``fallback_conn_ids`` chain failed.

    Subclasses pydantic-ai's ``FallbackExceptionGroup``, so ``except`` clauses and
    ``RetryRule(exception=FallbackExceptionGroup)`` written for it keep matching. The
    operator raises it ``from`` the group the chain raised, which stays ``__cause__``.

    ``replayed_steps`` and ``recorded_steps`` are what the durable journal replayed and recorded
    in this attempt before the chain failed, summed over every step kind, so a retry policy can
    tell an attempt that advanced the run from one that failed where it started. They are
    ``None`` when the run was not durable.
    """

    replayed_steps: int | None = None
    recorded_steps: int | None = None

    @property
    def member_errors(self) -> tuple[Exception, ...]:
        """The error each connection raised, in chain order."""
        return tuple(self.exceptions)

    @classmethod
    def from_group(
        cls,
        group: FallbackExceptionGroup,
        *,
        replayed_steps: int | None = None,
        recorded_steps: int | None = None,
    ) -> FallbackChainExhaustedError:
        """Build the error from the group the chain raised, keeping its attempts and members."""
        error = cls(group.message, list(group.exceptions))
        error.attempts = group.attempts
        error.replayed_steps = replayed_steps
        error.recorded_steps = recorded_steps
        return error
