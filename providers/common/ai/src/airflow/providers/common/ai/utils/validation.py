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
"""Validation helpers for common.ai decorators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def validate_prompt(value: Any, *, decorator_name: str) -> None:
    """
    Validate the prompt returned by a decorator's python_callable.

    Accepted (mirrors pydantic-ai's ``Agent.run_sync`` user_prompt):
      - non-empty, non-whitespace ``str``
      - non-empty ``Sequence`` (other than ``str``/``bytes``/``bytearray``)
        of pydantic-ai ``UserContent`` items; item-level validation is
        delegated to pydantic-ai at ``Agent.run_sync`` time.

    Raises ``TypeError`` with an actionable message on any other shape.
    """
    if isinstance(value, str):
        if not value.strip():
            raise TypeError(
                f"The returned value from the {decorator_name} callable must be "
                f"a non-empty string or a non-empty Sequence[UserContent]."
            )
        return
    if isinstance(value, (bytes, bytearray)):
        raise TypeError(
            f"The returned value from the {decorator_name} callable must be "
            f"str or Sequence[UserContent], not {type(value).__name__}."
        )
    if isinstance(value, Sequence):
        if len(value) == 0:
            raise TypeError(
                f"The returned value from the {decorator_name} callable must be "
                f"a non-empty string or a non-empty Sequence[UserContent]."
            )
        for index, item in enumerate(value):
            if isinstance(item, (bytes, bytearray)):
                raise TypeError(
                    f"{decorator_name}: Sequence prompt item at index {index} is "
                    f"{type(item).__name__}; raw bytes are not a valid UserContent "
                    f"member. Wrap bytes in pydantic-ai's BinaryContent or upload "
                    f"to object storage and pass an ImageUrl/AudioUrl/DocumentUrl."
                )
        return
    raise TypeError(
        f"The returned value from the {decorator_name} callable must be "
        f"str or Sequence[UserContent], got {type(value).__name__}."
    )


def reject_sequence_with_unsupported_feature(
    value: Any,
    *,
    decorator_name: str,
    feature_name: str,
    feature_enabled: bool,
) -> None:
    """
    Preflight check raised before the agent runs.

    Raises ``TypeError`` when *value* is a non-string Sequence and
    *feature_enabled* is True. Used to fail fast on combinations
    (e.g., ``enable_hitl_review=True`` + Sequence prompt) that would
    otherwise fail later -- after the LLM call -- when the downstream
    HITL/approval consumer tries to stringify the prompt.
    """
    if feature_enabled and not isinstance(value, str):
        raise TypeError(
            f"{decorator_name}: Sequence[UserContent] prompts are not supported "
            f"with {feature_name}=True. Return a str prompt, or disable "
            f"{feature_name}."
        )
