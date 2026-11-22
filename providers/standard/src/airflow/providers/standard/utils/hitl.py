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
Human-in-the-loop checks shared by ``HITLOperator`` and ``request_input()``, and the answers a resumed task replays.

Kept apart from :mod:`airflow.providers.standard.operators.hitl`, which cannot be imported before
Airflow 3.1, so that ``PythonOperator`` can import it on every supported Airflow version.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, NoReturn, TypedDict

from airflow.providers.standard.exceptions import HITLTimeoutError, HITLTriggerEventError
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk.definitions.param import Param, ParamsDict
else:
    from airflow.models.param import Param, ParamsDict  # type: ignore[no-redef]

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.sdk.execution_time.hitl import HITLUser

# The helpers below are shared by the HITL operators and PythonOperator, not public API.
__all__ = ["InputResponse"]


class InputResponse(TypedDict):
    """The answer to a :func:`~airflow.providers.standard.operators.hitl.request_input` call."""

    chosen_options: list[str]
    params_input: dict[str, Any]
    responded_at: datetime
    #: ``None`` when nobody answered and the request's defaults were applied at its timeout.
    responded_by_user: HITLUser | None
    timedout: bool


class AnswerLog:
    """
    The answers a resumed task has received in this try, by the key of the request each one answered.

    Each key is asked at most once per try, so an answer can never be handed to a later question that
    happens to share its key. The run that resumed with an answer (``latest``) must read it before it
    asks anything new or finishes; otherwise the key changed between runs, and asking again would
    never end.
    """

    def __init__(self, answers: dict[str, InputResponse], latest: str) -> None:
        self.answers = answers
        self.latest = latest
        self._read: set[str] = set()

    def take(self, key: str) -> InputResponse | None:
        """Return the answer to ``key``, or ``None`` when ``key`` has not been asked yet."""
        if key in self._read:
            raise ValueError(
                f"request_input() was called again with the key {key!r} after its answer was read. Each "
                "question gets one answer per try, so pass a key that is unique to each question, for "
                "example an agent's tool call id."
            )
        answer = self.answers.get(key)
        if answer is not None:
            self._read.add(key)
            return answer
        self.check_latest_read()
        return None

    def check_latest_read(self) -> None:
        """Refuse to go on when the answer this run resumed with was never read."""
        if self.latest not in self._read:
            raise RuntimeError(
                f"The answer to {self.latest!r} was never read: no request_input() call with that key "
                "ran after the task resumed. If the subject changes between runs, pass a fixed key=."
            )


answer_log_var: ContextVar[AnswerLog | None] = ContextVar("hitl_answer_log", default=None)


def validate_options(options: list[str]) -> None:
    """Refuse a request with no options."""
    if not options:
        raise ValueError('"options" cannot be empty.')


def validate_params(params: Mapping[str, Any]) -> None:
    """Refuse params that collide with the reserved ``_options`` key."""
    if "_options" in params:
        raise ValueError('"_options" is not allowed in params')


def validate_defaults(defaults: list[str] | None, options: list[str], multiple: bool) -> None:
    """Refuse defaults that are not options, or more than one default for a single-choice request."""
    if defaults is not None:
        if not set(defaults).issubset(options):
            raise ValueError(f'defaults "{defaults}" should be a subset of options "{options}"')

        if multiple is False and len(defaults) > 1:
            raise ValueError('More than one defaults given when "multiple" is set to False.')


def raise_for_error_event(event: Mapping[str, Any]) -> NoReturn:
    """Raise the error a resume event carries instead of an answer, such as a timeout without defaults."""
    if event["error_type"] == "timeout":
        raise HITLTimeoutError(event)

    raise HITLTriggerEventError(event)


def validate_chosen_options(chosen_options: list[str], options: list[str]) -> None:
    """Refuse an answer that chose something the request did not offer."""
    if diff := set(chosen_options) - set(options):
        raise ValueError(f"Responses {diff} not in {options}")


def validate_params_input(params_input: Mapping[str, Any], params: ParamsDict) -> None:
    """
    Refuse form input that does not match the request's params, and check each value against its schema.

    The values are written into ``params``, which raises if one fails its param's schema.
    """
    if params and params_input and set(params.keys()) ^ set(params_input):
        raise ValueError(f"params_input {params_input} does not match params {params}")

    for key, value in params_input.items():
        params[key] = value


def params_from_serialized(serialized: Mapping[str, Mapping[str, Any]]) -> ParamsDict:
    """Rebuild the params of a request from the form they were written to the Human-in-the-loop detail in."""
    return ParamsDict(
        {key: Param.deserialize(dict(value), Param.__version__) for key, value in serialized.items()}
    )
