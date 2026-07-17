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
Pluggable retry policies for Airflow tasks.

A ``retry_policy`` parameter on any task or operator evaluates the actual
exception at failure time and returns a :class:`RetryDecision` that can
override the retry/fail decision and the retry delay.
"""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

log = logging.getLogger(__name__)

__all__ = [
    "ExceptionRetryPolicy",
    "RetryAction",
    "RetryDecision",
    "RetryPolicy",
    "RetryRule",
]


class RetryAction(Enum):
    """What should happen after a failure."""

    RETRY = "retry"
    """Retry the task.

    The retry is still subject to the task's ``retries`` count -- the policy
    can fail a task earlier but cannot extend past the configured maximum.
    When all retries are exhausted, RETRY behaves identically to DEFAULT.
    """

    FAIL = "fail"
    """Fail immediately, skip remaining retries."""

    DEFAULT = "default"
    """Fall through to standard retry logic."""


@dataclass(frozen=True)
class RetryDecision:
    """The result of evaluating a :class:`RetryPolicy`."""

    action: RetryAction = RetryAction.DEFAULT
    retry_delay: timedelta | None = None
    """Override delay for this specific retry.  ``None`` uses the task's default."""
    reason: str | None = None
    """Human-readable reason, logged and stored on the task instance."""

    @classmethod
    def fail(cls, reason: str | None = None) -> RetryDecision:
        """Don't retry.  Mark as failed."""
        return cls(action=RetryAction.FAIL, reason=reason)

    @classmethod
    def retry(cls, delay: timedelta | None = None, reason: str | None = None) -> RetryDecision:
        """Retry, optionally with a custom delay."""
        return cls(action=RetryAction.RETRY, retry_delay=delay, reason=reason)

    @classmethod
    def default(cls) -> RetryDecision:
        """Use the task's standard retry behaviour."""
        return cls(action=RetryAction.DEFAULT)


class RetryPolicy(abc.ABC):
    """
    Base class for retry policies.

    Subclass this for custom retry logic.  The :meth:`evaluate` method runs
    in the **task worker process** and has full access to the exception object.
    The scheduler never calls this; it only sees the resulting state and
    optional delay override.
    """

    @abc.abstractmethod
    def evaluate(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None = None,
    ) -> RetryDecision:
        """
        Decide whether and how to retry given the failure.

        .. note::
           ``AirflowFailException`` and ``AirflowSensorTimeout`` always fail
           the task immediately.  The retry policy is never consulted for
           these exceptions.

        :param exception: The exception that caused the task failure.
        :param try_number: Current try number (1-based).
        :param max_tries: Maximum tries configured on the task.
        :param context: Airflow task context (may be ``None``).
        """

    def serialize(self) -> dict[str, Any]:
        """
        Serialize this policy for DAG serialization.

        Must return a JSON-serializable dict.  The default implementation
        raises :class:`NotImplementedError`; built-in policies like
        :class:`ExceptionRetryPolicy` provide their own implementation.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement serialize()")

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> RetryPolicy:
        """Deserialize from a JSON dict produced by :meth:`serialize`."""
        raise NotImplementedError


ExceptionSpec = type[BaseException] | str
"""A single exception class or dotted import path string."""


@dataclass
class RetryRule:
    """
    A single exception-to-behaviour mapping.

    :param exception: Exception class, dotted import path string, or a
        **list** of either.  When a list is provided, the rule matches if
        the exception matches *any* entry in the list.

        .. code-block:: python

            # Single exception
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY)

            # Multiple exceptions sharing the same behaviour
            RetryRule(
                exception=[ConnectionError, TimeoutError, "requests.exceptions.HTTPError"],
                action=RetryAction.RETRY,
                retry_delay=timedelta(seconds=30),
            )

    :param action: What to do when this rule matches.
    :param retry_delay: Override the retry delay for this specific error.
    :param reason: Human-readable reason logged with the decision.
    :param match_subclasses: When ``True`` (default), uses ``isinstance``
        for matching.  When ``False``, only exact type matches count.
    """

    exception: ExceptionSpec | list[ExceptionSpec]
    action: RetryAction = RetryAction.RETRY
    retry_delay: timedelta | None = None
    reason: str | None = None
    match_subclasses: bool = True

    def __post_init__(self) -> None:
        # Normalise to a list for uniform handling.
        specs = self.exception if isinstance(self.exception, list) else [self.exception]
        for spec in specs:
            if isinstance(spec, str):
                if not spec or "." not in spec:
                    raise ValueError(
                        f"RetryRule exception string must be a dotted import path "
                        f"(e.g. 'builtins.ValueError'), got {spec!r}"
                    )
                try:
                    from airflow.sdk.module_loading import import_string

                    import_string(spec)
                except (ImportError, AttributeError):
                    log.warning(
                        "RetryRule: exception class %r could not be imported at parse time. "
                        "It may resolve on the worker, but check for typos.",
                        spec,
                    )

    @property
    def _specs(self) -> list[ExceptionSpec]:
        """Normalised list of exception specs."""
        return self.exception if isinstance(self.exception, list) else [self.exception]

    @property
    def exception_path(self) -> str:
        """Dotted import path(s) of the exception class(es)."""
        paths = []
        for spec in self._specs:
            if isinstance(spec, str):
                paths.append(spec)
            else:
                paths.append(f"{spec.__module__}.{spec.__qualname__}")
        return paths[0] if len(paths) == 1 else ",".join(paths)

    def matches(self, exc: BaseException) -> bool:
        """Return ``True`` if *exc* matches any exception in this rule."""
        resolved = self._resolve_exception_classes()
        if not resolved:
            return False
        if self.match_subclasses:
            return isinstance(exc, tuple(resolved))
        return type(exc) in resolved

    _resolved_classes: list[type[BaseException]] | None | object = field(
        default=object, init=False, repr=False, compare=False
    )

    def _resolve_exception_classes(self) -> list[type[BaseException]]:
        """Resolve all exception specs to classes, caching the result."""
        if self._resolved_classes is not object:
            return self._resolved_classes or []  # type: ignore[return-value]

        from airflow.sdk.module_loading import import_string

        resolved: list[type[BaseException]] = []
        for spec in self._specs:
            if isinstance(spec, type):
                resolved.append(spec)
            else:
                try:
                    resolved.append(import_string(spec))
                except (ImportError, AttributeError):
                    log.warning("Could not import exception class %r, it will not match", spec)

        self._resolved_classes = resolved if resolved else None
        return resolved

    # -- Serialization -------------------------------------------------------

    def serialize(self) -> dict[str, Any]:
        specs = self._specs
        exc_paths = []
        for spec in specs:
            if isinstance(spec, str):
                exc_paths.append(spec)
            else:
                exc_paths.append(f"{spec.__module__}.{spec.__qualname__}")

        result: dict[str, Any] = {
            "exception": exc_paths if len(exc_paths) > 1 else exc_paths[0],
            "action": self.action.value,
            "match_subclasses": self.match_subclasses,
        }
        if self.retry_delay is not None:
            result["retry_delay"] = self.retry_delay.total_seconds()
        if self.reason is not None:
            result["reason"] = self.reason
        return result

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> RetryRule:
        return cls(
            exception=data["exception"],
            action=RetryAction(data["action"]),
            retry_delay=timedelta(seconds=data["retry_delay"]) if "retry_delay" in data else None,
            reason=data.get("reason"),
            match_subclasses=data.get("match_subclasses", True),
        )


class ExceptionRetryPolicy(RetryPolicy):
    """
    Declarative exception-to-behaviour mapping.

    Rules are evaluated in order; the first matching rule wins.
    If no rule matches, the *default* action applies.

    Example::

        retry_policy = ExceptionRetryPolicy(
            rules=[
                RetryRule(
                    exception="requests.exceptions.HTTPError",
                    action=RetryAction.RETRY,
                    retry_delay=timedelta(minutes=5),
                    reason="Likely rate limit, backing off",
                ),
                RetryRule(
                    exception="google.auth.exceptions.RefreshError",
                    action=RetryAction.FAIL,
                    reason="Auth failure, not retryable",
                ),
            ],
        )
    """

    def __init__(
        self,
        rules: list[RetryRule],
        default: RetryAction = RetryAction.DEFAULT,
    ) -> None:
        self.rules = rules
        self.default = default

    def evaluate(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None = None,
    ) -> RetryDecision:
        for rule in self.rules:
            if rule.matches(exception):
                return RetryDecision(
                    action=rule.action,
                    retry_delay=rule.retry_delay,
                    reason=rule.reason or f"Matched rule for {type(exception).__name__}",
                )
        return RetryDecision(action=self.default)

    # -- Serialization -------------------------------------------------------

    def serialize(self) -> dict[str, Any]:
        return {
            "__type": "ExceptionRetryPolicy",
            "rules": [r.serialize() for r in self.rules],
            "default": self.default.value,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> ExceptionRetryPolicy:
        rules = [RetryRule.deserialize(r) for r in data["rules"]]
        default = RetryAction(data.get("default", "default"))
        return cls(rules=rules, default=default)
