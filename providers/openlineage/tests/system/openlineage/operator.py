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

import json
import logging
import os
import re
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from dateutil.parser import parse
from jinja2 import Environment

from airflow.providers.common.compat.sdk import BaseOperator, Variable

try:
    from airflow.sdk.exceptions import AirflowRuntimeError as NoVariableError  # AF3
except (ImportError, ModuleNotFoundError, AttributeError):
    NoVariableError = KeyError  # type: ignore[misc, assignment]  # AF2

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

log = logging.getLogger(__name__)


def any(result: Any) -> Any:
    return result


def is_datetime(result: Any) -> str:
    try:
        parse(result)
        return "true"
    except Exception:
        pass
    return "false"


def is_uuid(result: Any) -> str:
    try:
        uuid.UUID(result)
        return "true"
    except Exception:
        pass
    return "false"


def regex_match(result: Any, pattern: str) -> str:
    try:
        if re.match(pattern=pattern, string=result):
            return "true"
    except Exception:
        pass
    return "false"


def env_var(var: str, default: str | None = None) -> str:
    """
    Use this jinja method to access the environment variable named 'var'.

    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]
    if default is not None:
        return default
    raise ValueError(f"Env var required but not provided: '{var}'")


def not_match(result: str, pattern: str) -> str:
    if pattern in result:
        raise ValueError(f"Found {pattern} in {result}")
    return "true"


def url_scheme_authority(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def url_path(url: str) -> str:
    return urlparse(url).path


def setup_jinja() -> Environment:
    env = Environment()
    env.globals["any"] = any
    env.globals["is_datetime"] = is_datetime
    env.globals["is_uuid"] = is_uuid
    env.globals["regex_match"] = regex_match
    env.globals["env_var"] = env_var
    env.globals["not_match"] = not_match
    env.filters["url_scheme_authority"] = url_scheme_authority
    env.filters["url_path"] = url_path
    return env


def match(expected, result, env: Environment) -> bool:
    """
    Check if result is "equal" to expected value.

    Omits keys not specified in expected value and resolves any jinja templates found.

    Special sentinel values in expected dicts:

    ``None`` (JSON ``null``) means the key must not be present in the result.

    ``{"$optional": "<reason>", ...}`` means the key is optional. When the key is
    present, the remaining template keys are validated. When the key is absent,
    validation passes silently and the reason is logged.

    ``{"$optional": "<reason>", "$value": <list_template>}`` is the list-template
    form of the optional sentinel. It behaves the same way, but also skips validation
    when the actual value is an empty list. Use this form when the template value is a
    list rather than a dict. Dict templates embed their keys directly alongside
    ``"$optional"``.

    """
    if isinstance(expected, dict):
        # Only keys present in expected are checked — extra keys in the actual event are ignored.
        if not isinstance(result, dict):
            log.error("Not a dict: %s\nExpected %s", result, expected)
            return False
        for k, v in expected.items():
            # null sentinel: assert the key must NOT appear in the actual event.
            if v is None:
                if k in result:
                    log.error("Key %s should not be present in received event %s", k, result)
                    return False
                continue

            # $optional sentinel: skip validation if the key is absent or its value is an
            # empty list; otherwise unwrap and validate normally.
            # Two forms:
            #   dict template — {"$optional": "reason", "field": "value", ...}
            #     Strip "$optional" and treat the remaining keys as the template.
            #   list template — {"$optional": "reason", "$value": [...]}
            #     Use the "$value" list as the template.
            # Both forms also skip when the actual value is an empty list [], which covers
            # the case where a field is always present but may carry no items.
            if isinstance(v, dict) and isinstance(v.get("$optional"), str):
                reason = v["$optional"]
                actual_val = result.get(k)
                if k not in result or actual_val == []:
                    log.info("Optional key `%s` not present or empty (%s), skipping", k, reason)
                    continue
                v = v["$value"] if "$value" in v else {ik: iv for ik, iv in v.items() if ik != "$optional"}  # noqa: PLW2901

            # At this point v is a plain template (dict, list, or scalar) with no sentinels.
            if k not in result:
                log.error("Key %s not in received event %s\nExpected %s", k, result, expected)
                return False
            if not match(v, result[k], env):
                log.error(
                    "For key %s, expected value %s not equals received %s\n\nExpected: %s,\n\n request: %s",
                    k,
                    v,
                    result[k],
                    expected,
                    result,
                )
                return False
    elif isinstance(expected, list):
        # Lists must match exactly in length and order. Each element is compared recursively,
        # so nested sentinels (null, $optional) work inside list items too.
        if len(expected) != len(result):
            log.error("Length does not match: expected %d, result: %d", len(expected), len(result))
            return False
        for i, x in enumerate(expected):
            if not match(x, result[i], env):
                log.error(
                    "List not matched at %d\nexpected:\n%s\nresult: \n%s",
                    i,
                    json.dumps(x),
                    json.dumps(result[i]),
                )
                return False
    elif isinstance(expected, str):
        if "{{" in expected:
            # Jinja template: render with result as the template variable, then check whether
            # the rendered output is the string "true" (type/format validators like is_uuid,
            # is_datetime, regex_match return "true" on success) or equals result directly
            # (for expressions that transform the value, e.g. filters).
            try:
                rendered = env.from_string(expected).render(result=result)
            except ValueError as e:
                log.error("Error rendering jinja template %s: %s", expected, e)
                return False
            if str(rendered).lower() == "true" or rendered == result:
                return True
            log.error("Rendered value %s does not equal 'true' or %s", rendered, result)
            return False
        # Plain string: exact equality check.
        if expected != result:
            log.error("Expected value %s does not equal result %s", expected, result)
            return False
    elif expected != result:
        # Scalar (int, bool, float, …): exact equality check.
        log.error("Object of type %s: %s does not match %s", type(expected), expected, result)
        return False
    return True


class OpenLineageTestOperator(BaseOperator):
    """
    This operator is added for system testing purposes.

    It compares expected event templates set on initialization with ones emitted by OpenLineage integration
    and stored in Variables by VariableTransport.

    Note:
        If `clear_variables` is True, only the Airflow Variables listed in `event_templates`
        (or derived from `file_path`) will be deleted - those that are supposed to be checked by the Operator.
        We won't remove all Airflow Variables to avoid interfering with other instances of this Operator
        running in parallel on different DAGs. Running continuous system tests without clearing Variables
        may lead to leftover or growing Variables size. We recommend implementing a process to remove all
        Airflow Variables after all system tests have run to ensure a clean environment for each test run.

    :param event_templates: Dictionary where each key is the key used by VariableTransport,
        in the format ``<DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>``. Each value may be one of
        the following: a single event template dict, matched against the last received event;
        a list of event template dicts, matched pairwise against all received events after
        sorting with ``event_sort_fn`` when provided; or ``{"$absent": "<reason>"}``, which
        asserts that no events exist for that key. The ``"$absent"`` reason is logged and
        included in the error if events are unexpectedly found.
    :param file_path: alternatively, a path to a JSON file containing an array of event templates.
        Multiple events sharing the same ``job.name`` + ``eventType`` key are accumulated into a list.
        An event with ``"$absent": "<reason>"`` asserts the variable must not exist.
    :param env: jinja environment used to render event templates.
    :param event_count_assertions: dict mapping a regex pattern (matched against variable keys) to a
        count expression. Supported expressions: ``"N"``, ``"==N"``, ``">=N"``, ``"<=N"``.
        Applied to single-template keys only; list-template keys derive their count from the list length.
        Example: ``{".*\\.event\\.start": ">=2"}``.
    :param clear_variables: if True, deletes only the checked variables after all checks run (or on
        failure). Default True.
    :param fail_fast: if True, raise on the first failure. If False (default), collect all failures
        and raise a combined error at the end.
    :param event_sort_fn: optional callable ``(event: dict) -> Any`` used to sort actual events
        ascending before comparison. Useful for mapped tasks whose events arrive in non-deterministic
        order. When the template for a key is a list, sorting is required for pairwise comparison to
        be meaningful.

    Special sentinel values in expected dicts:

    ``None`` (JSON ``null``) means the key must not be present in the result.

    ``{"$optional": "<reason>", ...}`` means the key is optional. When the key is
    present, the remaining template keys are validated. When the key is absent,
    validation passes silently and the reason is logged.

    ``{"$optional": "<reason>", "$value": <list_template>}`` is the list-template
    form of the optional sentinel. It behaves the same way, but also skips validation
    when the actual value is an empty list. Use this form when the template value is a
    list rather than a dict. Dict templates embed their keys directly alongside
    ``"$optional"``.

    :raises: ValueError if the received events do not match with expected ones.
    """

    def __init__(
        self,
        event_templates: dict[str, dict | list] | None = None,
        file_path: str | None = None,
        env: Environment | None = None,
        event_count_assertions: dict[str, str | int] | None = None,
        clear_variables: bool = True,
        fail_fast: bool = False,
        event_sort_fn: Callable[[dict], Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.event_templates = event_templates
        self.file_path = file_path
        self.env = env or setup_jinja()
        self.event_count_assertions = event_count_assertions
        self.clear_variables = clear_variables
        self.fail_fast = fail_fast
        self.event_sort_fn = event_sort_fn
        if self.event_templates and self.file_path:
            raise ValueError("Can't pass both event_templates and file_path")

    def execute(self, context: Context) -> None:
        time.sleep(10)  # Wait for all variables to update properly
        if self.file_path is not None:
            self.event_templates = {}
            self.log.info("Reading OpenLineage event templates from file `%s`", self.file_path)
            with open(self.file_path) as f:
                events = json.load(f)
            for event in events:
                key = event["job"]["name"] + ".event." + event["eventType"].lower()
                if isinstance(event.get("$absent"), str):
                    self.event_templates[key] = {"$absent": event["$absent"]}
                else:
                    existing = self.event_templates.get(key)
                    if existing is None:
                        self.event_templates[key] = event
                    elif isinstance(existing, list):
                        existing.append(event)
                    else:
                        self.event_templates[key] = [existing, event]
        errors: dict[str, str] = {}
        try:
            for key, template in self.event_templates.items():  # type: ignore[union-attr]
                log.info("Checking key: `%s`", key)
                try:
                    self._check_key(key, template)
                except ValueError as e:
                    if self.fail_fast:
                        raise
                    errors[key] = str(e)
        finally:
            if self.clear_variables:
                for key in self.event_templates:  # type: ignore[union-attr]
                    log.info("Removing variable `%s`", key)
                    Variable.delete(key=key)
        if errors:
            failures = "\n".join(f"  [{key}] {error}" for key, error in errors.items())
            raise ValueError(f"Event check failures:\n{failures}")

    def _check_key(self, key: str, template: Any) -> None:
        if isinstance(template, dict) and "$absent" in template:
            reason = template["$absent"]
            try:
                actual = Variable.get(key=key, deserialize_json=True)
                raise ValueError(f"Expected no events for key {key} ({reason}), got {len(actual)}")
            except NoVariableError:
                log.info("Key `%s` absent as expected (%s).", key, reason)
                return

        actual_events = Variable.get(key=key, deserialize_json=True)
        if not isinstance(actual_events, list):
            raise ValueError(
                f"Variable {key} does not contain a list of events, got {type(actual_events).__name__}"
            )
        self.log.info(
            "Events: len=`%s`, value=%s",
            len(actual_events),
            actual_events,
        )
        if self.event_sort_fn:
            actual_events = sorted(
                actual_events,
                key=lambda e: self.event_sort_fn(json.loads(e)),  # type: ignore[misc]
            )

        default_count = str(len(template)) if isinstance(template, list) else "1"
        self._assert_event_count(key, len(actual_events), default=default_count)

        if isinstance(template, list):
            # Multiple expected events: compare each one after sorting.
            for i, (tmpl, evt_str) in enumerate(zip(template, actual_events)):
                if not match(tmpl, json.loads(evt_str), self.env):
                    raise ValueError(f"Event at index {i} does not match template for key `{key}`")
        else:
            # Last event is checked against the template
            if not match(template, json.loads(actual_events[-1]), self.env):
                raise ValueError(f"Event received does not match one specified in test for key `{key}`")

    def _assert_event_count(self, key: str, actual: int, default: str = "1") -> None:
        """
        Resolve the expected count expression for key and assert actual satisfies it.

        Iterates event_count_assertions in definition order; the first regex that fully
        matches key wins. If no pattern matches, falls back to default ("1" for single
        templates, str(len(template)) for list templates — supplied by the caller).
        Delegates the numeric check to _check_count_expr.
        """
        expr = default
        if self.event_count_assertions:
            for pattern, count_expr in self.event_count_assertions.items():
                if re.fullmatch(pattern, key):
                    expr = str(count_expr)
                    break
        self._check_count_expr(key, actual, expr)

    @staticmethod
    def _check_count_expr(key: str, actual: int, expr: str) -> None:
        """
        Assert that actual satisfies the count expression expr.

        Supported expressions (leading/trailing whitespace is stripped):
          "N"   or "==N"  — exactly N events
          ">=N"           — at least N events
          "<=N"           — at most N events

        Raises ValueError if the assertion fails or the expression is not recognized.
        """
        expr = expr.strip()
        if expr.startswith(">="):
            n = int(expr[2:])
            if actual < n:
                raise ValueError(f"Expected >={n} events for key {key}, got {actual}")
        elif expr.startswith("<="):
            n = int(expr[2:])
            if actual > n:
                raise ValueError(f"Expected <={n} events for key {key}, got {actual}")
        elif expr.startswith("=="):
            n = int(expr[2:])
            if actual != n:
                raise ValueError(f"Expected =={n} events for key {key}, got {actual}")
        elif expr.isdigit():
            n = int(expr)
            if actual != n:
                raise ValueError(f"Expected {n} events for key {key}, got {actual}")
        else:
            raise ValueError(f"Invalid count expression '{expr}' for key {key}")
