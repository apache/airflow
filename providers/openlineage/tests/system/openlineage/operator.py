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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from dateutil.parser import parse
from jinja2 import Environment

from airflow.providers.common.compat.sdk import BaseOperator, Variable

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
    """
    if isinstance(expected, dict):
        # Take a look only at keys present at expected dictionary
        if not isinstance(result, dict):
            log.error("Not a dict: %s\nExpected %s", result, expected)
            return False
        for k, v in expected.items():
            if k not in result:
                log.error("Key %s not in received event %s\nExpected %s", k, result, expected)
                return False
            if not match(v, result[k], env):
                log.error(
                    "For key %s, expected value %s not equals received %s\nExpected: %s, request: %s",
                    k,
                    v,
                    result[k],
                    expected,
                    result,
                )
                return False
    elif isinstance(expected, list):
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
            # Evaluate jinja: in some cases, we want to check only if key exists, or if
            # value has the right type
            try:
                rendered = env.from_string(expected).render(result=result)
            except ValueError as e:
                log.error("Error rendering jinja template %s: %s", expected, e)
                return False
            if str(rendered).lower() == "true" or rendered == result:
                return True
            log.error("Rendered value %s does not equal 'true' or %s", rendered, result)
            return False
        if expected != result:
            log.error("Expected value %s does not equal result %s", expected, result)
            return False
    elif expected != result:
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

    :param event_templates: dictionary where key is the key used by VariableTransport in format of <DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>, and value is event template (fragment) that need to be in received events.
    :param file_path: alternatively, file_path pointing to file with event templates will be used
    :param env: jinja environment used to render event templates
    :param allow_duplicate_events_regex: regex pattern; keys matching it are allowed to have multiple events.
    :param clear_variables: if set to True, clears only variables to be checked after all events are checked or if any check fails
    :raises: ValueError if the received events do not match with expected ones.
    """

    def __init__(
        self,
        event_templates: dict[str, dict] | None = None,
        file_path: str | None = None,
        env: Environment = setup_jinja(),
        allow_duplicate_events_regex: str | None = None,
        clear_variables: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.event_templates = event_templates
        self.file_path = file_path
        self.env = env
        self.allow_duplicate_events_regex = allow_duplicate_events_regex
        self.clear_variables = clear_variables
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
                # Just a single event per job and event type is loaded as this is the most common scenario
                key = event["job"]["name"] + ".event." + event["eventType"].lower()
                self.event_templates[key] = event
        try:
            for key, template in self.event_templates.items():  # type: ignore[union-attr]
                log.info("Checking key: `%s`", key)
                actual_events = Variable.get(key=key, deserialize_json=True)
                self.log.info(
                    "Events: len=`%s`, type=`%s`, value=%s",
                    len(actual_events),
                    type(actual_events),
                    actual_events,
                )
                if len(actual_events) == 0:
                    raise ValueError(f"No event for key {key}")
                if len(actual_events) != 1:
                    regex = self.allow_duplicate_events_regex
                    if regex is None or not re.fullmatch(regex, key):
                        raise ValueError(f"Expected one event for key {key}, got {len(actual_events)}")
                # Last event is checked against the template, this will allow to f.e. check change in try_num
                if not match(template, json.loads(actual_events[-1]), self.env):
                    raise ValueError("Event received does not match one specified in test")
        finally:
            if self.clear_variables:
                for key in self.event_templates:  # type: ignore[union-attr]
                    log.info("Removing variable `%s`", key)
                    Variable.delete(key=key)
