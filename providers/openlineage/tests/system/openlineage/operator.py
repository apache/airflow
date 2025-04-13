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
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from dateutil.parser import parse
from jinja2 import Environment

from airflow.models.operator import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.session import create_session

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context

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
    :param event_templates: dictionary where key is the key used by VariableTransport in format of <DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>, and value is event template (fragment) that need to be in received events.
    :param file_path: alternatively, file_path pointing to file with event templates will be used
    :param env: jinja environment used to render event templates
    :param allow_duplicate_events: if set to True, allows multiple events for the same key
    :param clear_variables: if set to True, clears all variables after checking events
    :raises: ValueError if the received events do not match with expected ones.
    """

    def __init__(
        self,
        event_templates: dict[str, dict] | None = None,
        file_path: str | None = None,
        env: Environment = setup_jinja(),
        allow_duplicate_events: bool = False,
        clear_variables: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.event_templates = event_templates
        self.file_path = file_path
        self.env = env
        self.multiple_events = allow_duplicate_events
        self.delete = clear_variables
        if self.event_templates and self.file_path:
            raise ValueError("Can't pass both event_templates and file_path")

    def execute(self, context: Context) -> None:
        if self.file_path is not None:
            self.event_templates = {}
            with open(self.file_path) as f:  # type: ignore[arg-type]
                events = json.load(f)
            for event in events:
                key = event["job"]["name"] + ".event." + event["eventType"].lower()
                self.event_templates[key] = event
        for key, template in self.event_templates.items():  # type: ignore[union-attr]
            send_event = Variable.get(key=key, deserialize_json=True)
            if len(send_event) == 0:
                raise ValueError(f"No event for key {key}")
            if len(send_event) != 1 and not self.multiple_events:
                raise ValueError(f"Expected one event for key {key}, got {len(send_event)}")
            self.log.info("Events: %s, %s, %s", send_event, len(send_event), type(send_event))
            if not match(template, json.loads(send_event[0]), self.env):
                raise ValueError("Event received does not match one specified in test")
        if self.delete:
            with create_session() as session:
                session.query(Variable).delete()
