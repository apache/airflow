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
from urllib.parse import urlparse

from jinja2 import Environment

from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.context import Context

log = logging.getLogger(__name__)


def any(result):
    return result


def is_datetime(result):
    try:
        x = parse(result)  # noqa
        return "true"
    except:  # noqa
        pass
    return "false"


def is_uuid(result):
    try:
        uuid.UUID(result)  # noqa
        return "true"
    except:  # noqa
        pass
    return "false"


def env_var(var: str, default: str | None = None) -> str:
    """The env_var() function. Return the environment variable named 'var'.
    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = f"Env var required but not provided: '{var}'"
        raise Exception(msg)


def not_match(result, pattern) -> str:
    if pattern in result:
        raise Exception(f"Found {pattern} in {result}")
    return "true"


def url_scheme_authority(url) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def url_path(url) -> str:
    return urlparse(url).path


def setup_jinja() -> Environment:
    env = Environment()
    env.globals["any"] = any
    env.globals["is_datetime"] = is_datetime
    env.globals["is_uuid"] = is_uuid
    env.globals["env_var"] = env_var
    env.globals["not_match"] = not_match
    env.filters["url_scheme_authority"] = url_scheme_authority
    env.filters["url_path"] = url_path
    return env


env = setup_jinja()


def match(expected, result) -> bool:
    """
    Check if result is "equal" to expected value. Omits keys not specified in expected value
    and resolves any jinja templates found.
    """
    if isinstance(expected, dict):
        # Take a look only at keys present at expected dictionary
        for k, v in expected.items():
            if k not in result:
                log.error("Key %s not in received event %s\nExpected %s", k, result, expected)
                return False
            if not match(v, result[k]):
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
            if not match(x, result[i]):
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
            rendered = env.from_string(expected).render(result=result)
            if rendered == "true" or rendered == result:
                return True
            log.error("Rendered value %s does not equal 'true' or %s", rendered, result)
            return False
        elif expected != result:
            log.error("Expected value %s does not equal result %s", expected, result)
            return False
    elif expected != result:
        log.error("Object of type %s: %s does not match %s", type(expected), expected, result)
        return False
    return True


class OpenlineageTestOperator(BaseOperator):
    """Operator for testing purposes.
    It compares expected event templates set on initialization with ones emitted by OpenLineage integration
    and stored in Variables by VariableTransport.
    :param event_templates: dictionary where key is the key used by VariableTransport in format of
        <DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>, and value is event template (fragment)
         that need to be in received events.
    :raises: ValueError if the received events do not match with expected ones.
    """

    def __init__(self, event_templates: dict[str, dict], **kwargs):
        super().__init__(**kwargs)
        self.event_templates = event_templates

    def execute(self, context: Context):
        for key, template in self.event_templates.items():
            send_event = Variable.get(key=key)
            self.log.error("Events: %s", send_event)
            if send_event:
                self.log.error("Events: %s, %s, %s", send_event, len(send_event), type(send_event))
            if not match(template, json.loads(send_event)):
                raise ValueError("Event received does not match one specified in test")
