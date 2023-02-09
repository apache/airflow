#
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

from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState


def normalise_json_content(content, json_path: str = "json") -> str | bool | list | dict:
    """
    Normalize content or all values of content if it is a dict to a string. The
    function will throw if content contains non-string or non-numeric non-boolean types.
    The reason why we have this function is because the ``self.json`` field must be a
    dict with only string values. This is because ``render_template`` will fail
    for numerical values.

    The only one exception is when we have boolean values, they can not be converted
    to string type because databricks does not understand 'True' or 'False' values.
    """
    normalise = normalise_json_content
    if isinstance(content, (str, bool)):
        return content
    elif isinstance(
        content,
        (
            int,
            float,
        ),
    ):
        # Databricks can tolerate either numeric or string types in the API backend.
        return str(content)
    elif isinstance(content, (list, tuple)):
        return [normalise(e, f"{json_path}[{i}]") for i, e in enumerate(content)]
    elif isinstance(content, dict):
        return {k: normalise(v, f"{json_path}[{k}]") for k, v in list(content.items())}
    else:
        param_type = type(content)
        msg = f"Type {param_type} used for parameter {json_path} is not a number or a string"
        raise AirflowException(msg)


def validate_trigger_event(event: dict):
    """
    Validates correctness of the event
    received from :class:`~airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger`
    """
    keys_to_check = ["run_id", "run_page_url", "run_state"]
    for key in keys_to_check:
        if key not in event:
            raise AirflowException(f"Could not find `{key}` in the event: {event}")

    try:
        RunState.from_json(event["run_state"])
    except Exception:
        raise AirflowException(f'Run state returned by the Trigger is incorrect: {event["run_state"]}')


# Taken from PyHive
class ParamEscaper:
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = f"{_DATE_FORMAT} {_TIME_FORMAT}"

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise exc.ProgrammingError(f"Unsupported param format: {parameters}")

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("'", "''"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return f"'{formatted}'"

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        else:
            raise exc.ProgrammingError(f"Unsupported object {item}")
