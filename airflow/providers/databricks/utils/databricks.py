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

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState


def normalise_json_content(content, json_path: str = "json") -> str | bool | list | dict:
    """
    Normalize content or all values of content if it is a dict to a string.

    The function will throw if content contains non-string or non-numeric non-boolean
    types. The reason why we have this function is because the ``self.json`` field
    must be a dict with only string values. This is because ``render_template`` will
    fail for numerical values.

    The only one exception is when we have boolean values, they can not be converted
    to string type because databricks does not understand 'True' or 'False' values.
    """
    normalise = normalise_json_content
    if isinstance(content, (str, bool)):
        return content
    elif isinstance(content, (int, float)):
        # Databricks can tolerate either numeric or string types in the API backend.
        return str(content)
    elif isinstance(content, (list, tuple)):
        return [normalise(e, f"{json_path}[{i}]") for i, e in enumerate(content)]
    elif isinstance(content, dict):
        return {k: normalise(v, f"{json_path}[{k}]") for k, v in content.items()}
    else:
        param_type = type(content)
        msg = f"Type {param_type} used for parameter {json_path} is not a number or a string"
        raise AirflowException(msg)


def validate_trigger_event(event: dict):
    """
    Validate correctness of the event received from DatabricksExecutionTrigger.

    See: :class:`~airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger`.
    """
    keys_to_check = ["run_id", "run_page_url", "run_state", "errors"]
    for key in keys_to_check:
        if key not in event:
            raise AirflowException(f"Could not find `{key}` in the event: {event}")

    try:
        RunState.from_json(event["run_state"])
    except Exception:
        raise AirflowException(f'Run state returned by the Trigger is incorrect: {event["run_state"]}')
