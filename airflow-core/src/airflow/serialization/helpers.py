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
"""Serialized DAG and BaseOperator."""

from __future__ import annotations

from typing import Any

from airflow.configuration import conf
from airflow.sdk.execution_time.secrets_masker import redact
from airflow.settings import json


def serialize_template_field(
    template_field: Any, name: str, allow_tuple_conversion: bool = False
) -> str | dict | list | int | float:
    """
    Return a serializable representation of the templated field.

    If ``templated_field`` contains a class or instance that requires recursive
    templating, store them as strings. Otherwise simply return the field as-is.
    """
    print("template_field:", template_field)

    def is_jsonable(x):
        if isinstance(x, tuple) and not allow_tuple_conversion:
            # Tuple is converted to list in json.dumps
            # so while it is jsonable, it changes the type which might be a surprise
            # for the user, so instead we return False here -- which will convert it to string
            return False
        try:
            json.dumps(x)
            return True
        except (TypeError, OverflowError):
            return False

    def translate_tuples_to_lists(obj: Any):
        """Recursively convert tuples to lists."""
        if isinstance(obj, tuple):
            return [translate_tuples_to_lists(item) for item in obj]
        elif isinstance(obj, list):
            return [translate_tuples_to_lists(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: translate_tuples_to_lists(value) for key, value in obj.items()}
        return obj

    max_length = conf.getint("core", "max_templated_field_length")

    if not is_jsonable(template_field):
        try:
            serialized = template_field.serialize()
        except AttributeError:
            serialized = str(template_field)
        if len(serialized) > max_length:
            rendered = redact(serialized, name)
            return (
                "Truncated. You can change this behaviour in [core]max_templated_field_length. "
                f"{rendered[: max_length - 79]!r}... "
            )
        return serialized
    else:
        if not template_field:
            if isinstance(template_field, tuple):
                return []
            return template_field
        template_field = translate_tuples_to_lists(template_field)
        serialized = str(template_field)
        if len(serialized) > max_length:
            rendered = redact(serialized, name)
            return (
                "Truncated. You can change this behaviour in [core]max_templated_field_length. "
                f"{rendered[: max_length - 79]!r}... "
            )
        return template_field
