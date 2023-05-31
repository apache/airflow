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

from airflow.settings import json


def serialize_template_field(template_field: Any) -> str | dict | list | int | float:
    """Return a serializable representation of the templated field.

    If ``templated_field`` contains a class or instance that requires recursive
    templating, store them as strings. Otherwise simply return the field as-is.
    """

    def is_jsonable(x):
        try:
            json.dumps(x)
            return True
        except (TypeError, OverflowError):
            return False

    if not is_jsonable(template_field):
        return str(template_field)
    else:
        return template_field
