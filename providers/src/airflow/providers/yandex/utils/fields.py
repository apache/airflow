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

from typing import Any


def get_field_from_extras(
    extras: dict[str, Any], field_name: str, default: Any = None
) -> Any:
    """
    Get field from extras, first checking short name, then for backcompat checking for prefixed name.

    :param extras: Dictionary with extras keys
    :param field_name: Field name to get from extras
    :param default: Default value if field not found
    :return: Field value or default if not found
    """
    backcompat_prefix = "extra__yandexcloud__"
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
            "when using this function."
        )
    if field_name in extras:
        return extras[field_name]
    prefixed_name = f"{backcompat_prefix}{field_name}"
    if prefixed_name in extras:
        return extras[prefixed_name]
    return default
