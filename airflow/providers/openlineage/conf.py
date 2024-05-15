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

import os
from typing import Any

from airflow.compat.functools import cache
from airflow.configuration import conf

_CONFIG_SECTION = "openlineage"


def _is_true(arg: Any) -> bool:
    return str(arg).lower().strip() in ("true", "1", "t")


@cache
def config_path(check_legacy_env_var: bool = True) -> str:
    """[openlineage] config_path."""
    option = conf.get(_CONFIG_SECTION, "config_path", fallback="")
    if check_legacy_env_var and not option:
        option = os.getenv("OPENLINEAGE_CONFIG", "")
    return option


@cache
def is_source_enabled() -> bool:
    """[openlineage] disable_source_code."""
    option = conf.get(_CONFIG_SECTION, "disable_source_code", fallback="")
    if not option:
        option = os.getenv("OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "")
    # when disable_source_code is True, is_source_enabled() should be False
    return not _is_true(option)


@cache
def disabled_operators() -> set[str]:
    """[openlineage] disabled_for_operators."""
    option = conf.get(_CONFIG_SECTION, "disabled_for_operators", fallback="")
    return set(operator.strip() for operator in option.split(";") if operator.strip())


@cache
def selective_enable() -> bool:
    """[openlineage] selective_enable."""
    option = conf.get(_CONFIG_SECTION, "selective_enable", fallback="")
    return _is_true(option)


@cache
def custom_extractors() -> set[str]:
    """[openlineage] extractors."""
    option = conf.get(_CONFIG_SECTION, "extractors", fallback="")
    if not option:
        option = os.getenv("OPENLINEAGE_EXTRACTORS", "")
    return set(extractor.strip() for extractor in option.split(";") if extractor.strip())


@cache
def namespace() -> str:
    """[openlineage] namespace."""
    option = conf.get(_CONFIG_SECTION, "namespace", fallback="")
    if not option:
        option = os.getenv("OPENLINEAGE_NAMESPACE", "default")
    return option


@cache
def transport() -> dict[str, Any]:
    """[openlineage] transport."""
    option = conf.getjson(_CONFIG_SECTION, "transport", fallback={})
    if not isinstance(option, dict):
        raise ValueError(f"OpenLineage transport `{option}` is not a dict")
    return option


@cache
def is_disabled() -> bool:
    """[openlineage] disabled + check if any configuration is present."""
    option = conf.get(_CONFIG_SECTION, "disabled", fallback="")
    if _is_true(option):
        return True

    option = os.getenv("OPENLINEAGE_DISABLED", "")
    if _is_true(option):
        return True

    # Check if both 'transport' and 'config_path' are not present and also
    # if legacy 'OPENLINEAGE_URL' environment variables is not set
    return transport() == {} and config_path(True) == "" and os.getenv("OPENLINEAGE_URL", "") == ""


@cache
def dag_state_change_process_pool_size() -> int:
    """[openlineage] dag_state_change_process_pool_size."""
    option = conf.getint(_CONFIG_SECTION, "dag_state_change_process_pool_size", fallback=1)
    return option
