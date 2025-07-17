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
"""
This module provides functions for safely retrieving and handling OpenLineage configurations.

For the legacy boolean env variables `OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE` and `OPENLINEAGE_DISABLED`,
any string not equal to "true", "1", or "t" should be treated as False, to maintain backward compatibility.
Support for legacy variables will be removed in Airflow 3.
"""

from __future__ import annotations

import os
from typing import Any

# Disable caching if we're inside tests - this makes config easier to mock.
if os.getenv("PYTEST_VERSION"):

    def decorator(func):
        return func

    cache = decorator
else:
    from functools import cache
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
    option = conf.getboolean(_CONFIG_SECTION, "disable_source_code", fallback="False")
    if option is False:  # Check legacy variable
        option = _is_true(os.getenv("OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", ""))
    # when disable_source_code is True, is_source_enabled() should be False; hence the "not"
    return not option


@cache
def disabled_operators() -> set[str]:
    """[openlineage] disabled_for_operators."""
    option = conf.get(_CONFIG_SECTION, "disabled_for_operators", fallback="")
    return set(operator.strip() for operator in option.split(";") if operator.strip())


@cache
def selective_enable() -> bool:
    """[openlineage] selective_enable."""
    return conf.getboolean(_CONFIG_SECTION, "selective_enable", fallback="False")


@cache
def spark_inject_parent_job_info() -> bool:
    """[openlineage] spark_inject_parent_job_info."""
    return conf.getboolean(_CONFIG_SECTION, "spark_inject_parent_job_info", fallback="False")


@cache
def spark_inject_transport_info() -> bool:
    """[openlineage] spark_inject_transport_info."""
    return conf.getboolean(_CONFIG_SECTION, "spark_inject_transport_info", fallback="False")


@cache
def custom_extractors() -> set[str]:
    """[openlineage] extractors."""
    option = conf.get(_CONFIG_SECTION, "extractors", fallback="")
    if not option:
        option = os.getenv("OPENLINEAGE_EXTRACTORS", "")
    return set(extractor.strip() for extractor in option.split(";") if extractor.strip())


@cache
def custom_run_facets() -> set[str]:
    """[openlineage] custom_run_facets."""
    option = conf.get(_CONFIG_SECTION, "custom_run_facets", fallback="")
    return set(
        custom_facet_function.strip()
        for custom_facet_function in option.split(";")
        if custom_facet_function.strip()
    )


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
    if conf.getboolean(_CONFIG_SECTION, "disabled", fallback="False"):
        return True

    if _is_true(os.getenv("OPENLINEAGE_DISABLED", "")):  # Check legacy variable
        return True

    if transport():  # Check if transport is present
        return False
    if config_path(True):  # Check if config file is present
        return False
    if os.getenv("OPENLINEAGE_URL"):  # Check if url simple env var is present
        return False
    # Check if any transport configuration env var is present
    if any(k.startswith("OPENLINEAGE__TRANSPORT") and v for k, v in os.environ.items()):
        return False

    return True  # No transport configuration is present, we can disable OpenLineage


@cache
def dag_state_change_process_pool_size() -> int:
    """[openlineage] dag_state_change_process_pool_size."""
    return conf.getint(_CONFIG_SECTION, "dag_state_change_process_pool_size", fallback="1")


@cache
def execution_timeout() -> int:
    """[openlineage] execution_timeout."""
    return conf.getint(_CONFIG_SECTION, "execution_timeout", fallback="10")


@cache
def include_full_task_info() -> bool:
    """[openlineage] include_full_task_info."""
    return conf.getboolean(_CONFIG_SECTION, "include_full_task_info", fallback="False")


@cache
def debug_mode() -> bool:
    """[openlineage] debug_mode."""
    return conf.getboolean(_CONFIG_SECTION, "debug_mode", fallback="False")
