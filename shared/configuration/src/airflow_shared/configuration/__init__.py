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

"""Shared configuration parser for Airflow distributions."""

from __future__ import annotations

from .exceptions import AirflowConfigException
from .parser import (
    AIRFLOW_CONFIG,
    AIRFLOW_HOME,
    ENV_VAR_PREFIX,
    FERNET_KEY,
    JWT_SECRET_KEY,
    AirflowConfigParser,
    ConfigModifications,
    ConfigOptionsDictType,
    ConfigSectionSourcesType,
    ConfigSourcesType,
    ConfigType,
    conf,
    expand_env_var,
    find_config_templates_dir,
    get_all_expansion_variables,
    initialize_config,
    load_standard_airflow_configuration,
    make_group_other_inaccessible,
    run_command,
    write_default_airflow_configuration_if_needed,
)

__all__ = [
    "AirflowConfigParser",
    "AirflowConfigException",
    "ConfigModifications",
    "ConfigSourcesType",
    "ConfigType",
    "ConfigOptionsDictType",
    "ConfigSectionSourcesType",
    "ENV_VAR_PREFIX",
    "expand_env_var",
    "run_command",
    "get_all_expansion_variables",
    "conf",
    "initialize_config",
    "load_standard_airflow_configuration",
    "write_default_airflow_configuration_if_needed",
    "make_group_other_inaccessible",
    "find_config_templates_dir",
    "AIRFLOW_HOME",
    "AIRFLOW_CONFIG",
    "JWT_SECRET_KEY",
    "FERNET_KEY",
]
