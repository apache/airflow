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

import logging
import os
import re
import sys
import warnings
from typing import TYPE_CHECKING

# Import shared configuration parser and utilities
from airflow._shared.configuration import (
    AirflowConfigException,
)

# Import private utility functions from parser module directly
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
    from airflow.secrets import BaseSecretsBackend

log = logging.getLogger(__name__)

# show Airflow's deprecation warnings
if not sys.warnoptions:
    warnings.filterwarnings(action="default", category=DeprecationWarning, module="airflow")
    warnings.filterwarnings(action="default", category=PendingDeprecationWarning, module="airflow")

_SQLITE3_VERSION_PATTERN = re.compile(r"(?P<version>^\d+(?:\.\d+)*)\D?.*$")

ConfigType = str | int | float | bool
ConfigOptionsDictType = dict[str, ConfigType]
ConfigSectionSourcesType = dict[str, str | tuple[str, str]]
ConfigSourcesType = dict[str, ConfigSectionSourcesType]

ENV_VAR_PREFIX = "AIRFLOW__"


class ConfigModifications:
    """
    Holds modifications to be applied when writing out the config.

    :param rename: Mapping from (old_section, old_option) to (new_section, new_option)
    :param remove: Set of (section, option) to remove
    :param default_updates: Mapping from (section, option) to new default value
    """

    def __init__(self) -> None:
        self.rename: dict[tuple[str, str], tuple[str, str]] = {}
        self.remove: set[tuple[str, str]] = set()
        self.default_updates: dict[tuple[str, str], str] = {}

    def add_rename(self, old_section: str, old_option: str, new_section: str, new_option: str) -> None:
        self.rename[(old_section, old_option)] = (new_section, new_option)

    def add_remove(self, section: str, option: str) -> None:
        self.remove.add((section, option))

    def add_default_update(self, section: str, option: str, new_default: str) -> None:
        self.default_updates[(section, option)] = new_default


def _generate_fernet_key() -> str:
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


def get_custom_secret_backend(worker_mode: bool = False):
    """
    Get Secret Backend if defined in airflow.cfg.

    Conditionally selects the section, key and kwargs key based on whether it is called from worker or not.
    """
    section = "workers" if worker_mode else "secrets"
    key = "secrets_backend" if worker_mode else "backend"
    kwargs_key = "secrets_backend_kwargs" if worker_mode else "backend_kwargs"

    secrets_backend_cls = conf.getimport(section=section, key=key)
    if not secrets_backend_cls:
        if worker_mode:
            # if we find no secrets backend for worker, return that of secrets backend
            secrets_backend_cls = conf.getimport(section="secrets", key="backend")
            if not secrets_backend_cls:
                return None
            # When falling back to secrets backend, use its kwargs
            kwargs_key = "backend_kwargs"
            section = "secrets"
        else:
            return None

    try:
        backend_kwargs = conf.getjson(section=section, key=kwargs_key)
        if not backend_kwargs:
            backend_kwargs = {}
        elif not isinstance(backend_kwargs, dict):
            raise ValueError("not a dict")
    except AirflowConfigException:
        log.warning("Failed to parse [%s] %s as JSON, defaulting to no kwargs.", section, kwargs_key)
        backend_kwargs = {}
    except ValueError:
        log.warning("Failed to parse [%s] %s into a dict, defaulting to no kwargs.", section, kwargs_key)
        backend_kwargs = {}

    return secrets_backend_cls(**backend_kwargs)


def ensure_secrets_loaded(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
) -> list[BaseSecretsBackend]:
    """
    Ensure that all secrets backends are loaded.

    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends.

    # Check if we are loading the backends for worker too by checking if the default_backends is equal
    # to DEFAULT_SECRETS_SEARCH_PATH.
    if len(secrets_backend_list) == 2 or default_backends != DEFAULT_SECRETS_SEARCH_PATH:
        return initialize_secrets_backends(default_backends=default_backends)
    return secrets_backend_list


def initialize_secrets_backends(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
) -> list[BaseSecretsBackend]:
    """
    Initialize secrets backend.

    * import secrets backend classes
    * instantiate them and return them in a list
    """
    backend_list = []
    worker_mode = False
    if default_backends != DEFAULT_SECRETS_SEARCH_PATH:
        worker_mode = True

    custom_secret_backend = get_custom_secret_backend(worker_mode)

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

    for class_name in default_backends:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


def initialize_auth_manager() -> BaseAuthManager:
    """
    Initialize auth manager.

    * import user manager class
    * instantiate it and return it
    """
    auth_manager_cls = conf.getimport(section="core", key="auth_manager")

    if not auth_manager_cls:
        raise AirflowConfigException(
            "No auth manager defined in the config. Please specify one using section/key [core/auth_manager]."
        )

    return auth_manager_cls()


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "$AIRFLOW_HOME/airflow.cfg" respectively as defaults.
from airflow._shared.configuration import (  # noqa: E402
    AIRFLOW_HOME,
    conf,
)

# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "tests", "dags"
)
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, "dags")

# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "tests", "plugins"
)
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, "plugins")

secrets_backend_list = initialize_secrets_backends()
conf.validate()
