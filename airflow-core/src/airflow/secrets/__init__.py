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
"""
The Secrets framework provides a means of getting connection objects from various sources.

The following sources are available:

* Environment variables
* Metastore database
* Local Filesystem Secrets Backend
"""

from __future__ import annotations

from airflow.utils.deprecation_tools import add_deprecated_classes

__all__ = ["BaseSecretsBackend", "DEFAULT_SECRETS_SEARCH_PATH"]

from airflow.secrets.base_secrets import BaseSecretsBackend

DEFAULT_SECRETS_SEARCH_PATH = [
    "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    "airflow.secrets.metastore.MetastoreBackend",
]


__deprecated_classes = {
    "cache": {
        "SecretCache": "airflow.sdk.execution_time.cache.SecretCache",
    },
}
add_deprecated_classes(__deprecated_classes, __name__)


def __getattr__(name):
    if name == "DEFAULT_SECRETS_SEARCH_PATH_WORKERS":
        import warnings

        warnings.warn(
            "airflow.secrets.DEFAULT_SECRETS_SEARCH_PATH_WORKERS is moved to the Task SDK. "
            "Use airflow.sdk.execution_time.secrets.DEFAULT_SECRETS_SEARCH_PATH_WORKERS instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

            return DEFAULT_SECRETS_SEARCH_PATH_WORKERS
        except (ImportError, AttributeError):
            # Back-compat for older Task SDK clients
            return [
                "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
            ]

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
