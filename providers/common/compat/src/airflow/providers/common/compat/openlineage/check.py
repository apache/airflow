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

import functools
import logging
from importlib import metadata
from typing import Any

from packaging.version import Version

from airflow.exceptions import AirflowOptionalProviderFeatureException

log = logging.getLogger(__name__)


def require_openlineage_version(
    provider_min_version: str | None | Any = None, client_min_version: str | None = None
):
    """
    Enforce minimum version requirements for OpenLineage provider or client.

    Some providers, such as Snowflake and DBT Cloud, do not require an OpenLineage provider but may
    offer optional features that depend on it. These features are generally available starting
    from a specific version of the OpenLineage provider or client. This decorator helps ensure compatibility,
    preventing import errors and providing clear logs about version requirements.

    Args:
        provider_min_version: Optional minimum version requirement for apache-airflow-providers-openlineage
        client_min_version: Optional minimum version requirement for openlineage-python

    Raises:
        ValueError: If neither `provider_min_version` nor `client_min_version` is provided.
        TypeError: If the decorator is used without parentheses (e.g., `@require_openlineage_version`).
    """
    err_msg = (
        "`require_openlineage_version` decorator must be used with at least one argument: "
        "'provider_min_version' or 'client_min_version', "
        'e.g., @require_openlineage_version(provider_min_version="1.0.0")'
    )
    # Detect if decorator is mistakenly used without arguments
    if callable(provider_min_version) and client_min_version is None:
        raise TypeError(err_msg)

    # Ensure at least one argument is provided
    if provider_min_version is None and client_min_version is None:
        raise ValueError(err_msg)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if provider_min_version:
                try:
                    provider_version: str = metadata.version("apache-airflow-providers-openlineage")
                except metadata.PackageNotFoundError:
                    try:
                        from airflow.providers.openlineage import __version__ as provider_version
                    except (ImportError, AttributeError, ModuleNotFoundError):
                        raise AirflowOptionalProviderFeatureException(
                            "OpenLineage provider not found or has no version, "
                            f"skipping function `{func.__name__}` execution"
                        )

                if provider_version and Version(provider_version) < Version(provider_min_version):
                    raise AirflowOptionalProviderFeatureException(
                        f"OpenLineage provider version `{provider_version}` "
                        f"is lower than required `{provider_min_version}`, "
                        f"skipping function `{func.__name__}` execution"
                    )

            if client_min_version:
                try:
                    client_version: str = metadata.version("openlineage-python")
                except metadata.PackageNotFoundError:
                    raise AirflowOptionalProviderFeatureException(
                        f"OpenLineage client not found, skipping function `{func.__name__}` execution"
                    )

                if client_version and Version(client_version) < Version(client_min_version):
                    raise AirflowOptionalProviderFeatureException(
                        f"OpenLineage client version `{client_version}` "
                        f"is lower than required `{client_min_version}`, "
                        f"skipping function `{func.__name__}` execution"
                    )

            return func(*args, **kwargs)

        return wrapper

    return decorator
