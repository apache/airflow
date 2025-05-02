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
import importlib
from importlib import metadata

from packaging.version import Version

from airflow.exceptions import AirflowOptionalProviderFeatureException


def require_provider_version(provider_name: str, provider_min_version: str):
    """
    Enforce minimum version requirement for a specific provider.

    Some providers, do not explicitly require other provider packages but may offer optional features
    that depend on it. These features are generally available starting from a specific version of such
    provider. This decorator helps ensure compatibility, preventing import errors and providing clear
    logs about version requirements.

    Args:
        provider_name: Name of the provider e.g., apache-airflow-providers-openlineage
        provider_min_version: Optional minimum version requirement e.g., 1.0.1

    Raises:
        ValueError: If neither `provider_name` nor `provider_min_version` is provided.
        ValueError: If full provider name (e.g., apache-airflow-providers-openlineage) is not provided.
        TypeError: If the decorator is used without parentheses (e.g., `@require_provider_version`).
    """
    err_msg = (
        "`require_provider_version` decorator must be used with two arguments: "
        "'provider_name' and 'provider_min_version', "
        'e.g., @require_provider_version(provider_name="apache-airflow-providers-openlineage", '
        'provider_min_version="1.0.0")'
    )
    # Detect if decorator is mistakenly used without arguments
    if callable(provider_name) and not provider_min_version:
        raise TypeError(err_msg)

    # Ensure both arguments are provided and not empty
    if not provider_name or not provider_min_version:
        raise ValueError(err_msg)

    # Ensure full provider name is passed
    if not provider_name.startswith("apache-airflow-providers-"):
        raise ValueError(
            f"Full `provider_name` must be provided starting with 'apache-airflow-providers-', "
            f"got `{provider_name}`."
        )

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                provider_version: str = metadata.version(provider_name)
            except metadata.PackageNotFoundError:
                try:
                    # Try dynamically importing the provider module based on the provider name
                    import_provider_name = provider_name.replace("apache-airflow-providers-", "").replace(
                        "-", "."
                    )
                    provider_module = importlib.import_module(f"airflow.providers.{import_provider_name}")

                    provider_version = getattr(provider_module, "__version__")

                except (ImportError, AttributeError, ModuleNotFoundError):
                    raise AirflowOptionalProviderFeatureException(
                        f"Provider `{provider_name}` not found or has no version, "
                        f"skipping function `{func.__name__}` execution"
                    )

            if provider_version and Version(provider_version) < Version(provider_min_version):
                raise AirflowOptionalProviderFeatureException(
                    f"Provider's `{provider_name}` version `{provider_version}` is lower than required "
                    f"`{provider_min_version}`, skipping function `{func.__name__}` execution"
                )

            return func(*args, **kwargs)

        return wrapper

    return decorator
