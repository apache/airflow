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

"""Deprecated serializers module - moved to airflow.sdk.serde.serializers."""

from __future__ import annotations

import importlib
import warnings

from airflow.utils.deprecation_tools import DeprecatedImportWarning


def __getattr__(name: str):
    """Redirect all submodule imports to airflow.sdk.serde.serializers with deprecation warning."""
    warnings.warn(
        f"`airflow.serialization.serializers.{name}` is deprecated. "
        f"Please use `airflow.sdk.serde.serializers.{name}` instead.",
        DeprecatedImportWarning,
        stacklevel=2,
    )
    try:
        return importlib.import_module(f"airflow.sdk.serde.serializers.{name}")
    except ModuleNotFoundError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
