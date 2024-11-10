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

# We do not use "from __future__ import annotations" here because it is not supported
# by Pycharm when we want to make sure all imports in airflow work from namespace packages
# Adding it automatically is excluded in pyproject.toml via I002 ruff rule exclusion

# Make `airflow` a namespace package, supporting installing
# airflow.providers.* in different locations (i.e. one in site, and one in user
# lib.)  This is required by some IDEs to resolve the import paths.
from __future__ import annotations

import warnings

from airflow.assets import AssetAlias as DatasetAlias, Dataset

# TODO: Remove this module in Airflow 3.2

warnings.warn(
    "Import from the airflow.dataset module is deprecated and "
    "will be removed in the Airflow 3.2. Please import it from 'airflow.assets'.",
    DeprecationWarning,
    stacklevel=2,
)


__all__ = [
    "Dataset",
    "DatasetAlias",
]
