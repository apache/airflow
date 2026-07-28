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
Deprecated module - Stats has been removed, but we are maintaining a shim for backwards compat.

We don't want to encourage any future usage of Stats. This file is to avoid breaking
any kind of external packages such as 3rd party providers.
"""

from __future__ import annotations

import warnings

from airflow.sdk.observability.stats import Stats as Stats
from airflow.utils.deprecation_tools import DeprecatedImportWarning

# This is meant for 3rd party libraries and that's why the sdk module path is suggested.
warnings.warn(
    "Using 'from airflow.stats import Stats' is deprecated. "
    "Please use 'from airflow.sdk.observability import stats' instead.",
    DeprecatedImportWarning,
    stacklevel=2,
)
