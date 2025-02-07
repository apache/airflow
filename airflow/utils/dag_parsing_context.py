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

import warnings

from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.sdk.definitions.context import get_parsing_context

# TODO: Remove this module in Airflow 3.2

warnings.warn(
    "Import from the airflow.utils.dag_parsing_context module is deprecated and "
    "will be removed in Airflow 3.2. Please import it from 'airflow.sdk'.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["get_parsing_context", "_airflow_parsing_context_manager"]
