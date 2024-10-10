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
Utilities for migration legacy endpoints to FastAPI.

This module can be deleted once the AIP-84 is completed and the legacy API is deleted.
"""

from __future__ import annotations

from typing import Callable, TypeVar

from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")


def mark_fastapi_migration_done(function: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Mark an endpoint as migrated over to the new FastAPI API.

    This will help track what endpoints need to be migrated and the one that can be safely deleted.
    """
    return function
