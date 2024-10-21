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
"""Session authentication backend."""

from __future__ import annotations

import warnings
from typing import Any

import airflow.providers.fab.auth_manager.api.auth.backend.session as fab_session
from airflow.exceptions import RemovedInAirflow3Warning

CLIENT_AUTH: tuple[str, str] | Any | None = None


warnings.warn(
    "This module is deprecated. Please use `airflow.providers.fab.auth_manager.api.auth.backend.session` instead.",
    RemovedInAirflow3Warning,
    stacklevel=2,
)


init_app = fab_session.init_app
requires_authentication = fab_session.requires_authentication
