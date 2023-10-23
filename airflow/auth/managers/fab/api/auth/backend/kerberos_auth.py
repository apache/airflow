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
from __future__ import annotations

import logging
from functools import partial
from typing import Any

from requests_kerberos import HTTPKerberosAuth

from airflow.api.auth.backend.kerberos_auth import (
    init_app as base_init_app,
    requires_authentication as base_requires_authentication,
)
from airflow.utils.airflow_flask_app import get_airflow_app

log = logging.getLogger(__name__)

CLIENT_AUTH: tuple[str, str] | Any | None = HTTPKerberosAuth(service="airflow")

init_app = base_init_app
requires_authentication = partial(
    base_requires_authentication, find_user=get_airflow_app().appbuilder.sm.find_user
)
