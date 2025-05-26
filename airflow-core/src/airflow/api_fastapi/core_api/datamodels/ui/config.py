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

from pydantic import BaseModel

from airflow.api_fastapi.common.types import UIAlert


class ConfigResponse(BaseModel):
    """configuration serializer."""

    page_size: int
    auto_refresh_interval: int
    hide_paused_dags_by_default: bool
    instance_name: str
    enable_swagger_ui: bool
    require_confirmation_dag_change: bool
    default_wrap: bool
    test_connection: str
    dashboard_alert: list[UIAlert]
    show_external_log_redirect: bool
    external_log_name: str | None = None
