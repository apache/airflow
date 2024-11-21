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


class Config(BaseModel):
    """configuration serializer."""

    navbar_color: str
    navbar_text_color: str
    navbar_hover_color: str
    navbar_text_hover_color: str
    navbar_logo_text_color: str
    page_size: int
    auto_refresh_interval: int
    default_ui_timezone: str
    hide_paused_dags_by_default: bool
    instance_name: str
    show_trigger_form_if_no_params: bool
    instance_name_has_markup: bool
    enable_swagger_ui: bool
    require_confirmation_dag_change: bool
    default_wrap: bool
    warn_deployment_exposure: bool
    audit_view_excluded_events: str
    audit_view_included_events: str
    is_k8s: bool
    test_connection: str
    state_color_mapping: dict


class ConfigResponse(BaseModel):
    """config serializer for responses."""

    configs: Config
