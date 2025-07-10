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
"""Hook for Great Expectations Cloud connections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from airflow.hooks.base import BaseHook

from airflow.providers.greatexpectations.common.gx_context_actions import load_data_context


@dataclass(frozen=True)
class GXCloudConfig:
    """Configuration for GX Cloud connections."""
    cloud_access_token: str
    cloud_organization_id: str


class GXCloudHook(BaseHook):
    """
    Connect to the GX Cloud managed backend.

    :gx_cloud_conn_id str: name of the GX Cloud connection
    """

    conn_name_attr = "gx_cloud_conn_id"
    default_conn_name = "gx_cloud_default"
    conn_type = "gx_cloud"
    hook_name = "Great Expectations Cloud"

    def __init__(self, gx_cloud_conn_id: str = default_conn_name):
        super().__init__()
        self.gx_cloud_conn_id = gx_cloud_conn_id

    def get_conn(self) -> GXCloudConfig:  # type: ignore[override]
        """Get GX Cloud connection configuration."""
        config = self.get_connection(self.gx_cloud_conn_id)
        return GXCloudConfig(
            cloud_access_token=config.password, cloud_organization_id=config.login
        )

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra", "host"],
            "relabeling": {
                "login": "GX Cloud Organization ID",
                "password": "GX Cloud Access Token",
            },
            "placeholders": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test GX Cloud connection."""
        try:
            config = self.get_conn()
            load_data_context(context_type="cloud", gx_cloud_config=config)
            return True, "Connection successful."
        except Exception as error:
            return False, str(error) 