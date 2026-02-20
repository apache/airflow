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
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.common.ai.utils.config import ConnectionConfig
from airflow.sdk import BaseHook, Connection

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)


class CommonAIHookMixin:
    """Mixin for Common AI."""

    def get_db_api_hook(self, conn_id: str) -> DbApiHook:
        """Get the given connection's database hook."""
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook()

    def get_conn_config_from_airflow_connection(self, conn_id: str) -> ConnectionConfig:
        """Get connection configuration from Airflow connection."""
        try:
            airflow_conn = BaseHook.get_connection(conn_id)

            config = self._convert_airflow_connection(airflow_conn)

            log.info("Loaded connection config for: %s", conn_id)
            return config

        except Exception as e:
            log.error("Failed to get connection config for %s: %s", conn_id, e)
            raise

    def _convert_airflow_connection(self, conn: Connection) -> ConnectionConfig:
        """Convert Airflow connection to ConnectionConfig."""
        credentials = self._get_credentials(conn)

        extra_config = conn.extra_dejson if conn.extra else {}

        return ConnectionConfig(
            conn_id=conn.conn_id,
            credentials=credentials,
            extra_config=extra_config,
        )

    @classmethod
    def remove_none_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        """Filter out None values from the dictionary."""
        return {k: v for k, v in params.items() if v is not None}

    def _get_credentials(self, conn: Connection) -> dict[str, Any]:
        credentials = {}

        match conn.conn_type:
            case "aws":
                s3_conn: AwsGenericHook = AwsGenericHook(aws_conn_id=conn.conn_id, client_type="s3")
                creds = s3_conn.get_credentials()
                credentials.update(
                    {
                        "access_key_id": conn.login or creds.access_key,
                        "secret_access_key": conn.password or creds.secret_key,
                        "session_token": creds.token if creds.token else None,
                        "region": conn.extra_dejson.get("region"),
                        "endpoint": conn.extra_dejson.get("endpoint"),
                    }
                )
                credentials = self.remove_none_values(credentials)

            case _:
                raise ValueError(f"Unknown connection type {conn.conn_type}")
        return credentials
