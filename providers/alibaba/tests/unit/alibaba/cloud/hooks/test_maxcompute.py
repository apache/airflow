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

from unittest import mock

from airflow.providers.alibaba.cloud.hooks.maxcompute import MaxComputeHook

MAXCOMPUTE_HOOK_MODULE = "airflow.providers.alibaba.cloud.hooks.maxcompute.MaxComputeHook.{}"
MOCK_MAXCOMPUTE_CONN_ID = "mock_id"
MOCK_MAXCOMPUTE_PROJECT = "mock_project"
MOCK_MAXCOMPUTE_ENDPOINT = "mock_endpoint"


class TestMaxComputeHook:
    def setup_method(self):
        with mock.patch(
            MAXCOMPUTE_HOOK_MODULE.format("get_connection"),
        ) as mock_get_connection:
            mock_conn = mock.MagicMock()
            mock_conn.extra_dejson = {
                "access_key_id": "mock_access_key_id",
                "access_key_secret": "mock_access_key_secret",
                "project": MOCK_MAXCOMPUTE_PROJECT,
                "endpoint": MOCK_MAXCOMPUTE_ENDPOINT,
            }

            mock_get_connection.return_value = mock_conn
            self.hook = MaxComputeHook(maxcompute_conn_id=MOCK_MAXCOMPUTE_CONN_ID)

    @mock.patch(MAXCOMPUTE_HOOK_MODULE.format("get_client"))
    def test_run_sql(self, mock_get_client):
        mock_instance = mock.MagicMock()
        mock_client = mock.MagicMock()
        mock_client.run_sql.return_value = mock_instance
        mock_get_client.return_value = mock_client

        sql = "SELECT 1"
        priority = 1
        running_cluster = "mock_running_cluster"
        hints = {"hint_key": "hint_value"}
        aliases = {"alias_key": "alias_value"}
        default_schema = "mock_default_schema"
        quota_name = "mock_quota_name"

        instance = self.hook.run_sql(
            sql=sql,
            priority=priority,
            running_cluster=running_cluster,
            hints=hints,
            aliases=aliases,
            default_schema=default_schema,
            quota_name=quota_name,
        )

        assert instance == mock_instance

        assert mock_client.run_sql.asssert_called_once_with(
            sql=sql,
            priority=priority,
            running_cluster=running_cluster,
            hints=hints,
            aliases=aliases,
            default_schema=default_schema,
            quota_name=quota_name,
        )

    @mock.patch(MAXCOMPUTE_HOOK_MODULE.format("get_client"))
    def test_get_instance(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_client.exist_instance.return_value = True
        mock_instance = mock.MagicMock()
        mock_client.get_instance.return_value = mock_instance
        mock_get_client.return_value = mock_client
        instance_id = "mock_instance_id"

        instance = self.hook.get_instance(
            instance_id=instance_id,
            project=MOCK_MAXCOMPUTE_PROJECT,
            endpoint=MOCK_MAXCOMPUTE_ENDPOINT,
        )

        mock_client.get_instance.assert_called_once_with(
            id_=instance_id,
            project=MOCK_MAXCOMPUTE_PROJECT,
        )

        assert instance == mock_instance

    @mock.patch(MAXCOMPUTE_HOOK_MODULE.format("get_client"))
    def test_stop_instance_success(self, mock_get_client):
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        instance_id = "mock_instance_id"

        self.hook.stop_instance(
            instance_id=instance_id,
            project=MOCK_MAXCOMPUTE_PROJECT,
            endpoint=MOCK_MAXCOMPUTE_ENDPOINT,
        )

        mock_client.stop_instance.assert_called_once()
