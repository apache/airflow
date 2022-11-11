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

import json
import unittest

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.container_volume import AzureContainerVolumeHook
from airflow.utils import db
from tests.test_utils.providers import get_provider_min_airflow_version


class TestAzureContainerVolumeHook(unittest.TestCase):
    def test_get_file_volume(self):
        db.merge_conn(
            Connection(
                conn_id="azure_container_test_connection", conn_type="wasb", login="login", password="key"
            )
        )
        hook = AzureContainerVolumeHook(azure_container_volume_conn_id="azure_container_test_connection")
        volume = hook.get_file_volume(
            mount_name="mount", share_name="share", storage_account_name="storage", read_only=True
        )
        assert volume is not None
        assert volume.name == "mount"
        assert volume.azure_file.share_name == "share"
        assert volume.azure_file.storage_account_key == "key"
        assert volume.azure_file.storage_account_name == "storage"
        assert volume.azure_file.read_only is True

    def test_get_file_volume_connection_string(self):
        db.merge_conn(
            Connection(
                conn_id="azure_container_test_connection_connection_string",
                conn_type="wasb",
                login="login",
                password="key",
                extra=json.dumps({"extra__azure_container_volume__connection_string": "a=b;AccountKey=1"}),
            )
        )
        hook = AzureContainerVolumeHook(
            azure_container_volume_conn_id="azure_container_test_connection_connection_string"
        )
        volume = hook.get_file_volume(
            mount_name="mount", share_name="share", storage_account_name="storage", read_only=True
        )
        assert volume is not None
        assert volume.name == "mount"
        assert volume.azure_file.share_name == "share"
        assert volume.azure_file.storage_account_key == "1"
        assert volume.azure_file.storage_account_name == "storage"
        assert volume.azure_file.read_only is True

    def test_get_ui_field_behaviour_placeholders(self):
        """
        Check that ensure_prefixes decorator working properly

        Note: remove this test and the _ensure_prefixes decorator after min airflow version >= 2.5.0
        """
        assert list(AzureContainerVolumeHook.get_ui_field_behaviour()["placeholders"].keys()) == [
            "login",
            "password",
            "extra__azure_container_volume__connection_string",
        ]
        if get_provider_min_airflow_version("apache-airflow-providers-microsoft-azure") >= (2, 5):
            raise Exception(
                "You must now remove `_ensure_prefixes` from azure utils."
                " The functionality is now taken care of by providers manager."
            )
