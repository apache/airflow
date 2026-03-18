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

from airflow.providers.google.common.links.storage import FileDetailsLink, StorageLink


class TestStorageLink:
    def test_storage_link(self):
        assert StorageLink.name == "GCS Storage"
        assert StorageLink.key == "storage_conf"
        assert (
            StorageLink.format_str
            == "https://console.cloud.google.com/storage/browser/{uri};tab=objects?project={project_id}"
        )

    def test_storage_link_format(self):
        link = StorageLink()
        url = link._format_link(uri="test-bucket/test-folder", project_id="test-id")
        expected_url = "https://console.cloud.google.com/storage/browser/test-bucket/test-folder;tab=objects?project=test-id"
        assert url == expected_url


class TestFileDetailsLink:
    def test_file_details_link_name_and_key(self):
        assert FileDetailsLink.name == "GCS File Details"
        assert FileDetailsLink.key == "file_details"
        assert (
            FileDetailsLink.format_str
            == "https://console.cloud.google.com/storage/browser/_details/{uri};tab=live_object?project={project_id}"
        )

    def test_file_details_link_format(self):
        link = FileDetailsLink()
        url = link._format_link(uri="test-bucket/test-folder", project_id="test-id")
        expected_url = "https://console.cloud.google.com/storage/browser/_details/test-bucket/test-folder;tab=live_object?project=test-id"
        assert url == expected_url
