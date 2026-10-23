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

from airflow.providers.common.ai.plugins._plugin_urls import get_base_url_path, get_bundle_url

from tests_common.test_utils.config import conf_vars


class TestGetBaseUrlPath:
    def test_default_base_url(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert get_base_url_path("/ai-model") == "/ai-model"

    def test_http_base_url_extracts_path(self):
        with conf_vars({("api", "base_url"): "http://example.com/airflow/"}):
            assert get_base_url_path("/ai-model") == "/airflow/ai-model"


class TestGetBundleUrl:
    def test_default_base_url_returns_a_relative_path(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert get_bundle_url("/ai-model", "model.umd.cjs") == "/ai-model/static/model.umd.cjs"

    def test_http_base_url_returns_an_absolute_url(self):
        """An absolute bundle_url is needed in Vite dev mode, where import() resolves
        relative to the script origin (5173), not the document origin (28080)."""
        with conf_vars({("api", "base_url"): "http://example.com/airflow/"}):
            assert (
                get_bundle_url("/ai-model", "model.umd.cjs")
                == "http://example.com/airflow/ai-model/static/model.umd.cjs"
            )
