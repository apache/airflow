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

from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars


class TestFabAppRateLimitConfig:
    @conf_vars({})
    def test_rate_limit_config_defaults(self):
        flask_app = create_app(enable_plugins=False)
        assert flask_app.config["AUTH_RATE_LIMIT_STORAGE_URI"] == "memory://"
        assert flask_app.config["AUTH_RATE_LIMIT_STORAGE_OPTIONS"] == {}

    @conf_vars(
        {
            ("fab", "auth_rate_limit_storage_uri"): "redis://my-redis-host:6379/1",
            ("fab", "auth_rate_limit_storage_options"): '{"socket_timeout": 10}',
        }
    )
    def test_rate_limit_config_custom_redis(self):
        flask_app = create_app(enable_plugins=False)

        assert flask_app.config["AUTH_RATE_LIMIT_STORAGE_URI"] == "redis://my-redis-host:6379/1"
        expected_options = {"socket_timeout": 10}
        assert flask_app.config["AUTH_RATE_LIMIT_STORAGE_OPTIONS"] == expected_options

    @conf_vars(
        {
            ("fab", "auth_rate_limit_storage_options"): '{"invalid_json": "missing_quote',
        }
    )
    def test_rate_limit_config_bad_json(self):
        flask_app = create_app(enable_plugins=False)

        assert flask_app.config["AUTH_RATE_LIMIT_STORAGE_OPTIONS"] == {}
