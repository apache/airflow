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

import html

import pytest

from airflow.configuration import conf

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.www import check_content_in_response, check_content_not_in_response

pytestmark = pytest.mark.db_test


@conf_vars({("webserver", "expose_config"): "False"})
def test_user_cant_view_configuration(admin_client):
    resp = admin_client.get("configuration", follow_redirects=True)
    check_content_in_response(
        "Your Airflow administrator chose not to expose the configuration, "
        "most likely for security reasons.",
        resp,
    )


@conf_vars({("webserver", "expose_config"): "True"})
def test_user_can_view_configuration(admin_client):
    resp = admin_client.get("configuration", follow_redirects=True)
    for section, key in conf.sensitive_config_values:
        value = conf.get(section, key, fallback="")
        if value:
            check_content_in_response(html.escape(value), resp)


@conf_vars({("webserver", "expose_config"): "non-sensitive-only"})
def test_configuration_redacted(admin_client):
    resp = admin_client.get("configuration", follow_redirects=True)
    for section, key in conf.sensitive_config_values:
        value = conf.get(section, key, fallback="")
        if value and value != "airflow" and not value.startswith("db+postgresql"):
            check_content_not_in_response(value, resp)


@conf_vars({("webserver", "expose_config"): "non-sensitive-only"})
def test_configuration_redacted_in_running_configuration(admin_client):
    resp = admin_client.get("configuration", follow_redirects=True)
    for section, key in conf.sensitive_config_values:
        value = conf.get(section, key, fallback="")
        if value and value != "airflow":
            check_content_not_in_response("<td class='code'>" + html.escape(value) + "</td", resp)


@conf_vars({("webserver", "expose_config"): "non-sensitive-only"})
@conf_vars({("database", "# sql_alchemy_conn"): "testconn"})
@conf_vars({("core", "  # secret_key"): "core_secret"})
@conf_vars({("core", "fernet_key"): "secret_fernet_key"})
def test_commented_out_config(admin_client):
    resp = admin_client.get("configuration", follow_redirects=True)
    check_content_in_response("testconn", resp)
    check_content_in_response("core_secret", resp)
    check_content_not_in_response("secret_fernet_key", resp)
