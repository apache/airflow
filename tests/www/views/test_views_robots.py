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

import pytest

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


def test_robots(viewer_client):
    resp = viewer_client.get("/robots.txt", follow_redirects=True)
    assert resp.data.decode("utf-8") == "User-agent: *\nDisallow: /\n"


def test_deployment_warning_config(admin_client):
    warn_text = "webserver.warn_deployment_exposure"
    admin_client.get("/robots.txt", follow_redirects=True)
    resp = admin_client.get("", follow_redirects=True)
    assert warn_text in resp.data.decode("utf-8")

    with conf_vars({("webserver", "warn_deployment_exposure"): "False"}):
        admin_client.get("/robots.txt", follow_redirects=True)
        resp = admin_client.get("/robots.txt", follow_redirects=True)
        assert warn_text not in resp.data.decode("utf-8")
