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

import datetime
import urllib.parse

import pytest

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime.datetime(2022, 1, 1)

DEFAULT_VAL = urllib.parse.quote_plus(str(DEFAULT_DATE))


@pytest.fixture(scope="module", autouse=True)
def _reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


def test_task_view_no_task_instance(admin_client):
    url = (
        f"/task?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}"
    )
    resp = admin_client.get(url, follow_redirects=True)
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "<h5>No Task Instance Available</h5>" in html
    assert "<h5>Task Instance Attributes</h5>" not in html


def test_rendered_templates_view_no_task_instance(admin_client):
    url = f"/rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}"
    resp = admin_client.get(url, follow_redirects=True)
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "Rendered Template" in html
