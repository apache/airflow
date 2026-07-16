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
from pydantic import ValidationError

from airflow.api_fastapi.core_api.datamodels.plugins import ExternalViewResponse, ReactAppResponse


class TestUIPluginDestination:
    @pytest.mark.parametrize(
        "destination",
        ["nav", "dag", "dag_run", "task", "task_instance", "asset", "base"],
    )
    def test_external_view_accepts_destination(self, destination):
        view = ExternalViewResponse(name="Asset View", href="https://example.com", destination=destination)
        assert view.destination == destination

    @pytest.mark.parametrize(
        "destination",
        ["nav", "dag", "dag_run", "task", "task_instance", "asset", "base", "dashboard"],
    )
    def test_react_app_accepts_destination(self, destination):
        app = ReactAppResponse(
            name="Asset App", bundle_url="https://example.com/app.js", destination=destination
        )
        assert app.destination == destination

    def test_external_view_rejects_unknown_destination(self):
        with pytest.raises(ValidationError):
            ExternalViewResponse(name="Bad View", href="https://example.com", destination="assets")
