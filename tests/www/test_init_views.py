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

import re
from unittest import mock

import pytest

from airflow.www.extensions import init_views
from tests.test_utils.config import conf_vars


class TestInitApiExperimental:
    @conf_vars({("api", "enable_experimental_api"): "true"})
    def test_should_raise_deprecation_warning_when_enabled(self):
        app = mock.MagicMock()
        with pytest.warns(DeprecationWarning, match=re.escape("The experimental REST API is deprecated.")):
            init_views.init_api_experimental(app)

    @conf_vars({("api", "enable_experimental_api"): "false"})
    def test_should_not_raise_deprecation_warning_when_disabled(self):
        app = mock.MagicMock()
        with pytest.warns(None) as warnings:
            init_views.init_api_experimental(app)
        assert len(warnings) == 0
