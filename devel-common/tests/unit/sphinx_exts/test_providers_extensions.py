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

import pytest

from sphinx_exts.providers_extensions import (
    _render_connection_services_content,
)

FAKE_PACKAGE_DATA = [
    {
        "package-name": "apache-airflow-providers-example",
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.example.hooks.example.ExampleHook",
                "hook-name": "Example",
                "connection-type": "example",
                "external-services": ["OpenAI", "Anthropic"],
            }
        ],
    }
]


@mock.patch("sphinx_exts.providers_extensions.load_package_data", autospec=True)
def test_render_connection_services_content_renders_one_row_per_connection_type(mock_load_package_data):
    mock_load_package_data.return_value = FAKE_PACKAGE_DATA

    rendered = _render_connection_services_content("apache-airflow-providers-example")

    assert ":ref:`Example <howto/connection:example>`" in rendered
    assert "OpenAI, Anthropic" in rendered


@mock.patch("sphinx_exts.providers_extensions.load_package_data", autospec=True)
def test_render_connection_services_content_unknown_package_raises(mock_load_package_data):
    mock_load_package_data.return_value = FAKE_PACKAGE_DATA

    with pytest.raises(ValueError, match="No provider.yaml found"):
        _render_connection_services_content("apache-airflow-providers-does-not-exist")
