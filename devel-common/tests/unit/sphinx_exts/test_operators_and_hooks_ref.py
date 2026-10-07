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

from sphinx_exts.operators_and_hooks_ref import _render_operator_content


@mock.patch("sphinx_exts.operators_and_hooks_ref.load_package_data", autospec=True)
def test_render_operator_content_includes_trigger_only_integrations(mock_load_package_data):
    mock_load_package_data.return_value = [
        {
            "package-name": "apache-airflow-providers-example",
            "integrations": [
                {
                    "integration-name": "Example",
                    "external-doc-url": "https://example.com",
                }
            ],
            "triggers": [
                {
                    "integration-name": "Example",
                    "python-modules": ["airflow.providers.example.triggers.example"],
                }
            ],
        }
    ]

    rendered = _render_operator_content(tags=None, header_separator="=")

    assert "Example" in rendered
    assert ":Triggers: :mod:`airflow.providers.example.triggers.example`." in rendered
    assert ":Provider: :provider:`apache-airflow-providers-example`" in rendered
