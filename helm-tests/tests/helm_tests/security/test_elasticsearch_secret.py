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

import base64

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


class TestElasticsearchSecret:
    """Tests elasticsearch secret."""

    def _get_connection(self, values: dict) -> str:
        docs = render_chart(
            values=values,
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )
        encoded_connection = jmespath.search("data.connection", docs[0])
        return base64.b64decode(encoded_connection).decode()

    def test_should_correctly_handle_password_with_special_characters(self):
        connection = self._get_connection(
            {
                "elasticsearch": {
                    "enabled": True,
                    "connection": {
                        "user": "username!@#$%%^&*()",
                        "pass": "password!@#$%%^&*()",
                        "host": "elastichostname",
                    },
                }
            }
        )

        assert (
            connection
            == "http://username%21%40%23$%25%25%5E&%2A%28%29:password%21%40%23$%25%25%5E&%2A%28%29@"
            "elastichostname:9200"
        )

    def test_should_generate_secret_with_specified_port(self):
        connection = self._get_connection(
            {
                "elasticsearch": {
                    "enabled": True,
                    "connection": {
                        "user": "username",
                        "pass": "password",
                        "host": "elastichostname",
                        "port": 2222,
                    },
                }
            }
        )

        assert connection == "http://username:password@elastichostname:2222"

    @pytest.mark.parametrize("scheme", ["http", "https"])
    def test_should_generate_secret_with_specified_schemes(self, scheme):
        connection = self._get_connection(
            {
                "elasticsearch": {
                    "enabled": True,
                    "connection": {
                        "scheme": scheme,
                        "user": "username",
                        "pass": "password",
                        "host": "elastichostname",
                    },
                }
            }
        )

        assert f"{scheme}://username:password@elastichostname:9200" == connection

    @pytest.mark.parametrize(
        ("extra_conn_kwargs", "expected_user_info"),
        [
            # When both user and password are empty.
            ({}, ""),
            # When password is empty
            ({"user": "admin"}, ""),
            # When user is empty
            ({"pass": "password"}, ""),
            # Valid username/password
            ({"user": "admin", "pass": "password"}, "admin:password"),
        ],
    )
    def test_url_generated_when_user_pass_empty_combinations(self, extra_conn_kwargs, expected_user_info):
        connection = self._get_connection(
            {
                "elasticsearch": {
                    "enabled": True,
                    "connection": {"host": "elastichostname", "port": 8080, **extra_conn_kwargs},
                }
            }
        )

        if not expected_user_info:
            assert connection == "http://elastichostname:8080"
        else:
            assert f"http://{expected_user_info}@elastichostname:8080" == connection

    def test_should_add_annotations_to_elasticsearch_secret(self):
        docs = render_chart(
            values={
                "elasticsearch": {
                    "enabled": True,
                    "connection": {
                        "user": "username",
                        "pass": "password",
                        "host": "elastichostname",
                    },
                    "secretAnnotations": {"test_annotation": "test_annotation_value"},
                }
            },
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )[0]

        assert "annotations" in jmespath.search("metadata", docs)
        assert jmespath.search("metadata.annotations", docs)["test_annotation"] == "test_annotation_value"
