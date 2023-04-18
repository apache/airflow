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
from subprocess import CalledProcessError

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class TestElasticsearchSecret:
    def test_should_not_generate_a_document_if_elasticsearch_disabled(self):

        docs = render_chart(
            values={"elasticsearch": {"enabled": False}},
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )

        assert 0 == len(docs)

    def test_should_raise_error_when_connection_not_provided(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "elasticsearch": {
                        "enabled": True,
                    }
                },
                show_only=["templates/secrets/elasticsearch-secret.yaml"],
            )
        assert (
            "You must set one of the values elasticsearch.secretName or elasticsearch.connection "
            "when using a Elasticsearch" in ex_ctx.value.stderr.decode()
        )

    def test_should_raise_error_when_conflicting_options(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                values={
                    "elasticsearch": {
                        "enabled": True,
                        "secretName": "my-test",
                        "connection": {
                            "user": "username!@#$%%^&*()",
                            "pass": "password!@#$%%^&*()",
                            "host": "elastichostname",
                        },
                    },
                },
                show_only=["templates/secrets/elasticsearch-secret.yaml"],
            )
        assert (
            "You must not set both values elasticsearch.secretName and elasticsearch.connection"
            in ex_ctx.value.stderr.decode()
        )

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
            "http://username%21%40%23$%25%25%5E&%2A%28%29:password%21%40%23$%25%25%5E&%2A%28%29@"
            "elastichostname:9200" == connection
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

        assert "http://username:password@elastichostname:2222" == connection

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
        "extra_conn_kwargs, expected_user_info",
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
            assert "http://elastichostname:8080" == connection
        else:
            assert f"http://{expected_user_info}@elastichostname:8080" == connection
