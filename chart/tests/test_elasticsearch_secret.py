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

import base64
import unittest
from subprocess import CalledProcessError

import jmespath

from tests.helm_template_generator import render_chart


class ElasticsearchSecretTest(unittest.TestCase):
    def test_should_not_generate_a_document_if_elasticsearch_disabled(self):

        docs = render_chart(
            values={"elasticsearch": {"enabled": False}},
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )

        assert 0 == len(docs)

    def test_should_raise_error_when_connection_not_provided(self):
        with self.assertRaises(CalledProcessError) as ex_ctx:
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
            "when using a Elasticsearch" in ex_ctx.exception.stderr.decode()
        )

    def test_should_raise_error_when_conflicting_options(self):
        with self.assertRaises(CalledProcessError) as ex_ctx:
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
            in ex_ctx.exception.stderr.decode()
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
            "elastichostname:80" == connection
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
