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

import jmespath

from tests.charts.helm_template_generator import render_chart


class MetadataConnectionSecretTest(unittest.TestCase):

    non_chart_database_values = {
        "user": "someuser",
        "pass": "somepass",
        "host": "somehost",
        "port": 7777,
        "db": "somedb",
    }

    def test_should_not_generate_a_document_if_using_existing_secret(self):
        docs = render_chart(
            values={"data": {"metadataSecretName": "foo"}},
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
        )

        assert 0 == len(docs)

    def _get_connection(self, values: dict) -> str:
        docs = render_chart(
            values=values,
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
        )
        encoded_connection = jmespath.search("data.connection", docs[0])
        return base64.b64decode(encoded_connection).decode()

    def test_default_connection(self):
        connection = self._get_connection({})

        assert (
            "postgresql://postgres:postgres@release-name-postgresql.default:5432/postgres?sslmode=disable"
            == connection
        )

    def test_should_set_pgbouncer_overrides_when_enabled(self):
        values = {"pgbouncer": {"enabled": True}}
        connection = self._get_connection(values)

        # host, port, dbname get overridden
        assert (
            "postgresql://postgres:postgres@release-name-pgbouncer.default:6543"
            "/release-name-metadata?sslmode=disable" == connection
        )

    def test_should_set_pgbouncer_overrides_with_non_chart_database_when_enabled(self):
        values = {
            "pgbouncer": {"enabled": True},
            "data": {"metadataConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(values)

        # host, port, dbname still get overridden even with an non-chart db
        assert (
            "postgresql://someuser:somepass@release-name-pgbouncer.default:6543"
            "/release-name-metadata?sslmode=disable" == connection
        )

    def test_should_correctly_use_non_chart_database(self):
        values = {
            "data": {
                "metadataConnection": {
                    **self.non_chart_database_values,
                    "sslmode": "require",
                }
            }
        }
        connection = self._get_connection(values)

        assert "postgresql://someuser:somepass@somehost:7777/somedb?sslmode=require" == connection

    def test_should_support_non_postgres_db(self):
        values = {
            "data": {
                "metadataConnection": {
                    **self.non_chart_database_values,
                    "protocol": "mysql",
                }
            }
        }
        connection = self._get_connection(values)

        # sslmode is only added for postgresql
        assert "mysql://someuser:somepass@somehost:7777/somedb" == connection

    def test_should_correctly_handle_password_with_special_characters(self):
        values = {
            "data": {
                "metadataConnection": {
                    **self.non_chart_database_values,
                    "user": "username@123123",
                    "pass": "password@!@#$^&*()",
                }
            }
        }
        connection = self._get_connection(values)

        # sslmode is only added for postgresql
        assert (
            "postgresql://username%40123123:password%40%21%40%23$%5E&%2A%28%29@somehost:7777/"
            "somedb?sslmode=disable" == connection
        )
