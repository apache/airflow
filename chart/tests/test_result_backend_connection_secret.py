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
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class ResultBackendConnectionSecretTest(unittest.TestCase):

    non_chart_database_values = {
        "user": "someuser",
        "pass": "somepass",
        "host": "somehost",
        "protocol": "postgresql",
        "port": 7777,
        "db": "somedb",
        "sslmode": "allow",
    }

    def test_should_not_generate_a_document_if_using_existing_secret(self):
        docs = render_chart(
            values={"data": {"resultBackendSecretName": "foo"}},
            show_only=["templates/secrets/result-backend-connection-secret.yaml"],
        )

        assert 0 == len(docs)

    @parameterized.expand(
        [
            ("CeleryExecutor", 1),
            ("CeleryKubernetesExecutor", 1),
            ("LocalExecutor", 0),
        ]
    )
    def test_should_a_document_be_generated_for_executor(self, executor, expected_doc_count):
        docs = render_chart(
            values={"executor": executor},
            show_only=["templates/secrets/result-backend-connection-secret.yaml"],
        )

        assert expected_doc_count == len(docs)

    def _get_connection(self, values: dict) -> str:
        docs = render_chart(
            values=values,
            show_only=["templates/secrets/result-backend-connection-secret.yaml"],
        )
        encoded_connection = jmespath.search("data.connection", docs[0])
        return base64.b64decode(encoded_connection).decode()

    def test_default_connection(self):
        connection = self._get_connection({})

        assert (
            "db+postgresql://postgres:postgres@RELEASE-NAME-postgresql:5432/postgres?sslmode=disable"
            == connection
        )

    def test_should_default_to_custom_metadata_db_connection_with_pgbouncer_overrides(self):
        values = {
            "pgbouncer": {"enabled": True},
            "data": {"metadataConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(values)

        # host, port, dbname still get overridden
        assert (
            "db+postgresql://someuser:somepass@RELEASE-NAME-pgbouncer:6543"
            "/RELEASE-NAME-result-backend?sslmode=allow" == connection
        )

    def test_should_set_pgbouncer_overrides_when_enabled(self):
        values = {"pgbouncer": {"enabled": True}}
        connection = self._get_connection(values)

        # host, port, dbname get overridden
        assert (
            "db+postgresql://postgres:postgres@RELEASE-NAME-pgbouncer:6543"
            "/RELEASE-NAME-result-backend?sslmode=disable" == connection
        )

    def test_should_set_pgbouncer_overrides_with_non_chart_database_when_enabled(self):
        values = {
            "pgbouncer": {"enabled": True},
            "data": {"resultBackendConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(values)

        # host, port, dbname still get overridden even with an non-chart db
        assert (
            "db+postgresql://someuser:somepass@RELEASE-NAME-pgbouncer:6543"
            "/RELEASE-NAME-result-backend?sslmode=allow" == connection
        )

    def test_should_default_to_custom_metadata_db_connection(self):
        values = {
            "data": {"metadataConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(values)

        assert "db+postgresql://someuser:somepass@somehost:7777/somedb?sslmode=allow" == connection

    def test_should_correctly_use_non_chart_database(self):
        values = {"data": {"resultBackendConnection": {**self.non_chart_database_values}}}
        connection = self._get_connection(values)

        assert "db+postgresql://someuser:somepass@somehost:7777/somedb?sslmode=allow" == connection

    def test_should_support_non_postgres_db(self):
        values = {
            "data": {
                "resultBackendConnection": {
                    **self.non_chart_database_values,
                    "protocol": "mysql",
                }
            }
        }
        connection = self._get_connection(values)

        # sslmode is only added for postgresql
        assert "db+mysql://someuser:somepass@somehost:7777/somedb" == connection

    def test_should_correctly_use_non_chart_database_when_both_db_are_external(self):
        values = {
            "data": {
                "metadataConnection": {**self.non_chart_database_values},
                "resultBackendConnection": {
                    **self.non_chart_database_values,
                    "user": "anotheruser",
                    "pass": "anotherpass",
                },
            }
        }
        connection = self._get_connection(values)

        assert "db+postgresql://anotheruser:anotherpass@somehost:7777/somedb?sslmode=allow" == connection
