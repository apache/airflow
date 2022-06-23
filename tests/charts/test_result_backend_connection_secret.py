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
from typing import Union

import jmespath
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class ResultBackendConnectionSecretTest(unittest.TestCase):
    def _get_values_with_version(self, values, version):
        if version != "default":
            values["airflowVersion"] = version
        return values

    def _assert_for_old_version(self, version, value, expected_value):
        # TODO remove default from condition after airflow update
        if version == "2.3.2" or version == "default":
            assert value == expected_value
        else:
            assert value is None

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
            values={
                "executor": executor,
                "data": {
                    "metadataConnection": {**self.non_chart_database_values},
                    "resultBackendConnection": {
                        **self.non_chart_database_values,
                        "user": "anotheruser",
                        "pass": "anotherpass",
                    },
                },
            },
            show_only=["templates/secrets/result-backend-connection-secret.yaml"],
        )

        assert expected_doc_count == len(docs)

    def _get_connection(self, values: dict) -> Union[str, None]:
        docs = render_chart(
            values=values,
            show_only=["templates/secrets/result-backend-connection-secret.yaml"],
        )
        if len(docs) == 0:
            return None
        encoded_connection = jmespath.search("data.connection", docs[0])
        return base64.b64decode(encoded_connection).decode()

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_default_connection_old_version(self, version):
        connection = self._get_connection(self._get_values_with_version(version=version, values={}))
        self._assert_for_old_version(
            version,
            value=connection,
            expected_value="db+postgresql://postgres:postgres@RELEASE-NAME"
            "-postgresql:5432/postgres?sslmode=disable",
        )

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_should_default_to_custom_metadata_db_connection_with_pgbouncer_overrides(self, version):
        values = {
            "pgbouncer": {"enabled": True},
            "data": {"metadataConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(self._get_values_with_version(values=values, version=version))

        # host, port, dbname still get overridden
        self._assert_for_old_version(
            version,
            value=connection,
            expected_value="db+postgresql://someuser:somepass@RELEASE-NAME-pgbouncer"
            ":6543/RELEASE-NAME-result-backend?sslmode=allow",
        )

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_should_set_pgbouncer_overrides_when_enabled(self, version):
        values = {"pgbouncer": {"enabled": True}}
        connection = self._get_connection(self._get_values_with_version(values=values, version=version))

        # host, port, dbname get overridden
        self._assert_for_old_version(
            version,
            value=connection,
            expected_value="db+postgresql://postgres:postgres@RELEASE-NAME-pgbouncer"
            ":6543/RELEASE-NAME-result-backend?sslmode=disable",
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

    @parameterized.expand(["2.3.2", "2.4.0", "default"])
    def test_should_default_to_custom_metadata_db_connection_in_old_version(self, version):
        values = {
            "data": {"metadataConnection": {**self.non_chart_database_values}},
        }
        connection = self._get_connection(self._get_values_with_version(values=values, version=version))
        self._assert_for_old_version(
            version,
            value=connection,
            expected_value="db+postgresql://someuser:somepass@somehost:7777/somedb?sslmode=allow",
        )

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

    def test_should_correctly_handle_password_with_special_characters(self):
        values = {
            "data": {
                "resultBackendConnection": {
                    **self.non_chart_database_values,
                    "user": "username@123123",
                    "pass": "password@!@#$^&*()",
                },
            }
        }
        connection = self._get_connection(values)

        assert (
            "db+postgresql://username%40123123:password%40%21%40%23$%5E&%2A%28%29@somehost:7777/"
            "somedb?sslmode=allow" == connection
        )
