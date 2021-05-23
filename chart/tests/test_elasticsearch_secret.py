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

from tests.helm_template_generator import render_chart


class ElasticsearchSecretTest(unittest.TestCase):
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
                    "connection": {
                        "user": "username!@#$%%^&*()",
                        "pass": "password!@#$%%^&*()",
                        "host": "elastichostname",
                    }
                }
            }
        )

        assert (
            "http://username%21%40%23$%25%25%5E&%2A%28%29:password%21%40%23$%25%25%5E&%2A%28%29@"
            "elastichostname:80" == connection
        )
