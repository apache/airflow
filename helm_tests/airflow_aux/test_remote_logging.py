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


class TestElasticsearchConfig:
    """Tests elasticsearch configuration behaviors."""

    def test_should_not_generate_a_document_if_elasticsearch_disabled(self):
        docs = render_chart(
            values={"elasticsearch": {"enabled": False}},
            show_only=["templates/secrets/elasticsearch-secret.yaml"],
        )

        assert len(docs) == 0

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

