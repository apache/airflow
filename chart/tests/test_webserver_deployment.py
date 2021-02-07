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

import unittest

import jmespath

from tests.helm_template_generator import render_chart


class WebserverDeploymentTest(unittest.TestCase):
    def test_should_add_host_header_to_liveness_and_readiness_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": "https://example.com:21222/mypath/path"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "Host", "value": "example.com"} in jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]
        )
        assert {"name": "Host", "value": "example.com"} in jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]
        )

    def test_should_add_path_to_liveness_and_readiness_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": "https://example.com:21222/mypath/path"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.path", docs[0])
            == "/mypath/path/health"
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.path", docs[0])
            == "/mypath/path/health"
        )

    def test_should_not_contain_host_header_if_host_empty_string(self):
        docs = render_chart(
            values={},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0])
            is None
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0])
            is None
        )

    def test_should_not_contain_host_header_if_base_url_not_set(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": ""},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0])
            is None
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0])
            is None
        )

    def test_should_not_contain_host_header_by_default(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0])
            is None
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0])
            is None
        )
