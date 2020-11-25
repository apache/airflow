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
                "webserver": {
                    "livenessProbe": {"host": "example.com"},
                    "readinessProbe": {"host": "anotherexample.com"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        self.assertIn(
            {"name": "Host", "value": "example.com"},
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]),
        )
        self.assertIn(
            {"name": "Host", "value": "anotherexample.com"},
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]),
        )

    def test_should_not_contain_host_header_if_host_empty_string(self):
        docs = render_chart(
            values={
                "webserver": {
                    "livenessProbe": {"host": ""},
                    "readinessProbe": {"host": ""},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]),
        )
        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]),
        )

    def test_should_not_contain_host_header_if_host_not_set(self):
        docs = render_chart(
            values={
                "webserver": {
                    "livenessProbe": {"host": None},
                    "readinessProbe": {"host": None},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]),
        )
        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]),
        )

    def test_should_not_contain_host_header_by_default(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]),
        )
        self.assertIsNone(
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]),
        )
