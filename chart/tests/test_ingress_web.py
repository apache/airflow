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


class IngressWebTest(unittest.TestCase):
    def test_should_pass_validation_with_just_ingress_enabled_v1(self):
        render_chart(
            values={"ingress": {"enabled": True}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_just_ingress_enabled_v1beta1(self):
        render_chart(
            values={"ingress": {"enabled": True}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
            kubernetes_version='1.16.0',
        )  # checks that no validation exception is raised

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={"ingress": {"enabled": True, "web": {"annotations": {"aa": "bb", "cc": "dd"}}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert {"aa": "bb", "cc": "dd"} == jmespath.search("metadata.annotations", docs[0])

    def test_should_set_ingress_class_name(self):
        docs = render_chart(
            values={"ingress": {"enabled": True, "web": {"ingressClassName": "foo"}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert "foo" == jmespath.search("spec.ingressClassName", docs[0])

    def test_should_ingress_hosts_have_priority_over_host(self):
        docs = render_chart(
            values={
                "ingress": {
                    "enabled": True,
                    "web": {
                        "tls": {"enabled": True, "secretName": "supersecret"},
                        "hosts": ["*.a-host", "b-host"],
                        "host": "old-host",
                    },
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert (
            ["*.a-host", "b-host"]
            == jmespath.search("spec.rules[*].host", docs[0])
            == jmespath.search("spec.tls[0].hosts", docs[0])
        )

    def test_should_ingress_host_still_work(self):
        docs = render_chart(
            values={
                "ingress": {
                    "enabled": True,
                    "web": {
                        "tls": {"enabled": True, "secretName": "supersecret"},
                        "host": "old-host",
                    },
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert (
            ["old-host"]
            == jmespath.search("spec.rules[*].host", docs[0])
            == jmespath.search("spec.tls[0].hosts", docs[0])
        )

    def test_should_ingress_host_entry_not_exist(self):
        docs = render_chart(
            values={
                "ingress": {
                    "enabled": True,
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert not jmespath.search("spec.rules[*].host", docs[0])
