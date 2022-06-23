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
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class IngressWebTest(unittest.TestCase):
    def test_should_pass_validation_with_just_ingress_enabled_v1(self):
        render_chart(
            values={"ingress": {"web": {"enabled": True}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_just_ingress_enabled_v1beta1(self):
        render_chart(
            values={"ingress": {"web": {"enabled": True}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
            kubernetes_version='1.16.0',
        )  # checks that no validation exception is raised

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={"ingress": {"web": {"enabled": True, "annotations": {"aa": "bb", "cc": "dd"}}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert {"aa": "bb", "cc": "dd"} == jmespath.search("metadata.annotations", docs[0])

    def test_should_set_ingress_class_name(self):
        docs = render_chart(
            values={"ingress": {"web": {"enabled": True, "ingressClassName": "foo"}}},
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert "foo" == jmespath.search("spec.ingressClassName", docs[0])

    def test_should_ingress_hosts_objs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "ingress": {
                    "web": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "oldsecret"},
                        "hosts": [
                            {"name": "*.a-host", "tls": {"enabled": True, "secretName": "newsecret1"}},
                            {"name": "b-host", "tls": {"enabled": True, "secretName": "newsecret2"}},
                            {"name": "c-host", "tls": {"enabled": True, "secretName": "newsecret1"}},
                            {"name": "d-host", "tls": {"enabled": False, "secretName": ""}},
                            {"name": "e-host"},
                        ],
                        "host": "old-host",
                    },
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert ["*.a-host", "b-host", "c-host", "d-host", "e-host"] == jmespath.search(
            "spec.rules[*].host", docs[0]
        )
        assert [
            {"hosts": ["*.a-host"], "secretName": "newsecret1"},
            {"hosts": ["b-host"], "secretName": "newsecret2"},
            {"hosts": ["c-host"], "secretName": "newsecret1"},
        ] == jmespath.search("spec.tls[*]", docs[0])

    def test_should_ingress_hosts_strs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "ingress": {
                    "web": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "secret"},
                        "hosts": ["*.a-host", "b-host", "c-host", "d-host"],
                        "host": "old-host",
                    },
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert ["*.a-host", "b-host", "c-host", "d-host"] == jmespath.search("spec.rules[*].host", docs[0])
        assert [
            {"hosts": ["*.a-host", "b-host", "c-host", "d-host"], "secretName": "secret"}
        ] == jmespath.search("spec.tls[*]", docs[0])

    def test_should_ingress_deprecated_host_and_top_level_tls_still_work(self):
        docs = render_chart(
            values={
                "ingress": {
                    "web": {
                        "enabled": True,
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
                    "web": {
                        "enabled": True,
                    }
                }
            },
            show_only=["templates/webserver/webserver-ingress.yaml"],
        )
        assert not jmespath.search("spec.rules[*].host", docs[0])

    @parameterized.expand(
        [
            (None, None, False),
            (None, False, False),
            (None, True, True),
            (False, None, False),
            (True, None, True),
            (False, True, True),  # We will deploy it if _either_ are true
            (True, False, True),
        ]
    )
    def test_ingress_created(self, global_value, web_value, expected):
        values = {"ingress": {}}
        if global_value is not None:
            values["ingress"]["enabled"] = global_value
        if web_value is not None:
            values["ingress"]["web"] = {"enabled": web_value}
        if values["ingress"] == {}:
            del values["ingress"]
        docs = render_chart(values=values, show_only=["templates/webserver/webserver-ingress.yaml"])
        assert expected == (1 == len(docs))
