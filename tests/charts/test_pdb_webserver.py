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

import unittest

import jmespath

from tests.charts.helm_template_generator import render_chart


class WebserverPdbTest(unittest.TestCase):
    def test_should_pass_validation_with_just_pdb_enabled_v1(self):
        render_chart(
            values={"webserver": {"podDisruptionBudget": {"enabled": True}}},
            show_only=["templates/webserver/webserver-poddisruptionbudget.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_just_pdb_enabled_v1beta1(self):
        render_chart(
            values={"webserver": {"podDisruptionBudget": {"enabled": True}}},
            show_only=["templates/webserver/webserver-poddisruptionbudget.yaml"],
            kubernetes_version='1.16.0',
        )  # checks that no validation exception is raised

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "webserver": {
                    "podDisruptionBudget": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/webserver/webserver-poddisruptionbudget.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"
