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

from chart_utils.helm_template_generator import render_chart


class TestApiserverPdb:
    """Tests api-server pdb."""

    def test_should_pass_validation_with_just_pdb_enabled_v1(self):
        render_chart(
            values={"apiServer": {"podDisruptionBudget": {"enabled": True}}},
            show_only=["templates/api-server/api-server-poddisruptionbudget.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_just_pdb_enabled_v1beta1(self):
        render_chart(
            values={"apiServer": {"podDisruptionBudget": {"enabled": True}}},
            show_only=["templates/api-server/api-server-poddisruptionbudget.yaml"],
            kubernetes_version="1.16.0",
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_pdb_enabled_and_min_available_param(self):
        render_chart(
            values={
                "apiServer": {
                    "podDisruptionBudget": {
                        "enabled": True,
                        "config": {"maxUnavailable": None, "minAvailable": 1},
                    }
                }
            },
            show_only=["templates/api-server/api-server-poddisruptionbudget.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_pdb_enabled_and_min_available_param_only(self):
        render_chart(
            values={
                "apiServer": {
                    "podDisruptionBudget": {
                        "enabled": True,
                        "config": {"minAvailable": 1},
                    }
                }
            },
            show_only=["templates/api-server/api-server-poddisruptionbudget.yaml"],
        )  # checks that no validation exception is raised
