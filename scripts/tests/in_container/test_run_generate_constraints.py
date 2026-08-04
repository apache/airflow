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

import run_generate_constraints as m


class TestBuildProviderPreReleaseRequirements:
    """Pre-releases are allowed for the providers only, never for the whole resolution."""

    def test_every_requirement_names_a_provider_and_permits_a_pre_release(self, monkeypatch):
        monkeypatch.setattr(
            m,
            "get_all_active_provider_distributions",
            lambda python_version=None: [
                "apache-airflow-providers-amazon",
                "apache-airflow-providers-cncf-kubernetes",
            ],
        )

        requirements = m.build_provider_pre_release_requirements("3.10")

        assert requirements == [
            "apache-airflow-providers-amazon>=0.0.0rc0",
            "apache-airflow-providers-cncf-kubernetes>=0.0.0rc0",
        ]
        # The rc lower bound is what marks the package as explicit to uv; without it the
        # requirement would not permit a pre-release at all.
        assert all(requirement.endswith(">=0.0.0rc0") for requirement in requirements)
        assert all(requirement.startswith("apache-airflow-providers-") for requirement in requirements)

    def test_the_python_version_is_passed_through(self, monkeypatch):
        """Providers excluded on a Python version must not be named for it."""
        seen: list[str | None] = []

        def fake_distributions(python_version=None):
            seen.append(python_version)
            return []

        monkeypatch.setattr(m, "get_all_active_provider_distributions", fake_distributions)

        m.build_provider_pre_release_requirements("3.14")

        assert seen == ["3.14"]
