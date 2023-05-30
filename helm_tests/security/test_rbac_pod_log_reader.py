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

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class TestPodReader:
    """Tests RBAC Pod Reader."""

    @pytest.mark.parametrize(
        "triggerer, webserver, expected",
        [
            (True, True, ["release-name-airflow-webserver", "release-name-airflow-triggerer"]),
            (True, False, ["release-name-airflow-triggerer"]),
            (False, True, ["release-name-airflow-webserver"]),
            (False, False, []),
        ],
    )
    def test_pod_log_reader_rolebinding(self, triggerer, webserver, expected):
        docs = render_chart(
            values={
                "triggerer": {"enabled": triggerer},
                "webserver": {"allowPodLogReading": webserver},
            },
            show_only=["templates/rbac/pod-log-reader-rolebinding.yaml"],
        )
        actual = jmespath.search("subjects[*].name", docs[0]) if docs else []
        assert actual == expected

    @pytest.mark.parametrize(
        "triggerer, webserver, expected",
        [
            (True, True, "release-name-pod-log-reader-role"),
            (True, False, "release-name-pod-log-reader-role"),
            (False, True, "release-name-pod-log-reader-role"),
            (False, False, None),
        ],
    )
    def test_pod_log_reader_role(self, triggerer, webserver, expected):
        docs = render_chart(
            values={
                "triggerer": {"enabled": triggerer},
                "webserver": {"allowPodLogReading": webserver},
            },
            show_only=["templates/rbac/pod-log-reader-role.yaml"],
        )
        actual = jmespath.search("metadata.name", docs[0]) if docs else None
        assert actual == expected

    @pytest.mark.parametrize(
        "multiNamespaceMode, namespace, expectedRole, expectedRoleBinding",
        [
            (True, "namespace", "release-name-namespace-pod-log-reader-role", "release-name-namespace-pod-log-reader-rolebinding"),
            (True, "other-ns", "release-name-other-ns-pod-log-reader-role", "release-name-other-ns-pod-log-reader-rolebinding"),
            (False, "namespace", "release-name-pod-log-reader-role", "release-name-pod-log-reader-rolebinding"),
        ],
    )
    def test_pod_log_reader_rolebinding_multi_namespace(self, multiNamespaceMode, namespace, expectedRole, expectedRoleBinding):
        docs = render_chart(
            namespace = namespace,
            values={
                "webserver": {"allowPodLogReading": True},
                "multiNamespaceMode": multiNamespaceMode
            },
            show_only=["templates/rbac/pod-log-reader-rolebinding.yaml"],
        )

        actualRoleBinding = jmespath.search("metadata.name", docs[0]) if docs else []
        assert actualRoleBinding == expectedRoleBinding

        actualRoleRef = jmespath.search("roleRef.name", docs[0]) if docs else []
        assert actualRoleRef == expectedRole

        actualKind = jmespath.search("kind", docs[0]) if docs else []
        actualRoleRefKind = jmespath.search("roleRef.kind", docs[0]) if docs else []
        if multiNamespaceMode:
            assert actualKind == "ClusterRoleBinding"
            assert actualRoleRefKind == "ClusterRole"
        else:
            assert actualKind == "RoleBinding"
            assert actualRoleRefKind == "Role"

    @pytest.mark.parametrize(
        "multiNamespaceMode, namespace, expectedRole",
        [
            (True, "namespace", "release-name-namespace-pod-log-reader-role"),
            (True, "other-ns", "release-name-other-ns-pod-log-reader-role"),
            (False, "namespace", "release-name-pod-log-reader-role"),
        ],
    )
    def test_pod_log_reader_role_multi_namespace(self, multiNamespaceMode, namespace, expectedRole):
        docs = render_chart(
            namespace = namespace,
            values={
                "webserver": {"allowPodLogReading": True},
                "multiNamespaceMode": multiNamespaceMode
            },
            show_only=["templates/rbac/pod-log-reader-role.yaml"],
        )

        actualRole = jmespath.search("metadata.name", docs[0]) if docs else []
        assert actualRole == expectedRole

        actualKind = jmespath.search("kind", docs[0]) if docs else []
        if multiNamespaceMode:
            assert actualKind == "ClusterRole"
        else:
            assert actualKind == "Role"
