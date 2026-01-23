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
from chart_utils.helm_template_generator import render_chart


class TestSCCActivation:
    """Tests SCCs."""

    @pytest.mark.parametrize(
        ("rbac_enabled", "scc_enabled", "created"),
        [
            (False, False, False),
            (False, True, False),
            (True, True, True),
            (True, False, False),
        ],
    )
    def test_create_scc(self, rbac_enabled, scc_enabled, created):
        docs = render_chart(
            values={
                "multiNamespaceMode": False,
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "flower": {"enabled": True},
                "rbac": {"create": rbac_enabled, "createSCCRoleBinding": scc_enabled},
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert jmespath.search("kind", docs[0]) == "RoleBinding"
            assert jmespath.search("roleRef.kind", docs[0]) == "ClusterRole"
            assert jmespath.search("metadata.name", docs[0]) == "release-name-scc-rolebinding"
            assert jmespath.search("roleRef.name", docs[0]) == "system:openshift:scc:anyuid"
            assert jmespath.search("subjects[0].name", docs[0]) == "release-name-airflow-webserver"
            assert jmespath.search("subjects[1].name", docs[0]) == "release-name-airflow-worker"
            assert jmespath.search("subjects[2].name", docs[0]) == "release-name-airflow-scheduler"
            assert jmespath.search("subjects[3].name", docs[0]) == "release-name-airflow-api-server"
            assert jmespath.search("subjects[4].name", docs[0]) == "release-name-airflow-statsd"
            assert jmespath.search("subjects[5].name", docs[0]) == "release-name-airflow-flower"
            assert jmespath.search("subjects[6].name", docs[0]) == "release-name-airflow-redis"
            assert jmespath.search("subjects[7].name", docs[0]) == "release-name-airflow-triggerer"
            assert jmespath.search("subjects[8].name", docs[0]) == "release-name-airflow-migrate-database-job"
            assert jmespath.search("subjects[9].name", docs[0]) == "release-name-airflow-create-user-job"
            assert jmespath.search("subjects[10].name", docs[0]) == "release-name-airflow-cleanup"
            assert jmespath.search("subjects[11].name", docs[0]) == "release-name-airflow-database-cleanup"
            assert jmespath.search("subjects[12].name", docs[0]) == "release-name-airflow-dag-processor"

    @pytest.mark.parametrize(
        ("rbac_enabled", "scc_enabled", "created", "namespace", "expected_name"),
        [
            (True, True, True, "default", "default-release-name-scc-rolebinding"),
            (True, True, True, "other-ns", "other-ns-release-name-scc-rolebinding"),
        ],
    )
    def test_create_scc_multinamespace(self, rbac_enabled, scc_enabled, created, namespace, expected_name):
        docs = render_chart(
            namespace=namespace,
            values={
                "multiNamespaceMode": True,
                "createUserJob": {"enabled": False},
                "cleanup": {"enabled": False},
                "databaseCleanup": {"enabled": False},
                "flower": {"enabled": False},
                "rbac": {"create": rbac_enabled, "createSCCRoleBinding": scc_enabled},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert jmespath.search("kind", docs[0]) == "ClusterRoleBinding"
            assert jmespath.search("roleRef.kind", docs[0]) == "ClusterRole"
            assert expected_name == jmespath.search("metadata.name", docs[0])
            assert jmespath.search("roleRef.name", docs[0]) == "system:openshift:scc:anyuid"

    @pytest.mark.parametrize(
        ("rbac_enabled", "scc_enabled", "created"),
        [
            (True, True, True),
        ],
    )
    def test_create_scc_worker_only(self, rbac_enabled, scc_enabled, created):
        docs = render_chart(
            values={
                "multiNamespaceMode": False,
                "createUserJob": {"enabled": False},
                "cleanup": {"enabled": False},
                "databaseCleanup": {"enabled": False},
                "flower": {"enabled": False},
                "statsd": {"enabled": False},
                "rbac": {"create": rbac_enabled, "createSCCRoleBinding": scc_enabled},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert jmespath.search("kind", docs[0]) == "RoleBinding"
            assert jmespath.search("roleRef.kind", docs[0]) == "ClusterRole"
            assert jmespath.search("metadata.name", docs[0]) == "release-name-scc-rolebinding"
            assert jmespath.search("roleRef.name", docs[0]) == "system:openshift:scc:anyuid"
            assert jmespath.search("subjects[0].name", docs[0]) == "release-name-airflow-webserver"
            assert jmespath.search("subjects[1].name", docs[0]) == "release-name-airflow-worker"
            assert jmespath.search("subjects[2].name", docs[0]) == "release-name-airflow-scheduler"
            assert jmespath.search("subjects[3].name", docs[0]) == "release-name-airflow-api-server"
            assert jmespath.search("subjects[4].name", docs[0]) == "release-name-airflow-redis"
            assert jmespath.search("subjects[5].name", docs[0]) == "release-name-airflow-triggerer"
            assert jmespath.search("subjects[6].name", docs[0]) == "release-name-airflow-migrate-database-job"
            assert len(docs[0]["subjects"]) == 7
