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
"""Tests for the RBAC granted to the migrate-database-job.

The migrate-database-job needs ``pods`` + ``pods/exec`` access only on the
downgrade branch, where it must exec ``airflow db downgrade`` inside the
still-running api-server pod (whose image still ships the reverse alembic
scripts). The Role is always rendered so the forward-migrate path remains
identical for users who only ever upgrade.

Tracked in https://github.com/apache/airflow/issues/68072.
"""

from __future__ import annotations

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart

ROLE_TEMPLATE = "templates/rbac/migrate-database-job-role.yaml"
ROLEBINDING_TEMPLATE = "templates/rbac/migrate-database-job-rolebinding.yaml"


class TestMigrateDatabaseJobRBAC:
    def test_role_and_binding_render_by_default(self):
        docs = render_chart(show_only=[ROLE_TEMPLATE, ROLEBINDING_TEMPLATE])
        kinds = sorted(d["kind"] for d in docs)
        assert kinds == ["Role", "RoleBinding"]

    @pytest.mark.parametrize("rbac_create", [False, True])
    def test_gated_on_rbac_create(self, rbac_create):
        docs = render_chart(
            values={"rbac": {"create": rbac_create}},
            show_only=[ROLE_TEMPLATE, ROLEBINDING_TEMPLATE],
        )
        assert bool(docs) is rbac_create

    def test_role_rules_grant_pods_and_exec(self):
        docs = render_chart(show_only=[ROLE_TEMPLATE])
        rules = jmespath.search("rules", docs[0])
        resources_to_verbs = {tuple(r["resources"]): set(r["verbs"]) for r in rules}
        assert ("pods",) in resources_to_verbs
        assert {"get", "list"}.issubset(resources_to_verbs[("pods",)])
        assert ("pods/exec",) in resources_to_verbs
        assert {"create", "get"}.issubset(resources_to_verbs[("pods/exec",)])

    def test_rolebinding_subject_is_migrate_db_job_sa(self):
        docs = render_chart(show_only=[ROLEBINDING_TEMPLATE])
        subjects = jmespath.search("subjects", docs[0])
        assert len(subjects) == 1
        assert subjects[0]["kind"] == "ServiceAccount"
        # Matches the name rendered by templates/jobs/migrate-database-job-serviceaccount.yaml
        # via the "migrateDatabaseJob.serviceAccountName" helper.
        assert subjects[0]["name"] == "release-name-airflow-migrate-database-job"

    def test_rolebinding_references_role(self):
        docs = render_chart(show_only=[ROLEBINDING_TEMPLATE])
        role_ref = jmespath.search("roleRef", docs[0])
        assert role_ref["kind"] == "Role"
        assert role_ref["name"] == "release-name-migrate-database-job-role"
        assert role_ref["apiGroup"] == "rbac.authorization.k8s.io"

    def test_role_scoped_to_release_namespace(self):
        docs = render_chart(
            namespace="my-airflow-ns",
            show_only=[ROLE_TEMPLATE, ROLEBINDING_TEMPLATE],
        )
        for d in docs:
            assert jmespath.search("metadata.namespace", d) == "my-airflow-ns"
