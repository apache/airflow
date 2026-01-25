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
from chart_utils.helm_template_generator import render_chart


class TestDagProcessorPerBundle:
    """Tests DAG processor per-bundle deployment feature."""

    def test_deploy_per_bundle_disabled_creates_single_deployment(self):
        """Test that when deployPerBundle.enabled is false, single deployment is created."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {"enabled": False},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.name", docs[0]) == "release-name-dag-processor"
        assert "bundle" not in jmespath.search("metadata.labels", docs[0])

    def test_deploy_per_bundle_enabled_creates_multiple_deployments(self):
        """Test that when deployPerBundle.enabled is true, one deployment per bundle is created."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {"enabled": True},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                            "kwargs": {
                                "git_conn_id": "GITHUB__repo2",
                                "subdir": "dags",
                                "tracking_ref": "main",
                                "refresh_interval": 60,
                            },
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 2
        deployment_names = [jmespath.search("metadata.name", doc) for doc in docs]
        assert "release-name-dag-processor-bundle1" in deployment_names
        assert "release-name-dag-processor-bundle2" in deployment_names

    def test_per_bundle_deployment_has_bundle_label(self):
        """Test that per-bundle deployments have bundle label."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {"enabled": True},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.labels.bundle", docs[0]) == "bundle1"
        assert jmespath.search("spec.selector.matchLabels.bundle", docs[0]) == "bundle1"
        assert jmespath.search("spec.template.metadata.labels.bundle", docs[0]) == "bundle1"

    def test_per_bundle_args_contains_bundle_name(self):
        """Test that per-bundle args contain the bundle name."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {
                        "enabled": True,
                        "args": ["bash", "-c", "exec airflow dag-processor --bundle-name {{ bundleName }}"],
                    },
                    "dagBundleConfigList": [
                        {
                            "name": "test-bundle",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        args = jmespath.search("spec.template.spec.containers[0].args", docs[0])
        assert args is not None
        assert any("--bundle-name test-bundle" in arg for arg in args)

    def test_bundle_overrides_apply_to_deployment(self):
        """Test that bundleOverrides are correctly applied to per-bundle deployments."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "replicas": 1,
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                    "deployPerBundle": {
                        "enabled": True,
                        "bundleOverrides": {
                            "bundle1": {
                                "replicas": 3,
                                "resources": {
                                    "requests": {"memory": "2Gi", "cpu": "1000m"},
                                    "limits": {"memory": "4Gi", "cpu": "2000m"},
                                },
                            },
                        },
                    },
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 2
        # Find bundle1 deployment
        bundle1_doc = next(doc for doc in docs if "bundle1" in jmespath.search("metadata.name", doc))
        bundle2_doc = next(doc for doc in docs if "bundle2" in jmespath.search("metadata.name", doc))

        # bundle1 should have overridden replicas
        assert jmespath.search("spec.replicas", bundle1_doc) == 3
        assert (
            jmespath.search("spec.template.spec.containers[0].resources.requests.memory", bundle1_doc)
            == "2Gi"
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", bundle1_doc) == "1000m"
        )

        # bundle2 should have default replicas
        assert jmespath.search("spec.replicas", bundle2_doc) == 1

    def test_bundle_overrides_env_variables(self):
        """Test that bundleOverrides can override environment variables."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {
                        "enabled": True,
                        "bundleOverrides": {
                            "bundle1": {
                                "env": [
                                    {"name": "CUSTOM_VAR", "value": "custom_value"},
                                ],
                            },
                        },
                    },
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        env_vars = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        env_dict = {env["name"]: env["value"] for env in env_vars if "value" in env}
        assert env_dict.get("CUSTOM_VAR") == "custom_value"

    def test_per_bundle_pdb_created(self):
        """Test that PodDisruptionBudget is created for each bundle when deployPerBundle is enabled."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "podDisruptionBudget": {"enabled": True},
                    "deployPerBundle": {"enabled": True},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-poddisruptionbudget.yaml"],
        )

        assert len(docs) == 2
        pdb_names = [jmespath.search("metadata.name", doc) for doc in docs]
        assert "release-name-dag-processor-bundle1-pdb" in pdb_names
        assert "release-name-dag-processor-bundle2-pdb" in pdb_names

    def test_per_bundle_pdb_has_bundle_label(self):
        """Test that per-bundle PDBs have bundle label."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "podDisruptionBudget": {"enabled": True},
                    "deployPerBundle": {"enabled": True},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-poddisruptionbudget.yaml"],
        )

        assert len(docs) == 1
        assert jmespath.search("metadata.labels.bundle", docs[0]) == "bundle1"
        assert jmespath.search("spec.selector.matchLabels.bundle", docs[0]) == "bundle1"

    def test_per_bundle_pdb_bundle_overrides(self):
        """Test that bundleOverrides can override PodDisruptionBudget settings."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "podDisruptionBudget": {
                        "enabled": True,
                        "config": {"maxUnavailable": 1},
                    },
                    "deployPerBundle": {
                        "enabled": True,
                        "bundleOverrides": {
                            "bundle1": {
                                "podDisruptionBudget": {
                                    "enabled": True,
                                    "config": {"minAvailable": 1},
                                },
                            },
                        },
                    },
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                        {
                            "name": "bundle2",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-poddisruptionbudget.yaml"],
        )

        assert len(docs) == 2
        bundle1_pdb = next(doc for doc in docs if "bundle1" in jmespath.search("metadata.name", doc))
        bundle2_pdb = next(doc for doc in docs if "bundle2" in jmespath.search("metadata.name", doc))

        # bundle1 should have overridden config
        assert "minAvailable" in jmespath.search("spec", bundle1_pdb)
        assert jmespath.search("spec.minAvailable", bundle1_pdb) == 1

        # bundle2 should have default config
        assert "maxUnavailable" in jmespath.search("spec", bundle2_pdb)
        assert jmespath.search("spec.maxUnavailable", bundle2_pdb) == 1

    def test_per_bundle_affinity_includes_bundle_label(self):
        """Test that podAntiAffinity includes bundle label when deployPerBundle is enabled."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {"enabled": True},
                    "dagBundleConfigList": [
                        {
                            "name": "bundle1",
                            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                            "kwargs": {},
                        },
                    ],
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        affinity = jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )
        assert affinity.get("bundle") == "bundle1"
        assert affinity.get("component") == "dag-processor"

    def test_single_deployment_mode_affinity_no_bundle_label(self):
        """Test that single deployment mode affinity does not include bundle label."""
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "deployPerBundle": {"enabled": False},
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1
        affinity = jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )
        assert "bundle" not in affinity
        assert affinity.get("component") == "dag-processor"
