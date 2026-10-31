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

from subprocess import CalledProcessError

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart

DEPLOYMENT = "templates/dag-processor/dag-processor-deployment.yaml"
PDB = "templates/dag-processor/dag-processor-poddisruptionbudget.yaml"
BUNDLES = [
    {"name": "bundle1", "classpath": "a.B", "kwargs": {}},
    {"name": "bundle2", "classpath": "a.B", "kwargs": {}},
]


class TestDagProcessorSets:
    """Tests dag processor sets."""

    def test_default_renders_only_default_dag_processor(self):
        docs = render_chart(name="test", show_only=[DEPLOYMENT])

        assert jmespath.search("[*].metadata.name", docs) == ["test-dag-processor"]
        assert jmespath.search("spec.selector.matchLabels", docs[0]) == {
            "tier": "airflow",
            "component": "dag-processor",
            "release": "test",
        }

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-dag-processor", "test-dag-processor-set1", "test-dag-processor-set2"]),
            (False, ["test-dag-processor-set1", "test-dag-processor-set2"]),
        ],
    )
    def test_create_multiple_dag_processor_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "dagProcessor": {
                    "enableDefault": enable_default,
                    "sets": [{"name": "set1"}, {"name": "set2"}],
                }
            },
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    @pytest.mark.parametrize(
        ("dag_processor", "expected"),
        [
            ({"sets": [{"name": "set1"}]}, ["test-dag-processor-set1"]),
            ({"sets": [{"name": "set1"}], "enableDefault": None}, ["test-dag-processor-set1"]),
            (
                {"sets": [{"name": "set1"}], "enableDefault": True},
                ["test-dag-processor", "test-dag-processor-set1"],
            ),
            ({"sets": [], "enableDefault": None}, ["test-dag-processor"]),
            ({"sets": [], "enableDefault": False}, []),
        ],
    )
    def test_enable_default_is_derived_from_sets(self, dag_processor, expected):
        docs = render_chart(name="test", values={"dagProcessor": dag_processor}, show_only=[DEPLOYMENT])

        assert jmespath.search("[*].metadata.name", docs) == expected

    def test_set_labels_and_selector(self):
        docs = render_chart(
            values={"dagProcessor": {"enableDefault": False, "sets": [{"name": "set1"}]}},
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search('spec.selector.matchLabels."dag-processor-set"', docs[0]) == "set1"
        assert jmespath.search('spec.template.metadata.labels."dag-processor-set"', docs[0]) == "set1"

    def test_set_overwrites_parent_values(self):
        docs = render_chart(
            name="test",
            values={
                "dagProcessor": {
                    "enableDefault": True,
                    "replicas": 1,
                    "resources": {"limits": {"cpu": "1"}},
                    "sets": [
                        {"name": "big", "replicas": 3, "resources": {"limits": {"memory": "4Gi"}}},
                        {"name": "small"},
                    ],
                }
            },
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search("[*].spec.replicas", docs) == [1, 3, 1]
        assert jmespath.search("[*].spec.template.spec.containers[0].resources", docs) == [
            {"limits": {"cpu": "1"}},
            {"limits": {"memory": "4Gi"}},
            {"limits": {"cpu": "1"}},
        ]

    def test_bundle_names_are_appended_to_args(self):
        docs = render_chart(
            name="test",
            values={
                "dagProcessor": {
                    "enableDefault": False,
                    "dagBundleConfigList": BUNDLES,
                    "sets": [
                        {"name": "one", "bundleNames": ["bundle1"]},
                        {"name": "both", "bundleNames": ["bundle1", "bundle2"]},
                    ],
                }
            },
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search("[*].spec.template.spec.containers[0].args", docs) == [
            ["bash", "-c", "exec airflow dag-processor --bundle-name 'bundle1'"],
            ["bash", "-c", "exec airflow dag-processor --bundle-name 'bundle1' --bundle-name 'bundle2'"],
        ]

    def test_bundle_names_on_default_dag_processor(self):
        docs = render_chart(
            values={"dagProcessor": {"dagBundleConfigList": BUNDLES, "bundleNames": ["bundle2"]}},
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search("spec.template.spec.containers[0].args[-1]", docs[0]) == (
            "exec airflow dag-processor --bundle-name 'bundle2'"
        )

    def test_args_untouched_without_bundle_names(self):
        docs = render_chart(
            values={"dagProcessor": {"enableDefault": False, "sets": [{"name": "set1"}]}},
            show_only=[DEPLOYMENT],
        )

        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec airflow dag-processor",
        ]

    def test_all_bundles_stay_in_shared_config(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enableDefault": False,
                    "dagBundleConfigList": BUNDLES,
                    "sets": [{"name": "one", "bundleNames": ["bundle1"]}],
                }
            },
            show_only=["templates/configmaps/configmap.yaml"],
        )

        assert "bundle1" in docs[0]["data"]["airflow.cfg"]
        assert "bundle2" in docs[0]["data"]["airflow.cfg"]

    @pytest.mark.parametrize(
        ("dag_processor", "message"),
        [
            (
                {"dagBundleConfigList": BUNDLES, "sets": [{"name": "one", "bundleNames": ["unknown"]}]},
                'Bundle "unknown" is not defined',
            ),
            ({"sets": [{"replicas": 2}]}, "missing property 'name'"),
            (
                {"sets": [{"name": "one", "dagBundleConfigList": BUNDLES}]},
                "cannot define `dagBundleConfigList`",
            ),
        ],
    )
    def test_invalid_sets_fail(self, dag_processor, message):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(values={"dagProcessor": dag_processor}, show_only=[DEPLOYMENT])

        assert message in ex_ctx.value.stderr.decode()

    def test_pdb_per_set(self):
        docs = render_chart(
            name="test",
            values={
                "dagProcessor": {
                    "enableDefault": True,
                    "podDisruptionBudget": {"enabled": True},
                    "sets": [
                        {"name": "one", "podDisruptionBudget": {"config": {"maxUnavailable": 2}}},
                        {"name": "two", "podDisruptionBudget": {"enabled": False}},
                    ],
                }
            },
            show_only=[PDB],
        )

        assert jmespath.search("[*].metadata.name", docs) == [
            "test-dag-processor-pdb",
            "test-dag-processor-pdb-one",
        ]
        assert jmespath.search('[1].spec.selector.matchLabels."dag-processor-set"', docs) == "one"
        assert jmespath.search("[1].spec.maxUnavailable", docs) == 2

    def test_dag_processor_disabled_renders_nothing(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": False, "sets": [{"name": "one"}]}},
            show_only=[DEPLOYMENT],
        )

        assert docs == []
