#
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
from unittest import mock

try:
    import pytest
except ImportError:
    pytest = None  # type: ignore[assignment]
from google.cloud.dataproc_v1 import Cluster, ClusterStatus

from airflow.providers.google.cloud.operators import dataproc
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocUpdateClusterOperator,
    FlexibleInstanceGroupConfig,
    HardwareClass,
    InstanceFlexibilityPolicy,
    InstanceSelection,
    flexible_scale_cluster_update,
)

PROJECT_ID = "test-project"
REGION = "us-central1"
CLUSTER_NAME = "test-ho-cluster"

MASTER_FLEX = FlexibleInstanceGroupConfig(num_instances=1, core_count=4, hardware_class="STANDARD")
WORKER_FLEX = FlexibleInstanceGroupConfig(
    num_instances=2,
    core_count=8,
    hardware_class=HardwareClass.PERFORMANCE,
    boot_disk_size_gb=500,
)
SECONDARY_FLEX = {"num_instances": 3, "core_count": 8}

OPERATORS_MODULE = "airflow.providers.google.cloud.operators.dataproc"


class TestFlexibleInstanceGroupConfig(unittest.TestCase):
    def test_to_dict_full(self):
        self.assertEqual(
            WORKER_FLEX.to_dict(),
            {
                "num_instances": 2,
                "core_count": 8,
                "hardware_class": "PERFORMANCE",
                "boot_disk_size_gb": 500,
            },
        )

    def test_to_dict_omits_unset_boot_disk_and_defaults_to_standard(self):
        config = FlexibleInstanceGroupConfig(num_instances=0, core_count=2)
        self.assertEqual(
            config.to_dict(),
            {"num_instances": 0, "core_count": 2, "hardware_class": "STANDARD"},
        )

    def test_hardware_class_string_is_case_insensitive(self):
        config = FlexibleInstanceGroupConfig(num_instances=1, core_count=2, hardware_class="performance")
        self.assertEqual(config.hardware_class, HardwareClass.PERFORMANCE)

    def test_invalid_core_count(self):
        for core_count in (1, 3, 6, 64, 0, -2):
            with self.subTest(core_count=core_count):
                with self.assertRaisesRegex(ValueError, "core_count .* must be one of"):
                    FlexibleInstanceGroupConfig(num_instances=1, core_count=core_count)

    def test_invalid_hardware_class(self):
        with self.assertRaises(ValueError):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=2, hardware_class="PREMIUM")

    def test_invalid_num_instances(self):
        for num_instances in (-1, "2", None):
            with self.subTest(num_instances=num_instances):
                with self.assertRaisesRegex(ValueError, "num_instances"):
                    FlexibleInstanceGroupConfig(num_instances=num_instances, core_count=2)

    def test_invalid_boot_disk_size(self):
        for size in (0, -100):
            with self.subTest(size=size):
                with self.assertRaisesRegex(ValueError, "boot_disk_size_gb"):
                    FlexibleInstanceGroupConfig(num_instances=1, core_count=2, boot_disk_size_gb=size)


class TestClusterGeneratorHighObtainability(unittest.TestCase):
    def test_standard_cluster_is_unchanged(self):
        """Regression: without flexible configs the generated config is unchanged."""
        config = ClusterGenerator(
            project_id=PROJECT_ID,
            num_workers=2,
            zone="us-central1-a",
            master_machine_type="n2-standard-4",
            worker_machine_type="n2-standard-8",
            num_preemptible_workers=1,
        ).make()
        self.assertEqual(config["master_config"]["num_instances"], 1)
        self.assertEqual(config["worker_config"]["num_instances"], 2)
        self.assertEqual(config["secondary_worker_config"]["num_instances"], 1)
        self.assertNotIn("master_flexible_config", config)
        self.assertNotIn("worker_flexible_config", config)
        self.assertNotIn("secondary_worker_flexible_config", config)
        self.assertTrue(config["gce_cluster_config"]["zone_uri"].endswith("/zones/us-central1-a"))

    def test_ho_cluster_emits_only_flexible_configs(self):
        config = ClusterGenerator(
            project_id=PROJECT_ID,
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
            secondary_worker_flexible_config=SECONDARY_FLEX,
            image_version="2.2",
            subnetwork_uri="default",
        ).make()

        self.assertEqual(
            config["master_flexible_config"],
            {"num_instances": 1, "core_count": 4, "hardware_class": "STANDARD"},
        )
        self.assertEqual(
            config["worker_flexible_config"],
            {
                "num_instances": 2,
                "core_count": 8,
                "hardware_class": "PERFORMANCE",
                "boot_disk_size_gb": 500,
            },
        )
        self.assertEqual(
            config["secondary_worker_flexible_config"],
            {"num_instances": 3, "core_count": 8, "hardware_class": "STANDARD"},
        )
        # Mutual exclusivity: no concrete instance group configs are sent.
        self.assertNotIn("master_config", config)
        self.assertNotIn("worker_config", config)
        self.assertNotIn("secondary_worker_config", config)
        # AutoZone: no zone is sent.
        self.assertNotIn("zone_uri", config["gce_cluster_config"])
        # Unrelated settings still flow through.
        self.assertEqual(config["software_config"]["image_version"], "2.2")
        self.assertEqual(config["gce_cluster_config"]["subnetwork_uri"], "default")

    def test_ho_cluster_without_secondary_workers(self):
        config = ClusterGenerator(
            project_id=PROJECT_ID,
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
        ).make()
        self.assertNotIn("secondary_worker_flexible_config", config)
        self.assertNotIn("secondary_worker_config", config)

    def test_ho_cluster_accepts_dicts(self):
        config = ClusterGenerator(
            project_id=PROJECT_ID,
            master_flexible_config={"num_instances": 1, "core_count": 2},
            worker_flexible_config={
                "num_instances": 5,
                "core_count": 16,
                "hardware_class": "PERFORMANCE",
            },
        ).make()
        self.assertEqual(config["worker_flexible_config"]["num_instances"], 5)
        self.assertEqual(config["worker_flexible_config"]["hardware_class"], "PERFORMANCE")

    def test_ho_single_node_cluster(self):
        """Single-node HO: master flexible config only and num_workers=0.

        Mirrors the API: a worker flexible config must not be sent for single-node
        clusters, the single-node intent is carried by the
        ``dataproc:dataproc.allow.zero.workers`` property.
        """
        config = ClusterGenerator(
            project_id=PROJECT_ID,
            master_flexible_config=MASTER_FLEX,
            num_workers=0,
        ).make()
        self.assertEqual(
            config["master_flexible_config"],
            {"num_instances": 1, "core_count": 4, "hardware_class": "STANDARD"},
        )
        self.assertNotIn("worker_flexible_config", config)
        self.assertNotIn("secondary_worker_flexible_config", config)
        self.assertNotIn("master_config", config)
        self.assertNotIn("worker_config", config)
        self.assertNotIn("secondary_worker_config", config)
        self.assertEqual(
            config["software_config"]["properties"]["dataproc:dataproc.allow.zero.workers"],
            "true",
        )

    def test_ho_rejects_zero_primary_worker_instances(self):
        # The API requires a positive num_instances for the primary workers; a
        # single-node HO cluster is expressed with num_workers=0 instead.
        with self.assertRaisesRegex(ValueError, "num_workers=0"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 0, "core_count": 4},
            )

    def test_ho_single_node_rejects_secondary_workers(self):
        with self.assertRaisesRegex(ValueError, "single-node"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                num_workers=0,
                secondary_worker_flexible_config=SECONDARY_FLEX,
            )

    def test_ho_requires_master_flexible_config(self):
        with self.assertRaisesRegex(ValueError, "cluster-wide"):
            ClusterGenerator(project_id=PROJECT_ID, worker_flexible_config=WORKER_FLEX)
        with self.assertRaisesRegex(ValueError, "cluster-wide"):
            ClusterGenerator(project_id=PROJECT_ID, secondary_worker_flexible_config=SECONDARY_FLEX)

    def test_ho_requires_worker_flexible_config_unless_single_node(self):
        with self.assertRaisesRegex(ValueError, "cluster-wide"):
            ClusterGenerator(project_id=PROJECT_ID, master_flexible_config=MASTER_FLEX)
        with self.assertRaisesRegex(ValueError, "cluster-wide"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                secondary_worker_flexible_config=SECONDARY_FLEX,
            )

    def test_ho_rejects_concrete_secondary_workers(self):
        """num_preemptible_workers must not leak a concrete secondary_worker_config."""
        with self.assertRaisesRegex(ValueError, "secondary_worker_flexible_config instead"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                num_preemptible_workers=2,
            )

    def test_ho_rejects_explicit_zone(self):
        with self.assertRaisesRegex(ValueError, "auto-zoned"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                zone="us-central1-a",
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
            )

    def test_ho_rejects_ha_masters(self):
        with self.assertRaisesRegex(ValueError, "num_instances must be 1"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config={"num_instances": 3, "core_count": 4},
                worker_flexible_config=WORKER_FLEX,
            )
        # num_masters is a concrete master parameter and conflicts as such.
        with self.assertRaisesRegex(ValueError, r"master_flexible_config can't be set together with"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                num_masters=3,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
            )

    def test_ho_rejects_custom_images(self):
        for name, kwargs in (
            ("custom_image", {"custom_image": "my-image"}),
            ("custom_image_family", {"custom_image_family": "my-family"}),
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "custom_image"):
                    ClusterGenerator(
                        project_id=PROJECT_ID,
                        master_flexible_config=MASTER_FLEX,
                        worker_flexible_config=WORKER_FLEX,
                        **kwargs,
                    )

    def test_master_flexible_config_conflicts(self):
        cases = (
            ("master_machine_type", {"master_machine_type": "n2-standard-4"}),
            ("master_disk_type", {"master_disk_type": "pd-ssd"}),
            ("master_disk_size", {"master_disk_size": 100}),
            (
                "master_accelerator",
                {
                    "master_accelerator_type": "nvidia-tesla-t4",
                    "master_accelerator_count": 1,
                },
            ),
            (
                "master_instance_flexibility_policy",
                {
                    "master_instance_flexibility_policy": InstanceFlexibilityPolicy(
                        instance_selection_list=[InstanceSelection(machine_types=["n2-standard-4"])]
                    )
                },
            ),
        )
        for name, kwargs in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, r"master_flexible_config can't be set together with"):
                    ClusterGenerator(
                        project_id=PROJECT_ID,
                        master_flexible_config=MASTER_FLEX,
                        worker_flexible_config=WORKER_FLEX,
                        **kwargs,
                    )

    def test_worker_flexible_config_conflicts(self):
        cases = (
            ("num_workers", {"num_workers": 2}),
            ("num_workers_zero", {"num_workers": 0}),
            ("min_num_workers", {"num_workers": 2, "min_num_workers": 1}),
            ("worker_machine_type", {"worker_machine_type": "n2-standard-4"}),
            ("worker_disk_type", {"worker_disk_type": "pd-ssd"}),
            ("worker_disk_size", {"worker_disk_size": 100}),
            (
                "worker_accelerator",
                {
                    "worker_accelerator_type": "nvidia-tesla-t4",
                    "worker_accelerator_count": 1,
                },
            ),
            (
                "worker_instance_flexibility_policy",
                {
                    "worker_instance_flexibility_policy": InstanceFlexibilityPolicy(
                        instance_selection_list=[InstanceSelection(machine_types=["n2-standard-4"])]
                    )
                },
            ),
        )
        for name, kwargs in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, r"worker_flexible_config can't be set together with"):
                    ClusterGenerator(
                        project_id=PROJECT_ID,
                        master_flexible_config=MASTER_FLEX,
                        worker_flexible_config=WORKER_FLEX,
                        **kwargs,
                    )

    def test_secondary_worker_flexible_config_conflicts(self):
        cases = (
            ("num_preemptible_workers", {"num_preemptible_workers": 2}),
            (
                "instance_flexibility_policy",
                {
                    "secondary_worker_instance_flexibility_policy": InstanceFlexibilityPolicy(
                        instance_selection_list=[InstanceSelection(machine_types=["n2-standard-4"])]
                    )
                },
            ),
            (
                "accelerator",
                {
                    "secondary_worker_accelerator_type": "nvidia-tesla-t4",
                    "secondary_worker_accelerator_count": 1,
                },
            ),
        )
        for name, kwargs in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(
                    ValueError,
                    r"secondary_worker_flexible_config can't be set together with",
                ):
                    ClusterGenerator(
                        project_id=PROJECT_ID,
                        master_flexible_config=MASTER_FLEX,
                        worker_flexible_config=WORKER_FLEX,
                        secondary_worker_flexible_config=SECONDARY_FLEX,
                        **kwargs,
                    )

    def test_invalid_flexible_config_dict_key(self):
        with self.assertRaisesRegex(ValueError, "Invalid worker_flexible_config"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 1, "cores": 4},
            )

    def test_invalid_flexible_config_type(self):
        with self.assertRaisesRegex(ValueError, "must be a"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config="num_instances=2,core_count=4",
            )

    def test_invalid_core_count_in_dict_propagates(self):
        with self.assertRaisesRegex(ValueError, "core_count"):
            ClusterGenerator(
                project_id=PROJECT_ID,
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 1, "core_count": 6},
            )

    def test_ho_make_accepts_flexible_configs(self):
        gen = ClusterGenerator(project_id=PROJECT_ID)
        config = gen.make(
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
            secondary_worker_flexible_config=SECONDARY_FLEX,
        )
        self.assertEqual(config["master_flexible_config"]["num_instances"], 1)
        self.assertEqual(config["worker_flexible_config"]["num_instances"], 2)
        self.assertEqual(config["secondary_worker_flexible_config"]["num_instances"], 3)


class TestFlexibleScaleClusterUpdate(unittest.TestCase):
    def test_primary_workers_only(self):
        cluster, update_mask = flexible_scale_cluster_update(num_workers=5)
        self.assertEqual(cluster, {"config": {"worker_flexible_config": {"num_instances": 5}}})
        self.assertEqual(update_mask, {"paths": ["config.worker_flexible_config.num_instances"]})

    def test_both_node_groups(self):
        cluster, update_mask = flexible_scale_cluster_update(num_workers=5, num_secondary_workers=0)
        self.assertEqual(
            cluster,
            {
                "config": {
                    "worker_flexible_config": {"num_instances": 5},
                    "secondary_worker_flexible_config": {"num_instances": 0},
                }
            },
        )
        self.assertEqual(
            update_mask,
            {
                "paths": [
                    "config.worker_flexible_config.num_instances",
                    "config.secondary_worker_flexible_config.num_instances",
                ]
            },
        )
        # Never emit the legacy paths alongside the flexible ones.
        for path in update_mask["paths"]:
            self.assertNotIn("worker_config", path.replace("flexible_config", ""))

    def test_requires_at_least_one_count(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            flexible_scale_cluster_update()

    def test_rejects_negative_counts(self):
        with self.assertRaises(ValueError):
            flexible_scale_cluster_update(num_workers=-1)
        with self.assertRaises(ValueError):
            flexible_scale_cluster_update(num_secondary_workers=-1)


class TestDataprocUpdateClusterOperatorHighObtainability(unittest.TestCase):
    def _make(self, cluster, paths):
        return DataprocUpdateClusterOperator(
            task_id="scale",
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster=cluster,
            update_mask={"paths": paths},
            graceful_decommission_timeout={"seconds": 0},
        )

    def test_rejects_conflicting_counts_across_both_paths(self):
        for legacy_group, flexible_group in (
            ("worker_config", "worker_flexible_config"),
            ("secondary_worker_config", "secondary_worker_flexible_config"),
        ):
            with self.subTest(legacy_group=legacy_group):
                with self.assertRaisesRegex(ValueError, "[Cc]onflicting"):
                    self._make(
                        {
                            "config": {
                                legacy_group: {"num_instances": 10},
                                flexible_group: {"num_instances": 5},
                            }
                        },
                        [
                            f"config.{legacy_group}.num_instances",
                            f"config.{flexible_group}.num_instances",
                        ],
                    )

    def test_rejects_both_paths_when_a_count_is_missing(self):
        with self.assertRaisesRegex(ValueError, "[Cc]onflicting"):
            self._make(
                {"config": {"worker_flexible_config": {"num_instances": 5}}},
                [
                    "config.worker_config.num_instances",
                    "config.worker_flexible_config.num_instances",
                ],
            )

    def test_accepts_both_paths_with_equal_counts(self):
        # Redundant but legal: the API accepts both paths when the counts agree.
        self._make(
            {
                "config": {
                    "worker_config": {"num_instances": 5},
                    "worker_flexible_config": {"num_instances": 5},
                }
            },
            [
                "config.worker_config.num_instances",
                "config.worker_flexible_config.num_instances",
            ],
        )

    def test_proto_cluster_counts_are_read_from_the_message(self):
        cluster = Cluster({"config": {"worker_config": {"num_instances": 5}}})
        # A single path never needs the counts.
        self._make(cluster, ["config.worker_config.num_instances"])
        # Both paths: the flexible count is unreadable (or 0) -> conflict.
        with self.assertRaisesRegex(ValueError, "[Cc]onflicting"):
            self._make(
                cluster,
                [
                    "config.worker_config.num_instances",
                    "config.worker_flexible_config.num_instances",
                ],
            )

    def test_accepts_flexible_path_for_workers_and_legacy_for_secondary(self):
        # Different node groups may use different paths.
        self._make(
            {
                "config": {
                    "worker_flexible_config": {"num_instances": 5},
                    "secondary_worker_config": {"num_instances": 0},
                }
            },
            [
                "config.worker_flexible_config.num_instances",
                "config.secondary_worker_config.num_instances",
            ],
        )

    def test_rejects_create_time_only_flexible_paths(self):
        paths = (
            "config.worker_flexible_config.core_count",
            "config.worker_flexible_config.hardware_class",
            "config.worker_flexible_config.boot_disk_size_gb",
            "config.secondary_worker_flexible_config.core_count",
            "config.master_flexible_config.num_instances",
            "config.master_flexible_config.core_count",
            "config.worker_flexible_config",
        )
        for path in paths:
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, "create-time only"):
                    self._make({"config": {}}, [path])

    def test_templated_update_mask_is_not_validated_at_parse_time(self):
        DataprocUpdateClusterOperator(
            task_id="scale",
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster="{{ var.json.cluster }}",
            update_mask="{{ var.json.update_mask }}",  # type: ignore[arg-type]
            graceful_decommission_timeout={"seconds": 0},
        )

    def test_accepts_field_mask_proto(self):
        from google.protobuf import field_mask_pb2

        self._make(
            {"config": {"worker_flexible_config": {"num_instances": 5}}},
            ["config.worker_flexible_config.num_instances"],
        )
        DataprocUpdateClusterOperator(
            task_id="scale2",
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster={"config": {"worker_flexible_config": {"num_instances": 5}}},
            update_mask=field_mask_pb2.FieldMask(paths=["config.worker_flexible_config.num_instances"]),
            graceful_decommission_timeout={"seconds": 0},
        )

    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_passes_flexible_update_through(self, mock_hook_cls, unused_persist):
        cluster, update_mask = flexible_scale_cluster_update(num_workers=5)
        op = DataprocUpdateClusterOperator(
            task_id="scale",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster=cluster,
            update_mask=update_mask,
            graceful_decommission_timeout={"seconds": 600},
        )
        op.execute(context=mock.MagicMock())

        mock_hook_cls.return_value.update_cluster.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster={"config": {"worker_flexible_config": {"num_instances": 5}}},
            update_mask={"paths": ["config.worker_flexible_config.num_instances"]},
            graceful_decommission_timeout={"seconds": 600},
            request_id=None,
            retry=mock.ANY,
            timeout=None,
            metadata=(),
        )


class TestDataprocCreateClusterOperatorHighObtainability(unittest.TestCase):
    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_sends_flexible_configs(self, mock_hook_cls, unused_persist):
        cluster_config = ClusterGenerator(
            project_id=PROJECT_ID,
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
        ).make()
        op = DataprocCreateClusterOperator(
            task_id="create",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster_config=cluster_config,
        )
        hook = mock_hook_cls.return_value
        hook.wait_for_operation.return_value = Cluster(
            cluster_name=CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.RUNNING),
        )
        op.execute(context=mock.MagicMock())

        _, kwargs = hook.create_cluster.call_args
        sent = kwargs["cluster_config"]
        self.assertEqual(
            sent["master_flexible_config"],
            {"num_instances": 1, "core_count": 4, "hardware_class": "STANDARD"},
        )
        self.assertEqual(sent["worker_flexible_config"]["num_instances"], 2)
        self.assertNotIn("master_config", sent)
        self.assertNotIn("worker_config", sent)


class TestExampleDag(unittest.TestCase):
    def test_example_dag_imports_and_has_expected_tasks(self):
        try:
            from providers.google.tests.system.google.cloud.dataproc import (
                example_dataproc_cluster_high_obtainability,
            )
        except ImportError:
            import importlib.util
            from pathlib import Path

            dag_path = (
                Path(__file__).parents[4]
                / "system"
                / "google"
                / "cloud"
                / "dataproc"
                / "example_dataproc_cluster_high_obtainability.py"
            )
            spec = importlib.util.spec_from_file_location(
                "example_dataproc_cluster_high_obtainability", dag_path
            )
            example_dataproc_cluster_high_obtainability = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(example_dataproc_cluster_high_obtainability)

        self.assertCountEqual(
            example_dataproc_cluster_high_obtainability.dag.task_ids,
            [
                "create_ho_cluster",
                "scale_up_ho_workers",
                "spark_pi",
                "scale_down_ho_secondary_workers",
                "delete_ho_cluster",
            ],
        )
        config = example_dataproc_cluster_high_obtainability.HO_CLUSTER_CONFIG
        self.assertIn("master_flexible_config", config)
        self.assertIn("worker_flexible_config", config)
        self.assertIn("secondary_worker_flexible_config", config)
        self.assertNotIn("master_config", config)


if __name__ == "__main__":
    unittest.main()
