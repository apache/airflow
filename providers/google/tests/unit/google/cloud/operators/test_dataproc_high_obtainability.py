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

from unittest import mock

import pytest
from google.cloud.dataproc_v1 import Cluster, ClusterStatus

from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocUpdateClusterOperator,
    FlexibleInstanceGroupConfig,
    HardwareClass,
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


class TestFlexibleInstanceGroupConfig:
    def test_to_dict_full(self):
        assert WORKER_FLEX.to_dict() == {
            "num_instances": 2,
            "core_count": 8,
            "hardware_class": "PERFORMANCE",
            "boot_disk_size_gb": 500,
        }

    def test_to_dict_omits_unset_boot_disk_and_defaults_to_standard(self):
        config = FlexibleInstanceGroupConfig(num_instances=0, core_count=2)
        assert config.to_dict() == {"num_instances": 0, "core_count": 2, "hardware_class": "STANDARD"}

    def test_hardware_class_string_is_case_insensitive(self):
        config = FlexibleInstanceGroupConfig(num_instances=1, core_count=2, hardware_class="performance")
        assert config.hardware_class == HardwareClass.PERFORMANCE

    @pytest.mark.parametrize("core_count", [1, 3, 6, 64, 0, -2])
    def test_invalid_core_count(self, core_count):
        with pytest.raises(ValueError, match=r"core_count .* must be one of"):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=core_count)

    def test_invalid_hardware_class(self):
        with pytest.raises(ValueError, match=r"[Hh]ardware_?[Cc]lass"):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=2, hardware_class="PREMIUM")

    @pytest.mark.parametrize("num_instances", [-1, "2", None, True, False])
    def test_invalid_num_instances(self, num_instances):
        with pytest.raises(ValueError, match="num_instances"):
            FlexibleInstanceGroupConfig(num_instances=num_instances, core_count=2)

    @pytest.mark.parametrize("core_count", [0, 3, 6, True, False, "4"])
    def test_invalid_core_counts_rejected(self, core_count):
        with pytest.raises(ValueError, match="core_count"):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=core_count)

    @pytest.mark.parametrize("size", [0, -100, True, False, "500"])
    def test_invalid_boot_disk_size(self, size):
        with pytest.raises(ValueError, match="boot_disk_size_gb"):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=2, boot_disk_size_gb=size)

    def test_invalid_hardware_class_type(self):
        with pytest.raises(ValueError, match="hardware_class"):
            FlexibleInstanceGroupConfig(num_instances=1, core_count=2, hardware_class=123)


class TestDataprocCreateClusterOperatorValidation:
    def _make(self, **cluster_config_kwargs):
        op = DataprocCreateClusterOperator(
            task_id="create",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster_config=dict(**cluster_config_kwargs),
        )
        return op.cluster_config

    def test_standard_cluster_is_unchanged(self):
        """Regression: without flexible configs the cluster_config is unchanged."""
        config = {
            "master_config": {"num_instances": 1},
            "worker_config": {"num_instances": 2},
            "secondary_worker_config": {"num_instances": 1},
            "gce_cluster_config": {"zone_uri": "projects/p/zones/us-central1-a"},
        }
        op = DataprocCreateClusterOperator(
            task_id="create",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster_config=config,
        )
        assert op.cluster_config["master_config"]["num_instances"] == 1
        assert op.cluster_config["worker_config"]["num_instances"] == 2
        assert op.cluster_config["secondary_worker_config"]["num_instances"] == 1
        assert "master_flexible_config" not in op.cluster_config
        assert "worker_flexible_config" not in op.cluster_config
        assert "secondary_worker_flexible_config" not in op.cluster_config
        assert op.cluster_config["gce_cluster_config"]["zone_uri"].endswith("/zones/us-central1-a")

        # Verify ClusterGenerator still works as expected for standard clusters
        gen_config = ClusterGenerator(
            project_id=PROJECT_ID,
            num_workers=2,
            zone="us-central1-a",
            master_machine_type="n2-standard-4",
            worker_machine_type="n2-standard-8",
            num_preemptible_workers=1,
        ).make()
        assert gen_config["master_config"]["num_instances"] == 1
        assert gen_config["worker_config"]["num_instances"] == 2
        assert gen_config["secondary_worker_config"]["num_instances"] == 1

    def test_ho_cluster_normalizes_flexible_configs(self):
        config = self._make(
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
            secondary_worker_flexible_config=SECONDARY_FLEX,
            software_config={"image_version": "2.2"},
            gce_cluster_config={"subnetwork_uri": "default"},
        )

        assert config["master_flexible_config"] == {
            "num_instances": 1,
            "core_count": 4,
            "hardware_class": "STANDARD",
        }
        assert config["worker_flexible_config"] == {
            "num_instances": 2,
            "core_count": 8,
            "hardware_class": "PERFORMANCE",
            "boot_disk_size_gb": 500,
        }
        assert config["secondary_worker_flexible_config"] == {
            "num_instances": 3,
            "core_count": 8,
            "hardware_class": "STANDARD",
        }
        # Mutual exclusivity: no concrete instance group configs are sent.
        assert "master_config" not in config
        assert "worker_config" not in config
        assert "secondary_worker_config" not in config
        # Unrelated settings still flow through.
        assert config["software_config"]["image_version"] == "2.2"
        assert config["gce_cluster_config"]["subnetwork_uri"] == "default"

    def test_ho_cluster_without_secondary_workers(self):
        config = self._make(
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
        )
        assert "secondary_worker_flexible_config" not in config
        assert "secondary_worker_config" not in config

    def test_ho_cluster_accepts_dicts(self):
        config = self._make(
            master_flexible_config={"num_instances": 1, "core_count": 2},
            worker_flexible_config={
                "num_instances": 5,
                "core_count": 16,
                "hardware_class": "PERFORMANCE",
            },
        )
        assert config["worker_flexible_config"]["num_instances"] == 5
        assert config["worker_flexible_config"]["hardware_class"] == "PERFORMANCE"

    def test_ho_single_node_cluster(self):
        """Single-node HO: master flexible config only and allow.zero.workers property.

        Mirrors the API: a worker flexible config must not be sent for single-node
        clusters, the single-node intent is carried by the
        ``dataproc:dataproc.allow.zero.workers`` property.
        """
        config = self._make(
            master_flexible_config=MASTER_FLEX,
            software_config={"properties": {"dataproc:dataproc.allow.zero.workers": "true"}},
        )
        assert config["master_flexible_config"] == {
            "num_instances": 1,
            "core_count": 4,
            "hardware_class": "STANDARD",
        }
        assert "worker_flexible_config" not in config
        assert "secondary_worker_flexible_config" not in config
        assert "master_config" not in config
        assert "worker_config" not in config
        assert "secondary_worker_config" not in config
        assert config["software_config"]["properties"]["dataproc:dataproc.allow.zero.workers"] == "true"

    def test_ho_rejects_zero_primary_worker_instances(self):
        # The API requires a positive num_instances for the primary workers; a
        # single-node HO cluster is expressed by setting allow.zero.workers and omitting worker config.
        with pytest.raises(ValueError, match="num_instances must be positive"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 0, "core_count": 4},
            )

    def test_ho_single_node_rejects_worker_flexible_config(self):
        with pytest.raises(ValueError, match="single-node"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                software_config={"properties": {"dataproc:dataproc.allow.zero.workers": "true"}},
            )

    def test_ho_single_node_rejects_secondary_workers(self):
        with pytest.raises(ValueError, match="single-node"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                secondary_worker_flexible_config=SECONDARY_FLEX,
                software_config={"properties": {"dataproc:dataproc.allow.zero.workers": "true"}},
            )

    def test_ho_requires_master_flexible_config(self):
        with pytest.raises(ValueError, match="cluster-wide"):
            self._make(worker_flexible_config=WORKER_FLEX)
        with pytest.raises(ValueError, match="cluster-wide"):
            self._make(secondary_worker_flexible_config=SECONDARY_FLEX)

    def test_ho_requires_worker_flexible_config_unless_single_node(self):
        with pytest.raises(ValueError, match="cluster-wide"):
            self._make(master_flexible_config=MASTER_FLEX)
        with pytest.raises(ValueError, match="cluster-wide"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                secondary_worker_flexible_config=SECONDARY_FLEX,
            )

    def test_ho_rejects_concrete_secondary_workers(self):
        with pytest.raises(ValueError, match="secondary_worker_flexible_config instead"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                secondary_worker_config={"num_instances": 2},
            )

    def test_ho_rejects_explicit_zone(self):
        with pytest.raises(ValueError, match="auto-zoned"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                gce_cluster_config={"zone_uri": "projects/p/zones/us-central1-a"},
            )

    def test_ho_rejects_ha_masters(self):
        with pytest.raises(ValueError, match="num_instances must be 1"):
            self._make(
                master_flexible_config={"num_instances": 3, "core_count": 4},
                worker_flexible_config=WORKER_FLEX,
            )

    def test_ho_rejects_concrete_master_and_worker_configs(self):
        with pytest.raises(
            ValueError, match=r"master_flexible_config can't be set together with master_config"
        ):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                master_config={"num_instances": 1},
            )
        with pytest.raises(
            ValueError, match=r"worker_flexible_config can't be set together with worker_config"
        ):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config=WORKER_FLEX,
                worker_config={"num_instances": 2},
            )

    def test_ho_single_node_rejects_concrete_worker_config(self):
        with pytest.raises(
            ValueError, match=r"worker_config can't be set together with"
        ):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_config={"num_instances": 0},
                software_config={"properties": {"dataproc:dataproc.allow.zero.workers": "true"}},
            )

    def test_ho_cluster_with_none_subconfigs(self):
        config = self._make(
            master_flexible_config=MASTER_FLEX,
            worker_flexible_config=WORKER_FLEX,
            software_config=None,
            gce_cluster_config=None,
        )
        assert config["master_flexible_config"]["num_instances"] == 1
        assert config["worker_flexible_config"]["num_instances"] == 2

    def test_invalid_flexible_config_dict_key(self):
        with pytest.raises(ValueError, match="Invalid worker_flexible_config"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 1, "cores": 4},
            )

    def test_invalid_flexible_config_type(self):
        with pytest.raises(ValueError, match="must be a"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config="num_instances=2,core_count=4",
            )

    def test_invalid_core_count_in_dict_propagates(self):
        with pytest.raises(ValueError, match="core_count"):
            self._make(
                master_flexible_config=MASTER_FLEX,
                worker_flexible_config={"num_instances": 1, "core_count": 6},
            )


class TestFlexibleScaleClusterUpdate:
    def test_primary_workers_only(self):
        cluster, update_mask = flexible_scale_cluster_update(num_workers=5)
        assert cluster == {"config": {"worker_flexible_config": {"num_instances": 5}}}
        assert update_mask == {"paths": ["config.worker_flexible_config.num_instances"]}

    def test_both_node_groups(self):
        cluster, update_mask = flexible_scale_cluster_update(num_workers=5, num_secondary_workers=0)
        assert cluster == {
            "config": {
                "worker_flexible_config": {"num_instances": 5},
                "secondary_worker_flexible_config": {"num_instances": 0},
            }
        }
        assert update_mask == {
            "paths": [
                "config.worker_flexible_config.num_instances",
                "config.secondary_worker_flexible_config.num_instances",
            ]
        }
        # Never emit the legacy paths alongside the flexible ones.
        for path in update_mask["paths"]:
            assert "worker_config" not in path.replace("flexible_config", "")

    def test_requires_at_least_one_count(self):
        with pytest.raises(ValueError, match="At least one"):
            flexible_scale_cluster_update()

    def test_rejects_negative_counts(self):
        with pytest.raises(ValueError, match="num_workers"):
            flexible_scale_cluster_update(num_workers=-1)
        with pytest.raises(ValueError, match="num_secondary_workers"):
            flexible_scale_cluster_update(num_secondary_workers=-1)

    @pytest.mark.parametrize("invalid_val", [True, False, "5", 3.14])
    def test_rejects_boolean_and_non_integer_counts(self, invalid_val):
        with pytest.raises(ValueError, match="num_workers"):
            flexible_scale_cluster_update(num_workers=invalid_val)
        with pytest.raises(ValueError, match="num_secondary_workers"):
            flexible_scale_cluster_update(num_secondary_workers=invalid_val)


class TestDataprocUpdateClusterOperatorHighObtainability:
    def _make(self, cluster, paths):
        return DataprocUpdateClusterOperator(
            task_id="scale",
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster=cluster,
            update_mask={"paths": paths},
            graceful_decommission_timeout={"seconds": 0},
        )

    @pytest.mark.parametrize(
        ("legacy_group", "flexible_group"),
        [
            ("worker_config", "worker_flexible_config"),
            ("secondary_worker_config", "secondary_worker_flexible_config"),
        ],
    )
    def test_rejects_conflicting_counts_across_both_paths(self, legacy_group, flexible_group):
        with pytest.raises(ValueError, match=r"[Cc]onflicting"):
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
        with pytest.raises(ValueError, match=r"[Cc]onflicting"):
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
        with pytest.raises(ValueError, match=r"[Cc]onflicting"):
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

    @pytest.mark.parametrize(
        "path",
        [
            "config.worker_flexible_config.core_count",
            "config.worker_flexible_config.hardware_class",
            "config.worker_flexible_config.boot_disk_size_gb",
            "config.secondary_worker_flexible_config.core_count",
            "config.master_flexible_config.num_instances",
            "config.master_flexible_config.core_count",
            "config.worker_flexible_config",
        ],
    )
    def test_rejects_create_time_only_flexible_paths(self, path):
        with pytest.raises(ValueError, match="create-time only"):
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


class TestDataprocCreateClusterOperatorHighObtainability:
    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_sends_flexible_configs(self, mock_hook_cls, unused_persist):
        cluster_config = {
            "master_flexible_config": MASTER_FLEX,
            "worker_flexible_config": WORKER_FLEX,
        }
        op = DataprocCreateClusterOperator(
            task_id="create",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            cluster_config=cluster_config,
        )
        hook = mock_hook_cls.return_value
        running_cluster = Cluster(
            cluster_name=CLUSTER_NAME,
            status=ClusterStatus(state=ClusterStatus.State.RUNNING),
        )
        hook.get_cluster.return_value = running_cluster
        hook.wait_for_operation.return_value = running_cluster
        op.execute(context=mock.MagicMock())

        _, kwargs = hook.create_cluster.call_args
        sent = kwargs["cluster_config"]
        assert sent["master_flexible_config"] == {
            "num_instances": 1,
            "core_count": 4,
            "hardware_class": "STANDARD",
        }
        assert sent["worker_flexible_config"]["num_instances"] == 2
        assert "master_config" not in sent
        assert "worker_config" not in sent


class TestExampleDag:
    def test_example_dag_imports_and_has_expected_tasks(self):
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
        assert spec is not None
        assert spec.loader is not None
        example_dag_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(example_dag_mod)

        task_ids = [t for t in example_dag_mod.dag.task_ids if t != "watcher"]
        assert sorted(task_ids) == sorted(
            [
                "create_ho_cluster",
                "scale_up_ho_workers",
                "spark_pi",
                "scale_down_ho_secondary_workers",
                "delete_ho_cluster",
            ]
        )
        config = example_dag_mod.HO_CLUSTER_CONFIG
        assert "master_flexible_config" in config
        assert "worker_flexible_config" in config
        assert "secondary_worker_flexible_config" in config
        assert "master_config" not in config
