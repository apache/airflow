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

from google.cloud.dataproc_v1 import Cluster, ClusterStatus
from google.cloud.dataproc_v1.types import FlexibleInstanceGroupConfig

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocUpdateClusterOperator,
)

PROJECT_ID = "test-project"
REGION = "us-central1"
CLUSTER_NAME = "test-ho-cluster"

OPERATORS_MODULE = "airflow.providers.google.cloud.operators.dataproc"


class TestProtoClusterFlexibleConfig:
    def test_proto_cluster_supports_all_flexible_config_fields(self):
        # Verifies google-cloud-dataproc>=5.32.0 ClusterConfig fields are accepted on proto Cluster
        cluster = Cluster(
            {
                "config": {
                    "master_flexible_config": {"num_instances": 1, "core_count": 2, "boot_disk_size_gb": 32},
                    "worker_flexible_config": {"num_instances": 4, "core_count": 2, "boot_disk_size_gb": 32},
                    "secondary_worker_flexible_config": {
                        "num_instances": 1,
                        "core_count": 2,
                        "boot_disk_size_gb": 32,
                    },
                }
            }
        )
        assert cluster.config.master_flexible_config.num_instances == 1
        assert cluster.config.master_flexible_config.core_count == 2
        assert cluster.config.master_flexible_config.boot_disk_size_gb == 32
        assert cluster.config.worker_flexible_config.num_instances == 4
        assert cluster.config.worker_flexible_config.core_count == 2
        assert cluster.config.worker_flexible_config.boot_disk_size_gb == 32
        assert cluster.config.secondary_worker_flexible_config.num_instances == 1
        assert cluster.config.secondary_worker_flexible_config.core_count == 2
        assert cluster.config.secondary_worker_flexible_config.boot_disk_size_gb == 32

    def test_proto_flexible_instance_group_config_type(self):
        # Verifies the native proto-plus FlexibleInstanceGroupConfig type works as expected
        flex_cfg = FlexibleInstanceGroupConfig(
            num_instances=1,
            core_count=2,
            hardware_class=FlexibleInstanceGroupConfig.HardwareClass.STANDARD,
            boot_disk_size_gb=32,
        )
        assert flex_cfg.num_instances == 1
        assert flex_cfg.core_count == 2
        assert flex_cfg.boot_disk_size_gb == 32
        assert flex_cfg.hardware_class == FlexibleInstanceGroupConfig.HardwareClass.STANDARD


class TestDataprocCreateClusterOperatorHighObtainability:
    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_passes_flexible_cluster_config_dict(self, mock_hook_cls, unused_persist):
        cluster_config = {
            "master_flexible_config": {
                "num_instances": 1,
                "core_count": 2,
                "hardware_class": "STANDARD",
                "boot_disk_size_gb": 32,
            },
            "worker_flexible_config": {
                "num_instances": 4,
                "core_count": 2,
                "hardware_class": "STANDARD",
                "boot_disk_size_gb": 32,
            },
            "secondary_worker_flexible_config": {
                "num_instances": 1,
                "core_count": 2,
                "hardware_class": "STANDARD",
                "boot_disk_size_gb": 32,
            },
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
        assert sent == cluster_config

    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_passes_flexible_cluster_config_with_native_types(self, mock_hook_cls, unused_persist):
        cluster_config = {
            "master_flexible_config": FlexibleInstanceGroupConfig(
                num_instances=1,
                core_count=2,
                hardware_class=FlexibleInstanceGroupConfig.HardwareClass.STANDARD,
                boot_disk_size_gb=32,
            ),
            "worker_flexible_config": FlexibleInstanceGroupConfig(
                num_instances=4,
                core_count=2,
                hardware_class=FlexibleInstanceGroupConfig.HardwareClass.STANDARD,
                boot_disk_size_gb=32,
            ),
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
        assert sent == cluster_config


class TestDataprocUpdateClusterOperatorHighObtainability:
    @mock.patch(f"{OPERATORS_MODULE}.DataprocClusterLink.persist")
    @mock.patch(f"{OPERATORS_MODULE}.DataprocHook")
    def test_execute_passes_flexible_update(self, mock_hook_cls, unused_persist):
        cluster = {
            "config": {
                "worker_flexible_config": {"num_instances": 3},
                "secondary_worker_flexible_config": {"num_instances": 0},
            }
        }
        update_mask = {
            "paths": [
                "config.worker_flexible_config.num_instances",
                "config.secondary_worker_flexible_config.num_instances",
            ]
        }
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
            cluster=cluster,
            update_mask=update_mask,
            graceful_decommission_timeout={"seconds": 600},
            request_id=None,
            retry=mock.ANY,
            timeout=None,
            metadata=(),
        )


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
        spec = importlib.util.spec_from_file_location("example_dataproc_cluster_high_obtainability", dag_path)
        assert spec is not None
        assert spec.loader is not None
        example_dag_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(example_dag_mod)

        task_ids = [t for t in example_dag_mod.dag.task_ids if t != "watcher"]
        assert sorted(task_ids) == sorted(
            [
                "create_ho_cluster",
                "scale_ho_cluster",
                "spark_pi",
                "delete_ho_cluster",
            ]
        )
        config = example_dag_mod.HO_CLUSTER_CONFIG
        assert "master_flexible_config" in config
        assert "worker_flexible_config" in config
        assert "secondary_worker_flexible_config" in config
        assert "master_config" not in config

        # Verify minimized resources (2 cores, 32GB boot disk)
        assert config["master_flexible_config"]["core_count"] == 2
        assert config["master_flexible_config"]["boot_disk_size_gb"] == 32
        assert config["worker_flexible_config"]["core_count"] == 2
        assert config["worker_flexible_config"]["boot_disk_size_gb"] == 32
        assert config["secondary_worker_flexible_config"]["core_count"] == 2
        assert config["secondary_worker_flexible_config"]["boot_disk_size_gb"] == 32
