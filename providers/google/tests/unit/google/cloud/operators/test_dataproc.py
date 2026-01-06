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

import datetime as dt
import inspect
from copy import deepcopy
from unittest import mock
from unittest.mock import ANY, MagicMock, Mock, call

import pytest
from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.retry import Retry
from google.api_core.retry_async import AsyncRetry
from google.cloud import dataproc
from google.cloud.dataproc_v1 import Batch, Cluster, JobStatus
from openlineage.client.transport import HttpConfig, HttpTransport, KafkaConfig, KafkaTransport

from airflow import __version__ as AIRFLOW_VERSION
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import DAG, DagBag
from airflow.providers.common.compat.sdk import AirflowException, AirflowTaskTimeout, TaskDeferred
from airflow.providers.google.cloud.links.dataproc import (
    DATAPROC_BATCH_LINK,
    DATAPROC_CLUSTER_LINK_DEPRECATED,
    DATAPROC_JOB_LINK_DEPRECATED,
)
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteBatchOperator,
    DataprocDeleteClusterOperator,
    DataprocDiagnoseClusterOperator,
    DataprocGetBatchOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocListBatchesOperator,
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
    InstanceFlexibilityPolicy,
    InstanceSelection,
)
from airflow.providers.google.cloud.triggers.dataproc import (
    DataprocBatchTrigger,
    DataprocClusterTrigger,
    DataprocDeleteClusterTrigger,
    DataprocOperationTrigger,
    DataprocSubmitTrigger,
)
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

from tests_common.test_utils.compat import DagSerialization, timezone
from tests_common.test_utils.db import clear_db_runs, clear_db_xcom
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

AIRFLOW_VERSION_LABEL = "v" + str(AIRFLOW_VERSION).replace(".", "-").replace("+", "-")

cluster_params = inspect.signature(ClusterGenerator.__init__).parameters

DATAPROC_PATH = "airflow.providers.google.cloud.operators.dataproc.{}"
DATAPROC_TRIGGERS_PATH = "airflow.providers.google.cloud.triggers.dataproc.{}"

TASK_ID = "task-id"
GCP_PROJECT = "test-project"
GCP_REGION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEMPLATE_ID = "template_id"
CLUSTER_NAME = "cluster_name"
CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
        "min_num_instances": 1,
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
        "preemptibility": "SPOT",
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "cluster_tier": "CLUSTER_TIER_STANDARD",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {"seconds": 600}}
    ],
    "endpoint_config": {},
    "auxiliary_node_groups": [
        {
            "node_group": {
                "roles": ["DRIVER"],
                "node_group_config": {
                    "num_instances": 2,
                },
            },
            "node_group_id": "cluster_driver_pool",
        }
    ],
}
VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": "projects/project_id/locations/region/clusters/gke_cluster_name",
            "node_pool_target": [
                {
                    "node_pool": "projects/project_id/locations/region/clusters/gke_cluster_name/nodePools/dp",
                    "roles": ["DEFAULT"],
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b"3"}},
    },
    "staging_bucket": "test-staging-bucket",
}

CONFIG_WITH_CUSTOM_IMAGE_FAMILY = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/family/custom_image_family",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/family/custom_image_family",
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
        "preemptibility": "SPOT",
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {"seconds": 600}}
    ],
    "endpoint_config": {
        "enable_http_port_access": True,
    },
}

CONFIG_WITH_FLEX_MIG = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
        "preemptibility": "SPOT",
        "instance_flexibility_policy": {
            "instance_selection_list": [
                {
                    "machine_types": [
                        "projects/project_id/zones/zone/machineTypes/machine1",
                        "projects/project_id/zones/zone/machineTypes/machine2",
                    ],
                    "rank": 0,
                },
                {"machine_types": ["projects/project_id/zones/zone/machineTypes/machine3"], "rank": 1},
            ],
        },
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {"seconds": 600}}
    ],
    "endpoint_config": {},
}

CONFIG_WITH_GPU_ACCELERATOR = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
        "accelerators": {"accelerator_type_uri": "master_accelerator_type", "accelerator_count": 1},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
        "min_num_instances": 1,
        "accelerators": {"accelerator_type_uri": "worker_accelerator_type", "accelerator_count": 1},
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
        "accelerators": {"accelerator_type_uri": "secondary_worker_accelerator_type", "accelerator_count": 1},
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {"seconds": 600}}
    ],
    "endpoint_config": {},
}

LABELS = {"labels": "data", "airflow-version": AIRFLOW_VERSION_LABEL}

LABELS.update({"airflow-version": AIRFLOW_VERSION_LABEL})

CLUSTER = {"project_id": "project_id", "cluster_name": CLUSTER_NAME, "config": CONFIG, "labels": LABELS}

UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}

TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
RESULT_RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]
REQUEST_ID = "request_id_uuid"

WORKFLOW_NAME = "airflow-dataproc-test"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {
            "cluster_name": CLUSTER_NAME,
            "config": CLUSTER,
        }
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": {}}],
}
TEST_DAG_ID = "test-dataproc-operators"
DEFAULT_DATE = timezone.datetime(2020, 1, 1)
TEST_JOB_ID = "test-job"
TEST_WORKFLOW_ID = "test-workflow"

EXPECTED_LABELS = {
    "airflow-dag-id": TEST_DAG_ID,
    "airflow-dag-display-name": TEST_DAG_ID,
    "airflow-task-id": TASK_ID,
}

DATAPROC_JOB_LINK_EXPECTED = (
    f"https://console.cloud.google.com/dataproc/jobs/{TEST_JOB_ID}?region={GCP_REGION}&project={GCP_PROJECT}"
)
DATAPROC_CLUSTER_LINK_EXPECTED = (
    f"https://console.cloud.google.com/dataproc/clusters/{CLUSTER_NAME}/monitoring?"
    f"region={GCP_REGION}&project={GCP_PROJECT}"
)
DATAPROC_WORKFLOW_LINK_EXPECTED = (
    f"https://console.cloud.google.com/dataproc/workflows/instances/{GCP_REGION}/{TEST_WORKFLOW_ID}?"
    f"project={GCP_PROJECT}"
)
DATAPROC_JOB_CONF_EXPECTED = {
    "resource": TEST_JOB_ID,
    "region": GCP_REGION,
    "project_id": GCP_PROJECT,
    "url": DATAPROC_JOB_LINK_DEPRECATED,
}
DATAPROC_JOB_EXPECTED = {
    "job_id": TEST_JOB_ID,
    "region": GCP_REGION,
    "project_id": GCP_PROJECT,
}
DATAPROC_CLUSTER_CONF_EXPECTED = {
    "resource": CLUSTER_NAME,
    "region": GCP_REGION,
    "project_id": GCP_PROJECT,
    "url": DATAPROC_CLUSTER_LINK_DEPRECATED,
}
DATAPROC_CLUSTER_EXPECTED = {
    "cluster_id": CLUSTER_NAME,
    "region": GCP_REGION,
    "project_id": GCP_PROJECT,
}
DATAPROC_WORKFLOW_EXPECTED = {
    "workflow_id": TEST_WORKFLOW_ID,
    "region": GCP_REGION,
    "project_id": GCP_PROJECT,
}
BATCH_ID = "test-batch-id"
BATCH = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
EXAMPLE_CONTEXT = {
    "ti": MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=1,
        logical_date=dt.datetime(2024, 11, 11),
        dag_run=MagicMock(logical_date=dt.datetime(2024, 11, 11), clear_number=0),
    ),
    "task": mock.MagicMock(),
}
OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG = {
    "url": "https://some-custom.url",
    "endpoint": "/api/custom",
    "timeout": 123,
    "compression": "gzip",
    "custom_headers": {
        "key1": "val1",
        "key2": "val2",
    },
    "auth": {
        "type": "api_key",
        "apiKey": "secret_123",
    },
}
OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES = {
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "https://some-custom.url",
    "spark.openlineage.transport.endpoint": "/api/custom",
    "spark.openlineage.transport.auth.type": "api_key",
    "spark.openlineage.transport.auth.apiKey": "Bearer secret_123",
    "spark.openlineage.transport.compression": "gzip",
    "spark.openlineage.transport.headers.key1": "val1",
    "spark.openlineage.transport.headers.key2": "val2",
    "spark.openlineage.transport.timeoutInMillis": "123000",
}
OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES = {
    "spark.openlineage.parentJobName": "dag_id.task_id",
    "spark.openlineage.parentJobNamespace": "default",
    "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
    "spark.openlineage.rootParentJobName": "dag_id",
    "spark.openlineage.rootParentJobNamespace": "default",
    "spark.openlineage.rootParentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
}


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class DataprocTestBase:
    @classmethod
    def setup_class(cls):
        cls.dagbag = DagBag(dag_folder="/dev/null", include_examples=False)
        cls.dag = DAG(
            dag_id=TEST_DAG_ID,
            schedule=None,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
        )

    def setup_method(self):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti, "task": self.mock_ti.task}
        self.extra_links_manager_mock = Mock()
        self.extra_links_manager_mock.attach_mock(self.mock_ti, "ti")

    def tearDown(self):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti, "task": self.mock_ti.task}
        self.extra_links_manager_mock = Mock()
        self.extra_links_manager_mock.attach_mock(self.mock_ti, "ti")

    @classmethod
    def tearDownClass(cls):
        clear_db_runs()
        clear_db_xcom()


class DataprocJobTestBase(DataprocTestBase):
    @classmethod
    def setup_class(cls):
        if AIRFLOW_V_3_0_PLUS:
            cls.extra_links_expected_calls = [
                call.ti.xcom_push(key="conf", value=DATAPROC_JOB_CONF_EXPECTED),
                call.hook().wait_for_job(job_id=TEST_JOB_ID, region=GCP_REGION, project_id=GCP_PROJECT),
            ]
        else:
            cls.extra_links_expected_calls = [
                call.ti.xcom_push(key="conf", value=DATAPROC_JOB_CONF_EXPECTED, execution_date=None),
                call.hook().wait_for_job(job_id=TEST_JOB_ID, region=GCP_REGION, project_id=GCP_PROJECT),
            ]


class DataprocClusterTestBase(DataprocTestBase):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if AIRFLOW_V_3_0_PLUS:
            cls.extra_links_expected_calls_base = [
                call.ti.xcom_push(key="dataproc_cluster", value=DATAPROC_CLUSTER_EXPECTED),
            ]
        else:
            cls.extra_links_expected_calls_base = [
                call.ti.task.extra_links_params.__bool__(),
                call.ti.task.extra_links_params.keys(),
                call.ti.task.extra_links_params.keys().__iter__(),
                call.ti.xcom_push(key="dataproc_cluster", value=DATAPROC_CLUSTER_EXPECTED),
            ]


class TestsClusterGenerator:
    def test_image_version(self):
        with pytest.raises(ValueError, match="custom_image and image_version"):
            ClusterGenerator(
                custom_image="custom_image",
                image_version="image_version",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )

    def test_custom_image_family_error_with_image_version(self):
        with pytest.raises(ValueError, match="image_version and custom_image_family"):
            ClusterGenerator(
                image_version="image_version",
                custom_image_family="custom_image_family",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )

    def test_custom_image_family_error_with_custom_image(self):
        with pytest.raises(ValueError, match="custom_image and custom_image_family"):
            ClusterGenerator(
                custom_image="custom_image",
                custom_image_family="custom_image_family",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )

    def test_nodes_number(self):
        with pytest.raises(ValueError, match="Single node cannot have preemptible workers"):
            ClusterGenerator(
                num_workers=0, num_preemptible_workers=1, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
            )

    def test_min_num_workers_less_than_num_workers(self):
        with pytest.raises(
            ValueError,
            match="The value of min_num_workers must be less than or equal to num_workers. "
            r"Provided 4\(min_num_workers\) and 3\(num_workers\).",
        ):
            ClusterGenerator(
                num_workers=3, min_num_workers=4, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
            )

    def test_min_num_workers_without_num_workers(self):
        with pytest.raises(ValueError, match="Must specify num_workers when min_num_workers are provided."):
            ClusterGenerator(min_num_workers=4, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME)

    def test_build(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            min_num_workers=1,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image="custom_image",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            num_preemptible_workers=4,
            preemptibility="Spot",
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=timezone.datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
            driver_pool_id="cluster_driver_pool",
            driver_pool_size=2,
            cluster_tier="CLUSTER_TIER_STANDARD",
        )
        cluster = generator.make()
        assert cluster == CONFIG

    def test_build_with_custom_image_family(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image_family="custom_image_family",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            num_preemptible_workers=4,
            preemptibility="Spot",
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=timezone.datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
            enable_component_gateway=True,
        )
        cluster = generator.make()
        assert cluster == CONFIG_WITH_CUSTOM_IMAGE_FAMILY

    def test_build_with_flex_migs(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image="custom_image",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            num_preemptible_workers=4,
            preemptibility="Spot",
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=timezone.datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
            secondary_worker_instance_flexibility_policy=InstanceFlexibilityPolicy(
                [
                    InstanceSelection(
                        [
                            "projects/project_id/zones/zone/machineTypes/machine1",
                            "projects/project_id/zones/zone/machineTypes/machine2",
                        ],
                        0,
                    ),
                    InstanceSelection(["projects/project_id/zones/zone/machineTypes/machine3"], 1),
                ]
            ),
        )
        cluster = generator.make()
        assert cluster == CONFIG_WITH_FLEX_MIG

    def test_build_with_gpu_accelerator(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            min_num_workers=1,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image="custom_image",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            master_accelerator_type="master_accelerator_type",
            master_accelerator_count=1,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            worker_accelerator_type="worker_accelerator_type",
            worker_accelerator_count=1,
            num_preemptible_workers=4,
            secondary_worker_accelerator_type="secondary_worker_accelerator_type",
            secondary_worker_accelerator_count=1,
            preemptibility="preemptible",
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=timezone.datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
        )
        cluster = generator.make()
        assert cluster == CONFIG_WITH_GPU_ACCELERATOR

    def test_build_with_default_value_for_internal_ip_only(self):
        generator = ClusterGenerator(project_id="project_id")
        cluster = generator.make()
        assert "internal_ip_only" not in cluster["gce_cluster_config"]

    def test_build_sets_provided_value_for_internal_ip_only(self):
        for internal_ip_only in [True, False]:
            generator = ClusterGenerator(
                project_id="project_id", internal_ip_only=internal_ip_only, subnetwork_uri="subnetwork_uri"
            )
            cluster = generator.make()
            assert cluster["gce_cluster_config"]["internal_ip_only"] == internal_ip_only

    def test_build_with_cluster_tier(self):
        generator = ClusterGenerator(project_id="project_id", cluster_tier="CLUSTER_TIER_STANDARD")
        cluster = generator.make()
        assert cluster["cluster_tier"] == "CLUSTER_TIER_STANDARD"


class TestDataprocCreateClusterOperator(DataprocClusterTestBase):
    def test_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            op = DataprocCreateClusterOperator(
                task_id=TASK_ID,
                region=GCP_REGION,
                project_id=GCP_PROJECT,
                cluster_name="cluster_name",
                num_workers=2,
                zone="zone",
            )
        assert_warning("Passing cluster parameters by keywords", warnings)

        assert op.project_id == GCP_PROJECT
        assert op.cluster_name == "cluster_name"
        assert op.cluster_config["worker_config"]["num_instances"] == 2
        assert "zones/zone" in op.cluster_config["master_config"]["machine_type_uri"]

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        self.extra_links_manager_mock.attach_mock(mock_hook, "hook")
        mock_hook.return_value.create_cluster.result.return_value = None
        create_cluster_args = {
            "region": GCP_REGION,
            "project_id": GCP_PROJECT,
            "cluster_name": CLUSTER_NAME,
            "request_id": REQUEST_ID,
            "retry": RETRY,
            "timeout": TIMEOUT,
            "metadata": METADATA,
            "cluster_config": CONFIG,
            "labels": LABELS,
            "virtual_cluster_config": None,
        }
        expected_calls = [
            *self.extra_links_expected_calls_base,
            call.hook().create_cluster(**create_cluster_args),
        ]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(**create_cluster_args)

        # Test whether xcom push occurs before create cluster is called
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        to_dict_mock.assert_called_once_with(mock_hook().wait_for_operation())
        if AIRFLOW_V_3_0_PLUS:
            self.mock_ti.xcom_push.assert_called_once_with(
                key="dataproc_cluster",
                value=DATAPROC_CLUSTER_EXPECTED,
            )
        else:
            self.mock_ti.xcom_push.assert_called_once_with(
                key="dataproc_cluster",
                value=DATAPROC_CLUSTER_EXPECTED,
            )

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_in_gke(self, mock_hook, to_dict_mock):
        self.extra_links_manager_mock.attach_mock(mock_hook, "hook")
        mock_hook.return_value.create_cluster.return_value = None
        create_cluster_args = {
            "region": GCP_REGION,
            "project_id": GCP_PROJECT,
            "cluster_name": CLUSTER_NAME,
            "request_id": REQUEST_ID,
            "retry": RETRY,
            "timeout": TIMEOUT,
            "metadata": METADATA,
            "cluster_config": None,
            "labels": LABELS,
            "virtual_cluster_config": VIRTUAL_CLUSTER_CONFIG,
        }
        expected_calls = [
            *self.extra_links_expected_calls_base,
            call.hook().create_cluster(**create_cluster_args),
        ]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT,
            virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(**create_cluster_args)

        # Test whether xcom push occurs before create cluster is called
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        to_dict_mock.assert_called_once_with(mock_hook().wait_for_operation())
        if AIRFLOW_V_3_0_PLUS:
            self.mock_ti.xcom_push.assert_called_once_with(
                key="dataproc_cluster",
                value=DATAPROC_CLUSTER_EXPECTED,
            )
        else:
            self.mock_ti.xcom_push.assert_called_once_with(
                key="dataproc_cluster",
                value=DATAPROC_CLUSTER_EXPECTED,
            )

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists(self, mock_hook, to_dict_mock):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        mock_hook.return_value.get_cluster.return_value.status.state = 0
        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            virtual_cluster_config=None,
        )
        mock_hook.return_value.get_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        to_dict_mock.assert_called_once_with(mock_hook.return_value.get_cluster.return_value)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_do_not_use(self, mock_hook):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        mock_hook.return_value.get_cluster.return_value.status.state = 0
        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            use_if_exists=False,
        )
        with pytest.raises(AlreadyExists):
            op.execute(context=self.mock_context)

    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator._wait_for_cluster_in_deleting_state"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_in_error_state(self, mock_hook, mock_wait_for_deleting):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        cluster_status = mock_hook.return_value.get_cluster.return_value.status
        cluster_status.state = 0
        cluster_status.State.ERROR = 0

        mock_wait_for_deleting.return_value.get_cluster.side_effect = [NotFound]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
        )
        with pytest.raises(AirflowException):
            op.execute(context=self.mock_context)

        mock_hook.return_value.diagnose_cluster.assert_called_once_with(
            region=GCP_REGION, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
        )
        mock_hook.return_value.diagnose_cluster.return_value.result.assert_called_once_with()
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            region=GCP_REGION, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
        )

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("exponential_sleep_generator"))
    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator._create_cluster"))
    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator._get_cluster"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_in_deleting_state(
        self,
        mock_hook,
        mock_get_cluster,
        mock_create_cluster,
        mock_generator,
        to_dict_mock,
    ):
        cluster_deleting = mock.MagicMock()
        cluster_deleting.status.state = 0
        cluster_deleting.status.State.DELETING = 0

        cluster_running = mock.MagicMock()
        cluster_running.status.state = 0
        cluster_running.status.State.RUNNING = 0

        mock_create_cluster.side_effect = [AlreadyExists("test"), cluster_running]
        mock_generator.return_value = [0]
        mock_get_cluster.side_effect = [cluster_deleting, NotFound("test")]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            gcp_conn_id=GCP_CONN_ID,
        )

        op.execute(context=self.mock_context)
        calls = [mock.call(mock_hook.return_value), mock.call(mock_hook.return_value)]
        mock_get_cluster.assert_has_calls(calls)
        mock_create_cluster.assert_has_calls(calls)

        to_dict_mock.assert_called_once_with(cluster_running)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        mock_hook.return_value.create_cluster.return_value = None
        operator = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            retry=RETRY,
            timeout=TIMEOUT,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            request_id=None,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            virtual_cluster_config=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocClusterTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator.defer"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_finished_before_defer(self, mock_trigger_hook, mock_hook, mock_defer):
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.RUNNING),
        )
        mock_hook.return_value.create_cluster.return_value = cluster
        mock_hook.return_value.get_cluster.return_value = cluster
        operator = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            retry=RETRY,
            timeout=TIMEOUT,
            deferrable=True,
        )

        operator.execute(mock.MagicMock())
        assert not mock_defer.called

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            request_id=None,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            virtual_cluster_config=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_operation.assert_not_called()


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_create_cluster_operator_extra_links(
    dag_maker, create_task_instance_of_operator, mock_supervisor_comms
):
    ti = create_task_instance_of_operator(
        DataprocCreateClusterOperator,
        dag_id=TEST_DAG_ID,
        task_id=TASK_ID,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        cluster_name=CLUSTER_NAME,
        delete_on_error=True,
        gcp_conn_id=GCP_CONN_ID,
    )
    task = dag_maker.dag.get_task(ti.task_id)
    serialized_dag = dag_maker.get_serialized_data()
    # Assert operator links for serialized DAG
    deserialized_dag = DagSerialization.deserialize_dag(serialized_dag["dag"])
    operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
    assert operator_extra_link.name == "Dataproc Cluster"

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="key",
            value="",
        )
    # Assert operator link is empty when no XCom push occurred
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == ""

    ti.xcom_push(key="dataproc_cluster", value=DATAPROC_CLUSTER_EXPECTED)

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="key",
            value={"cluster_id": "cluster_name", "project_id": "test-project", "region": "test-location"},
        )
    # Assert operator links after execution
    assert (
        task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == DATAPROC_CLUSTER_LINK_EXPECTED
    )


class TestDataprocClusterDeleteOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocDeleteClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            cluster_uuid=None,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        mock_hook.return_value.create_cluster.return_value = None
        operator = DataprocDeleteClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.delete_cluster.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster_uuid=None,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocDeleteClusterTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocDeleteClusterOperator.defer"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_finished_before_defer(self, mock_trigger_hook, mock_hook, mock_defer):
        mock_hook.return_value.create_cluster.return_value = None
        mock_hook.return_value.get_cluster.side_effect = NotFound("test")
        operator = DataprocDeleteClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.delete_cluster.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster_uuid=None,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert not mock_defer.called


class TestDataprocSubmitJobOperator(DataprocJobTestBase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        xcom_push_call = call.ti.xcom_push(key="dataproc_job", value=DATAPROC_JOB_EXPECTED)
        wait_for_job_call = call.hook().wait_for_job(
            job_id=TEST_JOB_ID, region=GCP_REGION, project_id=GCP_PROJECT, timeout=None
        )

        job = {}
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID
        self.extra_links_manager_mock.attach_mock(mock_hook, "hook")

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)

        # Test whether xcom push occurs before polling for job
        assert self.extra_links_manager_mock.mock_calls.index(
            xcom_push_call
        ) < self.extra_links_manager_mock.mock_calls.index(wait_for_job_call), (
            "Xcom push for Job Link has to be done before polling for job status"
        )

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=GCP_PROJECT, region=GCP_REGION, timeout=None
        )

        self.mock_ti.xcom_push.assert_called_once_with(key="dataproc_job", value=DATAPROC_JOB_EXPECTED)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_async(self, mock_hook):
        job = {}
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            asynchronous=True,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_not_called()

        self.mock_ti.xcom_push.assert_called_once_with(key="dataproc_job", value=DATAPROC_JOB_EXPECTED)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_execute_deferrable(self, mock_trigger_hook, mock_hook):
        job = {}
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            asynchronous=True,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_not_called()

        self.mock_ti.xcom_push.assert_not_called()

        assert isinstance(exc.value.trigger, DataprocSubmitTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_PATH.format("DataprocSubmitJobOperator.defer"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook.submit_job"))
    def test_dataproc_operator_execute_async_done_before_defer(self, mock_submit_job, mock_defer, mock_hook):
        mock_submit_job.return_value.reference.job_id = TEST_JOB_ID
        job_status = mock_hook.return_value.get_job.return_value.status
        job_status.state = JobStatus.State.DONE

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job={},
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            asynchronous=True,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        op.execute(context=self.mock_context)
        assert not mock_defer.called

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection(
        self, mock_hook, mock_ol_accessible, mock_static_uuid
    ):
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    "spark.openlineage.transport.type": "console",
                },
            },
        }
        expected_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    "spark.openlineage.transport.type": "console",
                    "spark.openlineage.parentJobName": "dag_id.task_id",
                    "spark.openlineage.parentJobNamespace": "default",
                    "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
                    "spark.openlineage.rootParentJobName": "dag_id",
                    "spark.openlineage.rootParentJobNamespace": "default",
                    "spark.openlineage.rootParentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
                },
            },
        }
        context = {
            "ti": MagicMock(
                dag_id="dag_id",
                task_id="task_id",
                try_number=1,
                map_index=1,
                logical_date=dt.datetime(2024, 11, 11),
            ),
            "task": MagicMock(),
        }

        mock_ol_accessible.return_value = True

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_parent_job_info=True,
        )
        op.execute(context=context)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=expected_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_http_transport_info_injection(
        self, mock_hook, mock_ol_accessible, mock_ol_listener, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }
        expected_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=expected_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_all_info_injection(
        self, mock_hook, mock_ol_accessible, mock_ol_listener, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }
        expected_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                    **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=expected_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_unsupported_transport_info_injection(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        kafka_config = KafkaConfig(
            topic="my_topic",
            config={
                "bootstrap.servers": "test-kafka-xfgup:10009,another.host-ge7h0:100010",
                "acks": "all",
                "retries": "3",
            },
            flush=True,
            messageKey="some",
        )
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = KafkaTransport(
            kafka_config
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_when_already_present(
        self, mock_hook, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = True
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    "spark.openlineage.parentJobNamespace": "default",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_parent_job_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_when_already_present(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                    "spark.openlineage.transport.type": "console",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = True
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            # not passing openlineage_inject_parent_job_info, should be False by default
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            # not passing openlineage_inject_transport_info, should be False by default
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = False
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_parent_job_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = False
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        job_config = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://example/wordcount.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            job=job_config,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job=job_config,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_on_kill(self, mock_hook):
        job = {}
        job_id = "job_id"
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = job_id

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_on_kill=False,
        )
        op.execute(context=self.mock_context)

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            project_id=GCP_PROJECT, region=GCP_REGION, job_id=job_id
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_on_kill_after_execution_timeout(self, mock_hook):
        job = {}
        job_id = "job_id"
        mock_hook.return_value.wait_for_job.side_effect = AirflowTaskTimeout()
        mock_hook.return_value.submit_job.return_value.reference.job_id = job_id

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_on_kill=True,
        )
        with pytest.raises(AirflowTaskTimeout):
            op.execute(context=self.mock_context)

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            project_id=GCP_PROJECT, region=GCP_REGION, job_id=job_id
        )

    def test_missing_region_parameter(self):
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'region'"):
            DataprocSubmitJobOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                job={},
                gcp_conn_id=GCP_CONN_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                request_id=REQUEST_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
@mock.patch(DATAPROC_PATH.format("DataprocHook"))
def test_submit_job_operator_extra_links(
    mock_hook, dag_maker, create_task_instance_of_operator, mock_supervisor_comms
):
    mock_hook.return_value.project_id = GCP_PROJECT
    ti = create_task_instance_of_operator(
        DataprocSubmitJobOperator,
        dag_id=TEST_DAG_ID,
        task_id=TASK_ID,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        job={},
        gcp_conn_id=GCP_CONN_ID,
    )
    task = dag_maker.dag.get_task(ti.task_id)
    serialized_dag = dag_maker.get_serialized_data()

    # Assert operator links for serialized DAG
    deserialized_dag = DagSerialization.deserialize_dag(serialized_dag["dag"])
    operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
    assert operator_extra_link.name == "Dataproc Job"

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_job",
            value="",
        )

    # Assert operator link is empty when no XCom push occurred
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == ""

    ti.xcom_push(key="dataproc_job", value=DATAPROC_JOB_EXPECTED)

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_job",
            value=DATAPROC_JOB_EXPECTED,
        )

    # Assert operator links after execution
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == DATAPROC_JOB_LINK_EXPECTED


class TestDataprocUpdateClusterOperator(DataprocClusterTestBase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        self.extra_links_manager_mock.attach_mock(mock_hook, "hook")
        mock_hook.return_value.update_cluster.result.return_value = None
        cluster_decommission_timeout = {"graceful_decommission_timeout": "600s"}
        update_cluster_args = {
            "region": GCP_REGION,
            "project_id": GCP_PROJECT,
            "cluster_name": CLUSTER_NAME,
            "cluster": CLUSTER,
            "update_mask": UPDATE_MASK,
            "graceful_decommission_timeout": cluster_decommission_timeout,
            "request_id": REQUEST_ID,
            "retry": RETRY,
            "timeout": TIMEOUT,
            "metadata": METADATA,
        }
        expected_calls = [
            *self.extra_links_expected_calls_base,
            call.hook().update_cluster(**update_cluster_args),
        ]

        op = DataprocUpdateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout=cluster_decommission_timeout,
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_cluster.assert_called_once_with(**update_cluster_args)

        # Test whether the xcom push happens before updating the cluster
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        self.mock_ti.xcom_push.assert_called_once_with(
            key="dataproc_cluster",
            value=DATAPROC_CLUSTER_EXPECTED,
        )

    def test_missing_region_parameter(self):
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'region'"):
            DataprocUpdateClusterOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                cluster=CLUSTER,
                update_mask=UPDATE_MASK,
                request_id=REQUEST_ID,
                graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
                project_id=GCP_PROJECT,
                gcp_conn_id=GCP_CONN_ID,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        mock_hook.return_value.update_cluster.return_value = None
        operator = DataprocUpdateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocClusterTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator.defer"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_finished_before_defer(self, mock_trigger_hook, mock_hook, mock_defer):
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.RUNNING),
        )
        mock_hook.return_value.update_cluster.return_value = cluster
        mock_hook.return_value.get_cluster.return_value = cluster
        operator = DataprocUpdateClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert not mock_defer.called


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_update_cluster_operator_extra_links(
    dag_maker, create_task_instance_of_operator, mock_supervisor_comms
):
    ti = create_task_instance_of_operator(
        DataprocUpdateClusterOperator,
        dag_id=TEST_DAG_ID,
        task_id=TASK_ID,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
        project_id=GCP_PROJECT,
        gcp_conn_id=GCP_CONN_ID,
    )
    task = dag_maker.dag.get_task(ti.task_id)
    serialized_dag = dag_maker.get_serialized_data()

    # Assert operator links for serialized DAG
    deserialized_dag = DagSerialization.deserialize_dag(serialized_dag["dag"])
    operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
    assert operator_extra_link.name == "Dataproc Cluster"

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_cluster",
            value="",
        )
    # Assert operator link is empty when no XCom push occurred
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == ""

    ti.xcom_push(key="dataproc_cluster", value=DATAPROC_CLUSTER_EXPECTED)

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_cluster",
            value=DATAPROC_CLUSTER_EXPECTED,
        )

    # Assert operator links after execution
    assert (
        task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == DATAPROC_CLUSTER_LINK_EXPECTED
    )


class TestDataprocStartClusterOperator(DataprocClusterTestBase):
    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_to_dict):
        cluster = MagicMock()
        cluster.status.State.RUNNING = 3
        cluster.status.state = 0
        mock_hook.return_value.get_cluster.return_value = cluster

        op = DataprocStartClusterOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.return_value.get_cluster.assert_called_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.start_cluster.assert_called_once_with(
            cluster_name=CLUSTER_NAME,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_uuid=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocStopClusterOperator(DataprocClusterTestBase):
    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_to_dict):
        cluster = MagicMock()
        cluster.status.State.STOPPED = 4
        cluster.status.state = 0
        mock_hook.return_value.get_cluster.return_value = cluster

        op = DataprocStopClusterOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.return_value.get_cluster.assert_called_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.stop_cluster.assert_called_once_with(
            cluster_name=CLUSTER_NAME,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_uuid=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocInstantiateWorkflowTemplateOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        version = 6
        parameters = {}

        op = DataprocInstantiateWorkflowTemplateOperator(
            task_id=TASK_ID,
            template_id=TEMPLATE_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            version=version,
            parameters=parameters,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_workflow_template.assert_called_once_with(
            template_name=TEMPLATE_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            version=version,
            parameters=parameters,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        operator = DataprocInstantiateWorkflowTemplateOperator(
            task_id=TASK_ID,
            template_id=TEMPLATE_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            version=2,
            parameters={},
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)

        mock_hook.return_value.instantiate_workflow_template.assert_called_once()

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocOperationTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_on_kill(self, mock_hook):
        operation_name = "operation_name"
        mock_hook.return_value.instantiate_workflow_template.return_value.operation.name = operation_name
        op = DataprocInstantiateWorkflowTemplateOperator(
            task_id=TASK_ID,
            template_id=TEMPLATE_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            version=2,
            parameters={},
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_on_kill=False,
        )

        op.execute(context=mock.MagicMock())

        op.on_kill()
        mock_hook.return_value.get_operations_client.return_value.cancel_operation.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.get_operations_client.return_value.cancel_operation.assert_called_once_with(
            name=operation_name
        )


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
@mock.patch(DATAPROC_PATH.format("DataprocHook"))
def test_instantiate_workflow_operator_extra_links(
    mock_hook, dag_maker, create_task_instance_of_operator, mock_supervisor_comms
):
    mock_hook.return_value.project_id = GCP_PROJECT
    ti = create_task_instance_of_operator(
        DataprocInstantiateWorkflowTemplateOperator,
        dag_id=TEST_DAG_ID,
        task_id=TASK_ID,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        template_id=TEMPLATE_ID,
        gcp_conn_id=GCP_CONN_ID,
    )
    task = dag_maker.dag.get_task(ti.task_id)
    serialized_dag = dag_maker.get_serialized_data()

    # Assert operator links for serialized DAG
    deserialized_dag = DagSerialization.deserialize_dag(serialized_dag["dag"])
    operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
    assert operator_extra_link.name == "Dataproc Workflow"

    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_workflow",
            value="",
        )
    # Assert operator link is empty when no XCom push occurred
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == ""

    ti.xcom_push(key="dataproc_workflow", value=DATAPROC_WORKFLOW_EXPECTED)
    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_workflow",
            value=DATAPROC_WORKFLOW_EXPECTED,
        )
    # Assert operator links after execution
    assert (
        task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == DATAPROC_WORKFLOW_LINK_EXPECTED
    )


class TestDataprocWorkflowTemplateInstantiateInlineOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        template = {}

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        operator = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template={},
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)

        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once()

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocOperationTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_on_kill(self, mock_hook):
        operation_name = "operation_name"
        mock_hook.return_value.instantiate_inline_workflow_template.return_value.operation.name = (
            operation_name
        )
        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template={},
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_on_kill=False,
        )

        op.execute(context=mock.MagicMock())

        op.on_kill()
        mock_hook.return_value.get_operations_client.return_value.cancel_operation.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.get_operations_client.return_value.cancel_operation.assert_called_once_with(
            name=operation_name
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_wait_for_operation_on_execute(self, mock_hook):
        template = {}

        custom_timeout = 10800
        custom_retry = mock.MagicMock(AsyncRetry)
        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=custom_retry,
            timeout=custom_timeout,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_op = MagicMock()
        mock_hook.return_value.instantiate_inline_workflow_template.return_value = mock_op

        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.wait_for_operation.assert_called_once_with(
            timeout=custom_timeout, result_retry=custom_retry, operation=mock_op
        )
        mock_op.return_value.result.assert_not_called()

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection(
        self, mock_hook, mock_ol_accessible, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.parentJobNamespace": "test",
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
            ],
        }
        expected_template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {  # Injected properties
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.parentJobName": "dag_id.task_id",
                            "spark.openlineage.parentJobNamespace": "default",
                            "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
                            "spark.openlineage.rootParentJobName": "dag_id",
                            "spark.openlineage.rootParentJobNamespace": "default",
                            "spark.openlineage.rootParentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {  # Not modified because it's already present
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.parentJobNamespace": "test",
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {  # Not modified because it's unsupported job type
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            openlineage_inject_parent_job_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=expected_template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = True
        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                    },
                }
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            # not passing openlineage_inject_parent_job_info, should be False by default
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = False

        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                    },
                }
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            openlineage_inject_parent_job_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection(
        self, mock_hook, mock_ol_accessible, mock_ol_listener, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )

        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.transport.type": "console",
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
            ],
        }
        expected_template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {  # Injected properties
                            "spark.sql.shuffle.partitions": "1",
                            **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {  # Not modified because it's already present
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.transport.type": "console",
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {  # Not modified because it's unsupported job type
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=expected_template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_all_info_injection(
        self, mock_hook, mock_ol_accessible, mock_ol_listener, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.transport.type": "console",
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
                {
                    "step_id": "job_4",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.parentJobNamespace": "test",
                        },
                    },
                },
            ],
        }
        expected_template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {  # Injected all properties
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                            **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                        },
                    },
                },
                {
                    "step_id": "job_2",
                    "pyspark_job": {  # Transport not added because it's already present
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.transport.type": "console",
                            **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                        },
                    },
                },
                {
                    "step_id": "job_3",
                    "hive_job": {  # Not modified because it's unsupported job type
                        "main_python_file_uri": "gs://bucket3/hive_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                        },
                    },
                },
                {
                    "step_id": "job_4",
                    "pyspark_job": {  # Parent not added because it's already present
                        "main_python_file_uri": "gs://bucket2/spark_job.py",
                        "properties": {
                            "spark.sql.shuffle.partitions": "1",
                            "spark.openlineage.parentJobNamespace": "test",
                            **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                        },
                    },
                },
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=expected_template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig("https://some-custom.url")
        )

        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                    },
                }
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            # not passing openlineage_inject_transport_info, should be False by default
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = False
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig("https://some-custom.url")
        )

        template = {
            **WORKFLOW_TEMPLATE,
            "jobs": [
                {
                    "step_id": "job_1",
                    "pyspark_job": {
                        "main_python_file_uri": "gs://bucket1/spark_job.py",
                    },
                }
            ],
        }

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            openlineage_inject_transport_info=True,
        )
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
@mock.patch(DATAPROC_PATH.format("DataprocHook"))
def test_instantiate_inline_workflow_operator_extra_links(
    mock_hook, dag_maker, create_task_instance_of_operator, mock_supervisor_comms
):
    mock_hook.return_value.project_id = GCP_PROJECT
    ti = create_task_instance_of_operator(
        DataprocInstantiateInlineWorkflowTemplateOperator,
        dag_id=TEST_DAG_ID,
        task_id=TASK_ID,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        template={},
        gcp_conn_id=GCP_CONN_ID,
    )
    task = dag_maker.dag.get_task(ti.task_id)
    serialized_dag = dag_maker.get_serialized_data()

    # Assert operator links for serialized DAG
    deserialized_dag = DagSerialization.deserialize_dag(serialized_dag["dag"])
    operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
    assert operator_extra_link.name == "Dataproc Workflow"
    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_workflow",
            value="",
        )
    # Assert operator link is empty when no XCom push occurred
    assert task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == ""

    ti.xcom_push(key="dataproc_workflow", value=DATAPROC_WORKFLOW_EXPECTED)
    if AIRFLOW_V_3_0_PLUS:
        mock_supervisor_comms.send.return_value = XComResult(
            key="dataproc_workflow", value=DATAPROC_WORKFLOW_EXPECTED
        )

    # Assert operator links after execution
    assert (
        task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key) == DATAPROC_WORKFLOW_LINK_EXPECTED
    )


class TestDataprocCreateWorkflowTemplateOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocCreateWorkflowTemplateOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            template=WORKFLOW_TEMPLATE,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_workflow_template.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            template=WORKFLOW_TEMPLATE,
        )

    def test_missing_region_parameter(self):
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'region'"):
            DataprocCreateWorkflowTemplateOperator(
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                project_id=GCP_PROJECT,
                retry=RETRY,
                timeout=TIMEOUT,
                metadata=METADATA,
                template=WORKFLOW_TEMPLATE,
            )


class TestDataprocCreateBatchOperator:
    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, to_dict_mock, mock_log):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        batch_state_succeeded = Batch(state=Batch.State.SUCCEEDED)
        mock_hook.return_value.wait_for_batch.return_value = batch_state_succeeded

        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        to_dict_mock.assert_called_once_with(batch_state_succeeded)
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)
        mock_log.info.assert_has_calls(
            [
                mock.call("Starting batch %s", BATCH_ID),
                mock.call("The batch %s was created.", BATCH_ID),
                mock.call("Waiting for the completion of batch job %s", BATCH_ID),
                mock.call("Batch job %s completed.\nDriver logs: %s", BATCH_ID, logs_link),
            ]
        )

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_with_result_retry(self, mock_hook, to_dict_mock):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            result_retry=RESULT_RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_batch_failed(self, mock_hook, to_dict_mock):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.FAILED)
        with pytest.raises(AirflowException):
            op.execute(context=MagicMock())

    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_batch_already_exists_succeeds(self, mock_hook, mock_log):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.create_batch.side_effect = AlreadyExists("")
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED.name)

        op.execute(context=MagicMock())
        mock_hook.return_value.wait_for_batch.assert_called_once_with(
            batch_id=BATCH_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        # Check for succeeded run
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)

        mock_log.info.assert_has_calls(
            [
                mock.call(
                    "Batch with given id already exists.",
                ),
                mock.call("Attaching to the job %s if it is still running.", BATCH_ID),
                mock.call("Waiting for the completion of batch job %s", BATCH_ID),
                mock.call("Batch job %s completed.\nDriver logs: %s", BATCH_ID, logs_link),
            ]
        )

    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_batch_already_exists_fails(self, mock_hook, mock_log):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.create_batch.side_effect = AlreadyExists("")
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.FAILED.name)

        with pytest.raises(AirflowException) as exc:
            op.execute(context=MagicMock())
        # Check msg for FAILED batch state
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)
        assert str(exc.value) == (f"Batch job {BATCH_ID} failed with error: .\nDriver logs: {logs_link}")
        mock_hook.return_value.wait_for_batch.assert_called_once_with(
            batch_id=BATCH_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        # Check logs for AlreadyExists being called
        mock_log.info.assert_any_call("Batch with given id already exists.")

    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_batch_already_exists_cancelled(self, mock_hook, mock_log):
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.create_batch.side_effect = AlreadyExists("")
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.CANCELLED)

        with pytest.raises(AirflowException) as exc:
            op.execute(context=MagicMock())
        # Check msg for CANCELLED batch state
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)
        assert str(exc.value) == f"Batch job {BATCH_ID} was cancelled.\nDriver logs: {logs_link}"

        mock_hook.return_value.wait_for_batch.assert_called_once_with(
            batch_id=BATCH_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        # Check logs for AlreadyExists being called
        mock_log.info.assert_any_call("Batch with given id already exists.")

    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection(
        self,
        mock_hook,
        to_dict_mock,
        mock_ol_accessible,
        mock_static_uuid,
        mock_log,
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        expected_batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
            "runtime_config": {"properties": OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_parent_job_info=True,
            dag=DAG(dag_id=TEST_DAG_ID),
        )
        batch_state_succeeded = Batch(state=Batch.State.SUCCEEDED)
        mock_hook.return_value.wait_for_batch.return_value = batch_state_succeeded
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=expected_batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        to_dict_mock.assert_called_once_with(batch_state_succeeded)
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)
        # Check SUCCEED run from the logs
        mock_log.info.assert_any_call("Batch job %s completed.\nDriver logs: %s", BATCH_ID, logs_link)

    @mock.patch.object(DataprocCreateBatchOperator, "log", new_callable=mock.MagicMock)
    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection(
        self, mock_hook, to_dict_mock, mock_ol_accessible, mock_ol_listener, mock_static_uuid, mock_log
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        expected_batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
            "runtime_config": {"properties": OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_transport_info=True,
            dag=DAG(dag_id=TEST_DAG_ID),
        )
        batch_state_succeeded = Batch(state=Batch.State.SUCCEEDED)
        mock_hook.return_value.wait_for_batch.return_value = batch_state_succeeded
        mock_hook.return_value.create_batch.return_value.metadata.batch = f"prefix/{BATCH_ID}"
        op.execute(context=EXAMPLE_CONTEXT)

        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=expected_batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        to_dict_mock.assert_called_once_with(batch_state_succeeded)
        logs_link = DATAPROC_BATCH_LINK.format(region=GCP_REGION, project_id=GCP_PROJECT, batch_id=BATCH_ID)
        # Verify logs for successful run
        mock_log.info.assert_any_call(
            "Batch job %s completed.\nDriver logs: %s",
            BATCH_ID,
            logs_link,
        )

    @mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_all_info_injection(
        self, mock_hook, to_dict_mock, mock_ol_accessible, mock_ol_listener, mock_static_uuid
    ):
        mock_ol_accessible.return_value = True
        mock_static_uuid.return_value = "01931885-2800-7be7-aa8d-aaa15c337267"
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        expected_batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
            "runtime_config": {
                "properties": {
                    **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                    **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                }
            },
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
            dag=DAG(dag_id=TEST_DAG_ID),
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=expected_batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_when_already_present(
        self, mock_hook, to_dict_mock, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = True
        batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
            "runtime_config": {
                "properties": {
                    "spark.openlineage.parentJobName": "dag_id.task_id",
                }
            },
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_parent_job_info=True,
            dag=DAG(dag_id=TEST_DAG_ID),
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_when_already_present(
        self, mock_hook, to_dict_mock, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
            "runtime_config": {
                "properties": {
                    "spark.openlineage.transport.type": "console",
                }
            },
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_transport_info=True,
            dag=DAG(dag_id=TEST_DAG_ID),
        )
        mock_hook.return_value.wait_for_operation.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, to_dict_mock, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = True
        batch = {
            **BATCH,
            "runtime_config": {"properties": {}},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            # not passing openlineage_inject_parent_job_info, should be False by default
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_by_default_unless_enabled(
        self, mock_hook, to_dict_mock, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = True
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        batch = {
            **BATCH,
            "runtime_config": {"properties": {}},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            # not passing openlineage_inject_transport_info, should be False by default
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_parent_job_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, to_dict_mock, mock_ol_accessible
    ):
        mock_ol_accessible.return_value = False
        batch = {
            **BATCH,
            "runtime_config": {"properties": {}},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_parent_job_info=True,
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @mock.patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_openlineage_transport_info_injection_skipped_when_ol_not_accessible(
        self, mock_hook, to_dict_mock, mock_ol_accessible, mock_ol_listener
    ):
        mock_ol_accessible.return_value = False
        fake_listener = mock.MagicMock()
        mock_ol_listener.return_value = fake_listener
        fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
        )
        batch = {
            **BATCH,
            "runtime_config": {"properties": {}},
        }
        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            openlineage_inject_transport_info=True,
        )
        mock_hook.return_value.wait_for_batch.return_value = Batch(state=Batch.State.SUCCEEDED)
        op.execute(context=EXAMPLE_CONTEXT)
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=batch,
            batch_id=BATCH_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @staticmethod
    def __assert_batch_create(mock_hook, expected_batch):
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=ANY,
            project_id=ANY,
            batch=expected_batch,
            batch_id=ANY,
            request_id=ANY,
            retry=ANY,
            timeout=ANY,
            metadata=ANY,
        )

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_asdict_labels_updated(self, mock_hook, to_dict_mock):
        expected_batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
        }
        DataprocCreateBatchOperator(
            task_id=TASK_ID,
            dag=DAG(dag_id=TEST_DAG_ID),
            batch=BATCH,
            region=GCP_REGION,
        ).execute(context=EXAMPLE_CONTEXT)

        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, expected_batch)

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_asdict_labels_uppercase_transformed(self, mock_hook, to_dict_mock):
        expected_batch = {
            **BATCH,
            "labels": EXPECTED_LABELS,
        }
        DataprocCreateBatchOperator(
            task_id=TASK_ID,
            dag=DAG(dag_id=TEST_DAG_ID),
            batch=BATCH,
            region=GCP_REGION,
        ).execute(context=EXAMPLE_CONTEXT)

        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, expected_batch)

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_asdict_taskid_max_length_labels_updated(self, mock_hook, to_dict_mock):
        long_task_id = "a" * 63
        expected_batch = {
            **BATCH,
            "labels": {
                "airflow-dag-id": TEST_DAG_ID,
                "airflow-dag-display-name": TEST_DAG_ID,
                "airflow-task-id": long_task_id,
            },
        }
        DataprocCreateBatchOperator(
            task_id=long_task_id,
            dag=DAG(dag_id=TEST_DAG_ID),
            batch=BATCH,
            region=GCP_REGION,
        ).execute(context=EXAMPLE_CONTEXT)

        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, expected_batch)

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_invalid_taskid_labels_ignored(self, mock_hook, to_dict_mock):
        DataprocCreateBatchOperator(
            task_id=".task-id",
            dag=DAG(dag_id=TEST_DAG_ID),
            batch=BATCH,
            region=GCP_REGION,
        ).execute(context=EXAMPLE_CONTEXT)

        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, BATCH)

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_long_taskid_labels_ignored(self, mock_hook, to_dict_mock):
        DataprocCreateBatchOperator(
            task_id="a" * 64,
            dag=DAG(dag_id=TEST_DAG_ID),
            batch=BATCH,
            region=GCP_REGION,
        ).execute(context=EXAMPLE_CONTEXT)

        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, BATCH)

    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_create_batch_asobj_labels_updated(self, mock_hook, to_dict_mock):
        batch = Batch(name="test")
        batch.labels["foo"] = "bar"
        expected_batch = deepcopy(batch)
        expected_batch.labels.update(EXPECTED_LABELS)
        dag = DAG(dag_id=TEST_DAG_ID)

        DataprocCreateBatchOperator(task_id=TASK_ID, batch=batch, region=GCP_REGION, dag=dag).execute(
            context=EXAMPLE_CONTEXT
        )
        TestDataprocCreateBatchOperator.__assert_batch_create(mock_hook, expected_batch)


class TestDataprocDeleteBatchOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocDeleteBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            batch_id=BATCH_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_batch.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            batch_id=BATCH_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocGetBatchOperator:
    @mock.patch(DATAPROC_PATH.format("Batch.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = DataprocGetBatchOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            batch_id=BATCH_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_batch.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            batch_id=BATCH_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocListBatchesOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = 'batch_id=~"a-batch-id*" AND create_time>="2023-07-05T14:25:04.643818Z"'
        order_by = "create_time desc"

        op = DataprocListBatchesOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            filter=filter,
            order_by=order_by,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_batches.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            filter=filter,
            order_by=order_by,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_execute_deferrable(self, mock_trigger_hook, mock_hook):
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID

        op = DataprocCreateBatchOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch=BATCH,
            batch_id="batch_id",
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_batch.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            batch_id="batch_id",
            batch=BATCH,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_not_called()

        assert isinstance(exc.value.trigger, DataprocBatchTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestDataprocDiagnoseClusterOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocDiagnoseClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.diagnose_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            tarball_gcs_dir=None,
            diagnosis_interval=None,
            jobs=None,
            yarn_application_ids=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    @mock.patch(DATAPROC_TRIGGERS_PATH.format("DataprocAsyncHook"))
    def test_create_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        mock_hook.return_value.create_cluster.return_value = None
        operator = DataprocDiagnoseClusterOperator(
            task_id=TASK_ID,
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.diagnose_cluster.assert_called_once_with(
            region=GCP_REGION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            tarball_gcs_dir=None,
            diagnosis_interval=None,
            jobs=None,
            yarn_application_ids=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        mock_hook.return_value.wait_for_operation.assert_not_called()
        assert isinstance(exc.value.trigger, DataprocOperationTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME
