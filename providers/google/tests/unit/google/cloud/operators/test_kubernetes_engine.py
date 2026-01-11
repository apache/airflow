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

from copy import deepcopy
from unittest import mock
from unittest.mock import PropertyMock, call

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud.container_v1.types import Cluster, NodePool

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.operators.job import (
    KubernetesDeleteJobOperator,
    KubernetesJobOperator,
)
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesInstallKueueOperator,
    KubernetesStartKueueJobOperator,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEClusterAuthDetails,
    GKECreateClusterOperator,
    GKECreateCustomResourceOperator,
    GKEDeleteClusterOperator,
    GKEDeleteCustomResourceOperator,
    GKEDeleteJobOperator,
    GKEDescribeJobOperator,
    GKEListJobsOperator,
    GKEOperatorMixin,
    GKEResumeJobOperator,
    GKEStartJobOperator,
    GKEStartKueueInsideClusterOperator,
    GKEStartKueueJobOperator,
    GKEStartPodOperator,
    GKESuspendJobOperator,
)

pytestmark = pytest.mark.filterwarnings("error::airflow.exceptions.AirflowProviderDeprecationWarning")

TEST_PROJECT_ID = "test-id"
TEST_LOCATION = "test-location"
TEST_TASK_ID = "test-task-id"
TEST_CONN_ID = "test-conn-id"
TEST_IMPERSONATION_CHAIN = "test-sa-@google.com"

TEST_OPERATION_NAME = "test-operation-name"
TEST_SELF_LINK = "test-self-link"
TEST_TARGET_LINK = "test-target-link"
TEST_IMAGE = "bash"
TEST_POLL_INTERVAL = 20.0

GKE_CLUSTER_NAME = "test-cluster-name"
GKE_CLUSTER_ENDPOINT = "test-host"
GKE_CLUSTER_PRIVATE_ENDPOINT = "test-private-host"
GKE_CLUSTER_DNS_ENDPOINT = f"gke-dns-endpoint.{TEST_LOCATION}.gke.goog"
GKE_CLUSTER_URL = f"https://{GKE_CLUSTER_ENDPOINT}"
GKE_CLUSTER_PRIVATE_URL = f"https://{GKE_CLUSTER_PRIVATE_ENDPOINT}"
GKE_CLUSTER_DNS_URL = f"https://{GKE_CLUSTER_DNS_ENDPOINT}"
GKE_SSL_CA_CERT = "TEST_SSL_CA_CERT_CONTENT"

GKE_CLUSTER_CREATE_BODY_DICT = {
    "name": GKE_CLUSTER_NAME,
    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
}
GKE_CLUSTER_CREATE_BODY_OBJECT = Cluster(
    name=GKE_CLUSTER_NAME, node_pools=[NodePool(name="a_node_pool", initial_node_count=1)]
)
GKE_CLUSTER_CREATE_BODY_DICT_DEPRECATED = {"name": GKE_CLUSTER_NAME, "initial_node_count": 1}
GKE_CLUSTER_CREATE_BODY_OBJECT_DEPRECATED = Cluster(name=GKE_CLUSTER_NAME, initial_node_count=1)

K8S_KUEUE_VERSION = "v0.9.1"
K8S_JOB_NAME = "test-job"
K8S_POD_NAME = "test-pod"
K8S_NAMESPACE = "default"

GKE_OPERATORS_PATH = "airflow.providers.google.cloud.operators.kubernetes_engine.{}"


class TestGKEClusterAuthDetails:
    @pytest.mark.parametrize(
        (
            "use_dns_endpoint",
            "use_internal_ip",
            "endpoint",
            "private_endpoint",
            "dns_endpoint",
            "expected_cluster_url",
        ),
        [
            (
                False,
                False,
                GKE_CLUSTER_ENDPOINT,
                GKE_CLUSTER_PRIVATE_ENDPOINT,
                GKE_CLUSTER_DNS_ENDPOINT,
                GKE_CLUSTER_URL,
            ),
            (
                False,
                True,
                GKE_CLUSTER_ENDPOINT,
                GKE_CLUSTER_PRIVATE_ENDPOINT,
                GKE_CLUSTER_DNS_ENDPOINT,
                GKE_CLUSTER_PRIVATE_URL,
            ),
            (
                True,
                False,
                GKE_CLUSTER_ENDPOINT,
                GKE_CLUSTER_PRIVATE_ENDPOINT,
                GKE_CLUSTER_DNS_ENDPOINT,
                GKE_CLUSTER_DNS_URL,
            ),
            (
                True,
                True,
                GKE_CLUSTER_ENDPOINT,
                GKE_CLUSTER_PRIVATE_ENDPOINT,
                GKE_CLUSTER_DNS_ENDPOINT,
                GKE_CLUSTER_DNS_URL,
            ),
        ],
    )
    def test_fetch_cluster_info(
        self,
        use_dns_endpoint,
        use_internal_ip,
        endpoint,
        private_endpoint,
        dns_endpoint,
        expected_cluster_url,
    ):
        mock_cluster = mock.MagicMock(
            endpoint=endpoint,
            private_cluster_config=mock.MagicMock(private_endpoint=private_endpoint),
            control_plane_endpoints_config=mock.MagicMock(
                dns_endpoint_config=mock.MagicMock(endpoint=dns_endpoint)
            ),
            master_auth=mock.MagicMock(cluster_ca_certificate=GKE_SSL_CA_CERT),
        )
        mock_cluster_hook = mock.MagicMock(get_cluster=mock.MagicMock(return_value=mock_cluster))

        cluster_auth_details = GKEClusterAuthDetails(
            cluster_name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
            use_internal_ip=use_internal_ip,
            use_dns_endpoint=use_dns_endpoint,
            cluster_hook=mock_cluster_hook,
        )

        cluster_url, ssl_ca_cert = cluster_auth_details.fetch_cluster_info()
        assert expected_cluster_url == cluster_url
        assert ssl_ca_cert == GKE_SSL_CA_CERT
        mock_cluster_hook.get_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME, project_id=TEST_PROJECT_ID
        )


class TestGKEOperatorMixin:
    def setup_method(self):
        self.operator = GKEOperatorMixin()
        self.operator.gcp_conn_id = TEST_CONN_ID
        self.operator.cluster_name = GKE_CLUSTER_NAME
        self.operator.location = TEST_LOCATION
        self.operator.impersonation_chain = TEST_IMPERSONATION_CHAIN
        self.operator.project_id = TEST_PROJECT_ID
        self.operator.use_internal_ip = False
        self.operator.use_dns_endpoint = False

    def test_template_fields(self):
        expected_template_fields = {
            "location",
            "cluster_name",
            "use_internal_ip",
            "use_dns_endpoint",
            "project_id",
            "gcp_conn_id",
            "impersonation_chain",
        }
        assert set(self.operator.template_fields) == expected_template_fields

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_cluster_hook(self, mock_cluster_hook):
        actual_hook = self.operator.cluster_hook

        assert actual_hook == mock_cluster_hook.return_value
        mock_cluster_hook.assert_called_once_with(
            gcp_conn_id=TEST_CONN_ID,
            location=TEST_LOCATION,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    def test_hook(self, mock_hook, mock_cluster_auth_details, mock_cluster_hook):
        mock_cluster_auth_details.return_value.fetch_cluster_info.return_value = (
            GKE_CLUSTER_URL,
            GKE_SSL_CA_CERT,
        )
        actual_hook = self.operator.hook

        assert actual_hook == mock_hook.return_value
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            cluster_url=GKE_CLUSTER_URL,
            ssl_ca_cert=GKE_SSL_CA_CERT,
            enable_tcp_keepalive=False,
            use_dns_endpoint=False,
        )

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails"))
    def test_cluster_info(self, mock_cluster_auth_details, mock_cluster_hook):
        mock_fetch_cluster_info = mock_cluster_auth_details.return_value.fetch_cluster_info
        mock_fetch_cluster_info.return_value = (GKE_CLUSTER_URL, GKE_SSL_CA_CERT)

        cluster_info = self.operator.cluster_info

        assert cluster_info == (GKE_CLUSTER_URL, GKE_SSL_CA_CERT)
        mock_cluster_auth_details.assert_called_once_with(
            cluster_name=self.operator.cluster_name,
            project_id=self.operator.project_id,
            use_internal_ip=self.operator.use_internal_ip,
            use_dns_endpoint=self.operator.use_dns_endpoint,
            cluster_hook=self.operator.cluster_hook,
        )
        mock_fetch_cluster_info.assert_called_once_with()

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEOperatorMixin.cluster_info"), new_callable=PropertyMock)
    def test_cluster_url(self, mock_cluster_info, mock_cluster_hook):
        mock_cluster_info.return_value = (GKE_CLUSTER_URL, GKE_SSL_CA_CERT)

        cluster_url = self.operator.cluster_url

        assert cluster_url == GKE_CLUSTER_URL

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEOperatorMixin.cluster_info"), new_callable=PropertyMock)
    def test_ssl_ca_cert(self, mock_cluster_info, mock_cluster_hook):
        mock_cluster_info.return_value = (GKE_CLUSTER_URL, GKE_SSL_CA_CERT)

        ssl_ca_cert = self.operator.ssl_ca_cert

        assert ssl_ca_cert == GKE_SSL_CA_CERT


class TestGKEDeleteClusterOperator:
    def setup_method(self):
        self.operator = GKEDeleteClusterOperator(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_template_fields(self):
        expected_template_fields = {"api_version", "deferrable", "poll_interval"} | set(
            GKEOperatorMixin.template_fields
        )
        assert set(self.operator.template_fields) == expected_template_fields

    @pytest.mark.parametrize("missing_parameter", ["project_id", "location", "cluster_name"])
    def test_check_input(self, missing_parameter):
        setattr(self.operator, missing_parameter, None)

        with pytest.raises(AirflowException):
            self.operator._check_input()

    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_cluster_hook):
        mock_delete_cluster = mock_cluster_hook.return_value.delete_cluster
        mock_operation = mock_delete_cluster.return_value
        mock_operation.self_link = TEST_SELF_LINK

        result = self.operator.execute(context=mock.MagicMock())

        mock_delete_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
            wait_to_complete=True,
        )
        assert result == TEST_SELF_LINK

    @mock.patch(GKE_OPERATORS_PATH.format("GKEOperationTrigger"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEDeleteClusterOperator.defer"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_deferrable(self, mock_cluster_hook, mock_defer, mock_trigger):
        mock_delete_cluster = mock_cluster_hook.return_value.delete_cluster
        mock_operation = mock_delete_cluster.return_value
        mock_operation.name = TEST_OPERATION_NAME
        mock_trigger_instance = mock_trigger.return_value
        self.operator.deferrable = True

        self.operator.execute(context=mock.MagicMock())

        mock_delete_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
            wait_to_complete=False,
        )
        mock_trigger.assert_called_once_with(
            operation_name=TEST_OPERATION_NAME,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poll_interval=10,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger_instance,
            method_name="execute_complete",
        )

    @mock.patch(GKE_OPERATORS_PATH.format("GKEDeleteClusterOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute_complete(self, cluster_hook, mock_log):
        mock_get_operation = cluster_hook.return_value.get_operation
        mock_get_operation.return_value.self_link = TEST_SELF_LINK
        expected_status, expected_message = "success", "test-message"
        event = dict(status=expected_status, message=expected_message, operation_name=TEST_OPERATION_NAME)

        result = self.operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.info.assert_called_once_with(expected_message)
        mock_get_operation.assert_called_once_with(operation_name=TEST_OPERATION_NAME)
        assert result == TEST_SELF_LINK

    @pytest.mark.parametrize("status", ["failed", "error"])
    @mock.patch(GKE_OPERATORS_PATH.format("GKEDeleteClusterOperator.log"))
    def test_execute_complete_error(self, mock_log, status):
        expected_message = "test-message"
        event = dict(status=status, message=expected_message)

        with pytest.raises(AirflowException):
            self.operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.exception.assert_called_once_with("Trigger ended with one of the failed statuses.")


class TestGKECreateClusterOperator:
    def setup_method(self):
        self.operator = GKECreateClusterOperator(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            body=GKE_CLUSTER_CREATE_BODY_DICT,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_template_fields(self):
        expected_template_fields = {"body", "api_version", "deferrable", "poll_interval"} | set(
            GKEOperatorMixin.template_fields
        )
        assert set(GKECreateClusterOperator.template_fields) == expected_template_fields

    @pytest.mark.parametrize("body", [GKE_CLUSTER_CREATE_BODY_DICT, GKE_CLUSTER_CREATE_BODY_OBJECT])
    def test_body(self, body):
        op = GKECreateClusterOperator(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            body=body,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        assert op.cluster_name == GKE_CLUSTER_NAME

    @pytest.mark.parametrize(
        "body",
        [
            None,
            {"missing_name": "test-name", "initial_node_count": 1},
            {
                "name": "test-name",
                "initial_node_count": 1,
                "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            {"missing_name": "test-name", "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}]},
            {
                "name": "test-name",
                "missing_initial_node_count": 1,
                "missing_node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            type("Cluster", (object,), {"missing_name": "test-name", "initial_node_count": 1})(),
            type(
                "Cluster",
                (object,),
                {
                    "missing_name": "test-name",
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "missing_initial_node_count": 1,
                    "missing_node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "initial_node_count": 1,
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
        ],
    )
    def test_body_error(self, body):
        deprecated_fields = {"initial_node_count", "node_config", "zone", "instance_group_urls"}
        used_deprecated_fields = {}
        if body:
            if isinstance(body, dict):
                used_deprecated_fields = set(body.keys()).intersection(deprecated_fields)
            else:
                used_deprecated_fields = {getattr(body, field, None) for field in deprecated_fields}
                used_deprecated_fields = {field for field in used_deprecated_fields if field}

        if used_deprecated_fields:
            with pytest.raises(AirflowProviderDeprecationWarning):
                GKECreateClusterOperator(
                    project_id=TEST_PROJECT_ID, location=TEST_LOCATION, body=body, task_id=TEST_TASK_ID
                )
        else:
            with pytest.raises(AirflowException):
                GKECreateClusterOperator(
                    project_id=TEST_PROJECT_ID, location=TEST_LOCATION, body=body, task_id=TEST_TASK_ID
                )

    @pytest.mark.parametrize(
        ("deprecated_field_name", "deprecated_field_value"),
        [
            ("initial_node_count", 1),
            ("node_config", mock.MagicMock()),
            ("zone", mock.MagicMock()),
            ("instance_group_urls", mock.MagicMock()),
        ],
    )
    def test_alert_deprecated_body_fields(self, deprecated_field_name, deprecated_field_value):
        body = deepcopy(GKE_CLUSTER_CREATE_BODY_DICT)
        body[deprecated_field_name] = deprecated_field_value
        with pytest.raises(AirflowProviderDeprecationWarning):
            GKECreateClusterOperator(
                project_id=TEST_PROJECT_ID, location=TEST_LOCATION, body=body, task_id=TEST_TASK_ID
            )

    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineClusterLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_cluster_hook, mock_link):
        mock_create_cluster = mock_cluster_hook.return_value.create_cluster
        mock_operation = mock_create_cluster.return_value
        mock_operation.target_link = TEST_TARGET_LINK
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(context=mock_context, cluster=GKE_CLUSTER_CREATE_BODY_DICT)
        mock_create_cluster.assert_called_once_with(
            cluster=GKE_CLUSTER_CREATE_BODY_DICT,
            project_id=TEST_PROJECT_ID,
            wait_to_complete=True,
        )
        assert result == TEST_TARGET_LINK

    @mock.patch(GKE_OPERATORS_PATH.format("GKECreateClusterOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineClusterLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute_error(self, mock_cluster_hook, mock_link, mock_log):
        mock_create_cluster = mock_cluster_hook.return_value.create_cluster
        expected_error_message = "test-message"
        mock_create_cluster.side_effect = AlreadyExists(message=expected_error_message)
        mock_get_cluster = mock_cluster_hook.return_value.get_cluster
        mock_get_cluster.return_value.self_link = TEST_SELF_LINK
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(context=mock_context, cluster=GKE_CLUSTER_CREATE_BODY_DICT)
        mock_create_cluster.assert_called_once_with(
            cluster=GKE_CLUSTER_CREATE_BODY_DICT,
            project_id=TEST_PROJECT_ID,
            wait_to_complete=True,
        )
        mock_get_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
        )
        mock_log.info.assert_called_once_with("Assuming Success: %s", expected_error_message)
        assert result == TEST_SELF_LINK

    @mock.patch(GKE_OPERATORS_PATH.format("GKEOperationTrigger"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineClusterLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKECreateClusterOperator.defer"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_deferrable(self, mock_cluster_hook, mock_defer, mock_link, mock_trigger):
        mock_create_cluster = mock_cluster_hook.return_value.create_cluster
        mock_operation = mock_create_cluster.return_value
        mock_operation.name = TEST_OPERATION_NAME
        mock_trigger_instance = mock_trigger.return_value
        mock_context = mock.MagicMock()
        self.operator.deferrable = True

        self.operator.execute(context=mock_context)

        mock_link.persist.assert_called_once_with(context=mock_context, cluster=GKE_CLUSTER_CREATE_BODY_DICT)
        mock_create_cluster.assert_called_once_with(
            cluster=GKE_CLUSTER_CREATE_BODY_DICT,
            project_id=TEST_PROJECT_ID,
            wait_to_complete=False,
        )
        mock_trigger.assert_called_once_with(
            operation_name=TEST_OPERATION_NAME,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poll_interval=10,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger_instance,
            method_name="execute_complete",
        )

    @mock.patch(GKE_OPERATORS_PATH.format("GKECreateClusterOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute_complete(self, cluster_hook, mock_log):
        mock_get_operation = cluster_hook.return_value.get_operation
        mock_get_operation.return_value.target_link = TEST_TARGET_LINK
        expected_status, expected_message = "success", "test-message"
        event = dict(status=expected_status, message=expected_message, operation_name=TEST_OPERATION_NAME)

        result = self.operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.info.assert_called_once_with(expected_message)
        mock_get_operation.assert_called_once_with(operation_name=TEST_OPERATION_NAME)
        assert result == TEST_TARGET_LINK

    @pytest.mark.parametrize("status", ["failed", "error"])
    @mock.patch(GKE_OPERATORS_PATH.format("GKECreateClusterOperator.log"))
    def test_execute_complete_error(self, mock_log, status):
        expected_message = "test-message"
        event = dict(status=status, message=expected_message)

        with pytest.raises(AirflowException):
            self.operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.exception.assert_called_once_with("Trigger ended with one of the failed statuses.")


class TestGKEStartKueueInsideClusterOperator:
    def setup_method(self):
        self.operator = GKEStartKueueInsideClusterOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            kueue_version=K8S_KUEUE_VERSION,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            use_internal_ip=False,
        )

    def test_template_fields(self):
        expected_template_fields = set(GKEOperatorMixin.template_fields) | set(
            KubernetesInstallKueueOperator.template_fields
        )
        assert set(GKEStartKueueInsideClusterOperator.template_fields) == expected_template_fields

    def test_enable_tcp_keepalive(self):
        assert self.operator.enable_tcp_keepalive

    @mock.patch(GKE_OPERATORS_PATH.format("super"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineClusterLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_hook, mock_link, mock_super):
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_cluster = mock_get_cluster.return_value
        mock_check_cluster_autoscaling_ability = mock_hook.return_value.check_cluster_autoscaling_ability
        mock_check_cluster_autoscaling_ability.return_value = True
        mock_context = mock.MagicMock()

        self.operator.execute(context=mock_context)

        mock_get_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
        )
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            cluster=mock_cluster,
        )
        mock_check_cluster_autoscaling_ability.assert_called_once_with(cluster=mock_cluster)
        mock_super.assert_called_once()
        mock_super.return_value.execute.assert_called_once_with(mock_context)

    @mock.patch(GKE_OPERATORS_PATH.format("GKEStartKueueInsideClusterOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("super"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineClusterLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute_not_scalable(self, mock_hook, mock_link, mock_super, mock_log):
        mock_get_cluster = mock_hook.return_value.get_cluster
        mock_cluster = mock_get_cluster.return_value
        mock_check_cluster_autoscaling_ability = mock_hook.return_value.check_cluster_autoscaling_ability
        mock_check_cluster_autoscaling_ability.return_value = False
        mock_context = mock.MagicMock()

        self.operator.execute(context=mock_context)

        mock_get_cluster.assert_called_once_with(
            name=GKE_CLUSTER_NAME,
            project_id=TEST_PROJECT_ID,
        )
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            cluster=mock_cluster,
        )
        mock_check_cluster_autoscaling_ability.assert_called_once_with(cluster=mock_cluster)
        mock_super.assert_not_called()
        mock_log.info.assert_called_once_with(
            "Cluster doesn't have ability to autoscale, will not install Kueue inside. Aborting"
        )


class TestGKEStartPodOperator:
    def setup_method(self):
        self.operator = GKEStartPodOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            name=K8S_POD_NAME,
            namespace=K8S_NAMESPACE,
            image=TEST_IMAGE,
            on_finish_action=OnFinishAction.KEEP_POD,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_template_fields(self):
        expected_template_fields = (
            {"deferrable"}
            | (set(KubernetesPodOperator.template_fields) - {"is_delete_operator_pod", "regional"})
            | set(GKEOperatorMixin.template_fields)
        )
        assert set(GKEStartPodOperator.template_fields) == expected_template_fields

    def test_config_file_error(self):
        with pytest.raises(AirflowException):
            GKEStartPodOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                task_id=TEST_TASK_ID,
                name=K8S_POD_NAME,
                namespace=K8S_NAMESPACE,
                image=TEST_IMAGE,
                config_file="/path/to/alternative/kubeconfig",
                on_finish_action=OnFinishAction.KEEP_POD,
            )

    @pytest.mark.parametrize(
        ("kwargs", "expected_attributes"),
        [
            (
                {"on_finish_action": "delete_succeeded_pod"},
                {"on_finish_action": OnFinishAction.DELETE_SUCCEEDED_POD},
            ),
            (
                {"on_finish_action": "keep_pod"},
                {"on_finish_action": OnFinishAction.KEEP_POD},
            ),
            (
                {"on_finish_action": "delete_pod"},
                {"on_finish_action": OnFinishAction.DELETE_POD},
            ),
        ],
    )
    def test_on_finish_action_handler(
        self,
        kwargs,
        expected_attributes,
    ):
        kpo_init_args_mock = mock.MagicMock(**{"parameters": ["on_finish_action"]})
        with mock.patch("inspect.signature", return_value=kpo_init_args_mock):
            op = GKEStartPodOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                task_id=TEST_TASK_ID,
                name=K8S_POD_NAME,
                namespace=K8S_NAMESPACE,
                image=TEST_IMAGE,
                **kwargs,
            )
            for expected_attr in expected_attributes:
                assert op.__getattribute__(expected_attr) == expected_attributes[expected_attr]

    @mock.patch(GKE_OPERATORS_PATH.format("GKEStartPodOperator.defer"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails.fetch_cluster_info"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEStartPodTrigger"))
    @mock.patch(GKE_OPERATORS_PATH.format("utcnow"))
    def test_invoke_defer_method(
        self, mock_utcnow, mock_trigger, mock_cluster_hook, mock_fetch_cluster_info, mock_defer
    ):
        mock_trigger_start_time = mock_utcnow.return_value

        mock_metadata = mock.MagicMock()
        mock_metadata.name = K8S_POD_NAME
        mock_metadata.namespace = K8S_NAMESPACE
        self.operator.pod = mock.MagicMock(metadata=mock_metadata)
        mock_fetch_cluster_info.return_value = GKE_CLUSTER_URL, GKE_SSL_CA_CERT
        mock_get_logs = mock.MagicMock()
        self.operator.get_logs = mock_get_logs
        mock_last_log_time = mock.MagicMock()

        self.operator.invoke_defer_method(last_log_time=mock_last_log_time)

        mock_trigger.assert_called_once_with(
            pod_name=K8S_POD_NAME,
            pod_namespace=K8S_NAMESPACE,
            trigger_start_time=mock_trigger_start_time,
            cluster_url=GKE_CLUSTER_URL,
            ssl_ca_cert=GKE_SSL_CA_CERT,
            get_logs=mock_get_logs,
            startup_timeout=120,
            cluster_context=None,
            poll_interval=2,
            in_cluster=None,
            base_container_name="base",
            on_finish_action=OnFinishAction.KEEP_POD,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            logging_interval=None,
            last_log_time=mock_last_log_time,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger.return_value,
            method_name="trigger_reentry",
        )


class TestGKEStartJobOperator:
    def setup_method(self):
        self.operator = GKEStartJobOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
            image=TEST_IMAGE,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_template_fields(self):
        expected_template_fields = (
            {"deferrable", "poll_interval"}
            | set(GKEOperatorMixin.template_fields)
            | set(KubernetesJobOperator.template_fields)
        )
        assert set(GKEStartJobOperator.template_fields) == expected_template_fields

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEStartJobOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                task_id=TEST_TASK_ID,
                name=K8S_JOB_NAME,
                namespace=K8S_NAMESPACE,
                image=TEST_IMAGE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch(GKE_OPERATORS_PATH.format("super"))
    def test_execute(self, mock_super):
        mock_context = mock.MagicMock()

        self.operator.execute(context=mock_context)

        mock_super.assert_called_once()
        mock_super.return_value.execute.assert_called_once_with(mock_context)

    @mock.patch(GKE_OPERATORS_PATH.format("super"))
    @mock.patch(GKE_OPERATORS_PATH.format("ProvidersManager"))
    def test_deferrable(self, mock_providers_manager, mock_super):
        kubernetes_package_name = "apache-airflow-providers-cncf-kubernetes"
        mock_providers_manager.return_value.providers = {
            kubernetes_package_name: mock.MagicMock(
                data={
                    "package-name": kubernetes_package_name,
                },
                version="8.0.2",
            )
        }
        mock_context = mock.MagicMock()
        self.operator.deferrable = True

        self.operator.execute(context=mock_context)

        mock_providers_manager.assert_called_once()
        mock_super.assert_called_once()
        mock_super.return_value.execute.assert_called_once_with(mock_context)

    @mock.patch(GKE_OPERATORS_PATH.format("super"))
    @mock.patch(GKE_OPERATORS_PATH.format("ProvidersManager"))
    def test_deferrable_error(self, mock_providers_manager, mock_super):
        kubernetes_package_name = "apache-airflow-providers-cncf-kubernetes"
        mock_providers_manager.return_value.providers = {
            kubernetes_package_name: mock.MagicMock(
                data={
                    "package-name": kubernetes_package_name,
                },
                version="8.0.1",
            )
        }
        self.operator.deferrable = True

        with pytest.raises(AirflowException):
            self.operator.execute(context=mock.MagicMock())

        mock_providers_manager.assert_called_once()
        mock_super.assert_not_called()
        mock_super.return_value.execute.assert_not_called()

    @mock.patch(GKE_OPERATORS_PATH.format("GKEStartJobOperator.defer"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails.fetch_cluster_info"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEJobTrigger"))
    def test_execute_deferrable(self, mock_trigger, mock_cluster_hook, mock_fetch_cluster_info, mock_defer):
        mock_pod_metadata = mock.MagicMock()
        mock_pod_metadata.name = K8S_POD_NAME
        mock_pod_metadata.namespace = K8S_NAMESPACE
        self.operator.pods = [
            mock.MagicMock(metadata=mock_pod_metadata),
        ]

        mock_job_metadata = mock.MagicMock()
        mock_job_metadata.name = K8S_JOB_NAME
        mock_job_metadata.namespace = K8S_NAMESPACE
        self.operator.job = mock.MagicMock(metadata=mock_job_metadata)

        mock_fetch_cluster_info.return_value = GKE_CLUSTER_URL, GKE_SSL_CA_CERT
        mock_get_logs = mock.MagicMock()
        self.operator.get_logs = mock_get_logs

        self.operator.execute_deferrable()

        mock_trigger.assert_called_once_with(
            cluster_url=GKE_CLUSTER_URL,
            ssl_ca_cert=GKE_SSL_CA_CERT,
            job_name=K8S_JOB_NAME,
            job_namespace=K8S_NAMESPACE,
            pod_names=[
                K8S_POD_NAME,
            ],
            pod_namespace=K8S_NAMESPACE,
            base_container_name="base",
            gcp_conn_id=TEST_CONN_ID,
            poll_interval=10.0,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            get_logs=mock_get_logs,
            do_xcom_push=False,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger.return_value,
            method_name="execute_complete",
        )

    @mock.patch(GKE_OPERATORS_PATH.format("GKEStartJobOperator.defer"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails.fetch_cluster_info"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEJobTrigger"))
    def test_execute_deferrable_with_parallelism(
        self, mock_trigger, mock_cluster_hook, mock_fetch_cluster_info, mock_defer
    ):
        op = GKEStartJobOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
            image=TEST_IMAGE,
            gcp_conn_id=TEST_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            parallelism=2,
        )
        mock_pod_name_1 = K8S_POD_NAME + "-1"
        mock_pod_metadata_1 = mock.MagicMock()
        mock_pod_metadata_1.name = mock_pod_name_1
        mock_pod_metadata_1.namespace = K8S_NAMESPACE

        mock_pod_name_2 = K8S_POD_NAME + "-2"
        mock_pod_metadata_2 = mock.MagicMock()
        mock_pod_metadata_2.name = mock_pod_name_2
        mock_pod_metadata_2.namespace = K8S_NAMESPACE
        op.pods = [mock.MagicMock(metadata=mock_pod_metadata_1), mock.MagicMock(metadata=mock_pod_metadata_2)]

        mock_job_metadata = mock.MagicMock()
        mock_job_metadata.name = K8S_JOB_NAME
        mock_job_metadata.namespace = K8S_NAMESPACE
        op.job = mock.MagicMock(metadata=mock_job_metadata)

        mock_fetch_cluster_info.return_value = GKE_CLUSTER_URL, GKE_SSL_CA_CERT
        mock_get_logs = mock.MagicMock()
        op.get_logs = mock_get_logs

        op.execute_deferrable()

        mock_trigger.assert_called_once_with(
            cluster_url=GKE_CLUSTER_URL,
            ssl_ca_cert=GKE_SSL_CA_CERT,
            job_name=K8S_JOB_NAME,
            job_namespace=K8S_NAMESPACE,
            pod_names=[mock_pod_name_1, mock_pod_name_2],
            pod_namespace=K8S_NAMESPACE,
            base_container_name="base",
            gcp_conn_id=TEST_CONN_ID,
            poll_interval=10.0,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            get_logs=mock_get_logs,
            do_xcom_push=False,
        )
        mock_defer.assert_called_once_with(
            trigger=mock_trigger.return_value,
            method_name="execute_complete",
        )


class TestGKEDescribeJobOperator:
    def setup_method(self):
        self.operator = GKEDescribeJobOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            job_name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
        )

    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineJobLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEDescribeJobOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEClusterAuthDetails.fetch_cluster_info"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_cluster_hook, mock_hook, mock_fetch_cluster_info, mock_log, mock_link):
        mock_fetch_cluster_info.return_value = GKE_CLUSTER_URL, GKE_SSL_CA_CERT
        mock_job = mock_hook.return_value.get_job.return_value
        mock_context = mock.MagicMock()

        self.operator.execute(context=mock_context)

        mock_hook.return_value.get_job.assert_called_once_with(job_name=K8S_JOB_NAME, namespace=K8S_NAMESPACE)
        mock_log.info.assert_called_once_with(
            "Retrieved description of Job %s from cluster %s:\n %s",
            K8S_JOB_NAME,
            GKE_CLUSTER_NAME,
            mock_job,
        )
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            namespace=mock_job.metadata.namespace,
            job_name=mock_job.metadata.name,
        )


class TestGKEListJobsOperator:
    def setup_method(self):
        self.operator = GKEListJobsOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
        )

    def test_template_fields(self):
        expected_template_fields = {"namespace"} | set(GKEOperatorMixin.template_fields)
        assert set(GKEListJobsOperator.template_fields) == expected_template_fields

    @mock.patch(GKE_OPERATORS_PATH.format("V1JobList.to_dict"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEListJobsOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    def test_execute(self, mock_hook, cluster_hook, mock_log, mock_to_dict):
        mock_list_jobs_from_namespace = mock_hook.return_value.list_jobs_from_namespace
        mock_list_jobs_all_namespaces = mock_hook.return_value.list_jobs_all_namespaces
        mock_job_1, mock_job_2 = mock.MagicMock(), mock.MagicMock()
        mock_jobs = mock.MagicMock(items=[mock_job_1, mock_job_2])
        mock_list_jobs_all_namespaces.return_value = mock_jobs
        mock_to_dict_value = mock_to_dict.return_value

        mock_ti = mock.MagicMock()
        context = {"ti": mock_ti, "task": mock.MagicMock()}

        result = self.operator.execute(context=context)

        mock_list_jobs_all_namespaces.assert_called_once()
        mock_list_jobs_from_namespace.assert_not_called()
        mock_log.info.assert_has_calls(
            [
                call("Retrieved description of Job:\n %s", mock_job_1),
                call("Retrieved description of Job:\n %s", mock_job_2),
            ]
        )
        mock_to_dict.assert_has_calls([call(mock_jobs), call(mock_jobs)])
        mock_ti.xcom_push.assert_has_calls(
            [call(key="jobs_list", value=mock_to_dict_value), call(key="kubernetes_workloads_conf", value={})]
        )
        assert result == mock_to_dict_value

    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineWorkloadsLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("V1JobList.to_dict"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEListJobsOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    def test_execute_namespaced(self, mock_hook, cluster_hook, mock_log, mock_to_dict, mock_link):
        mock_list_jobs_from_namespace = mock_hook.return_value.list_jobs_from_namespace
        mock_list_jobs_all_namespaces = mock_hook.return_value.list_jobs_all_namespaces
        mock_job_1, mock_job_2 = mock.MagicMock(), mock.MagicMock()
        mock_jobs = mock.MagicMock(items=[mock_job_1, mock_job_2])
        mock_list_jobs_from_namespace.return_value = mock_jobs
        mock_to_dict_value = mock_to_dict.return_value

        mock_ti = mock.MagicMock()
        context = {"ti": mock_ti, "task": mock.MagicMock()}

        self.operator.namespace = K8S_NAMESPACE
        result = self.operator.execute(context=context)

        mock_list_jobs_all_namespaces.assert_not_called()
        mock_list_jobs_from_namespace.assert_called_once_with(namespace=K8S_NAMESPACE)
        mock_log.info.assert_has_calls(
            [
                call("Retrieved description of Job:\n %s", mock_job_1),
                call("Retrieved description of Job:\n %s", mock_job_2),
            ]
        )
        mock_to_dict.assert_has_calls([call(mock_jobs), call(mock_jobs)])
        mock_ti.xcom_push.assert_called_once_with(key="jobs_list", value=mock_to_dict_value)
        mock_link.persist.assert_called_once_with(context=context)
        assert result == mock_to_dict_value

    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineWorkloadsLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("V1JobList.to_dict"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEListJobsOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    def test_execute_not_do_xcom_push(self, mock_hook, cluster_hook, mock_log, mock_to_dict, mock_link):
        mock_list_jobs_from_namespace = mock_hook.return_value.list_jobs_from_namespace
        mock_list_jobs_all_namespaces = mock_hook.return_value.list_jobs_all_namespaces
        mock_job_1, mock_job_2 = mock.MagicMock(), mock.MagicMock()
        mock_jobs = mock.MagicMock(items=[mock_job_1, mock_job_2])
        mock_list_jobs_all_namespaces.return_value = mock_jobs
        mock_to_dict_value = mock_to_dict.return_value

        mock_ti = mock.MagicMock()
        context = {"ti": mock_ti, "task": mock.MagicMock()}

        self.operator.do_xcom_push = False
        result = self.operator.execute(context=context)

        mock_list_jobs_all_namespaces.assert_called_once()
        mock_list_jobs_from_namespace.assert_not_called()
        mock_log.info.assert_has_calls(
            [
                call("Retrieved description of Job:\n %s", mock_job_1),
                call("Retrieved description of Job:\n %s", mock_job_2),
            ]
        )
        mock_to_dict.assert_called_once_with(mock_jobs)
        mock_link.persist.assert_called_once_with(context=context)
        assert result == mock_to_dict_value


class TestGKECreateCustomResourceOperator:
    def test_template_fields(self):
        assert set(GKECreateCustomResourceOperator.template_fields) == set(
            GKEOperatorMixin.template_fields
        ) | set(KubernetesCreateResourceOperator.template_fields)

    def test_gcp_conn_id_required(self):
        with pytest.raises(AirflowException):
            GKECreateCustomResourceOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                task_id=TEST_TASK_ID,
                yaml_conf_file="/path/to/yaml_conf_file",
                gcp_conn_id=None,
            )

    def test_config_file_throws_error(self):
        expected_error_message = (
            "config_file is not an allowed parameter for the GKECreateCustomResourceOperator."
        )
        with pytest.raises(AirflowException, match=expected_error_message):
            GKECreateCustomResourceOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                gcp_conn_id=TEST_CONN_ID,
                task_id=TEST_TASK_ID,
                yaml_conf_file="/path/to/yaml_conf_file",
                config_file="/path/to/alternative/kubeconfig",
            )


class TestGKEDeleteCustomResourceOperator:
    def test_template_fields(self):
        assert set(GKEDeleteCustomResourceOperator.template_fields) == set(
            GKEOperatorMixin.template_fields
        ) | set(KubernetesDeleteResourceOperator.template_fields)

    def test_gcp_conn_id_required(self):
        with pytest.raises(AirflowException):
            GKEDeleteCustomResourceOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                task_id=TEST_TASK_ID,
                yaml_conf_file="/path/to/yaml_conf_file",
                gcp_conn_id=None,
            )

    def test_config_file_throws_error(self):
        expected_error_message = (
            "config_file is not an allowed parameter for the GKEDeleteCustomResourceOperator."
        )
        with pytest.raises(AirflowException, match=expected_error_message):
            GKEDeleteCustomResourceOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                gcp_conn_id=TEST_CONN_ID,
                task_id=TEST_TASK_ID,
                yaml_conf_file="/path/to/yaml_conf_file",
                config_file="/path/to/alternative/kubeconfig",
            )


class TestGKEStartKueueJobOperator:
    def test_template_fields(self):
        assert set(GKEStartKueueJobOperator.template_fields) == set(GKEOperatorMixin.template_fields) | set(
            KubernetesStartKueueJobOperator.template_fields
        )


class TestGKEDeleteJobOperator:
    def test_template_fields(self):
        assert set(GKEDeleteJobOperator.template_fields) == set(GKEOperatorMixin.template_fields) | set(
            KubernetesDeleteJobOperator.template_fields
        )

    def test_gcp_conn_id_required(self):
        with pytest.raises(AirflowException):
            GKEDeleteJobOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                name=K8S_JOB_NAME,
                namespace=K8S_NAMESPACE,
                task_id=TEST_TASK_ID,
                gcp_conn_id=None,
            )

    def test_config_file_throws_error(self):
        expected_error_message = "config_file is not an allowed parameter for the GKEDeleteJobOperator."
        with pytest.raises(AirflowException, match=expected_error_message):
            GKEDeleteJobOperator(
                project_id=TEST_PROJECT_ID,
                location=TEST_LOCATION,
                cluster_name=GKE_CLUSTER_NAME,
                name=K8S_JOB_NAME,
                namespace=K8S_NAMESPACE,
                task_id=TEST_TASK_ID,
                config_file="/path/to/alternative/kubeconfig",
            )


class TestGKESuspendJobOperator:
    def setup_method(self):
        self.operator = GKESuspendJobOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
        )

    def test_template_fields(self):
        expected_template_fields = {"name", "namespace"} | set(GKEOperatorMixin.template_fields)
        assert set(GKESuspendJobOperator.template_fields) == expected_template_fields

    @mock.patch(GKE_OPERATORS_PATH.format("k8s.V1Job.to_dict"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineJobLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKESuspendJobOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_cluster_hook, mock_hook, mock_log, mock_link, mock_to_dict):
        mock_patch_namespaced_job = mock_hook.return_value.patch_namespaced_job
        mock_job = mock_patch_namespaced_job.return_value
        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_patch_namespaced_job.assert_called_once_with(
            job_name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
            body={"spec": {"suspend": True}},
        )
        mock_log.info.assert_called_once_with(
            "Job %s from cluster %s was suspended.",
            K8S_JOB_NAME,
            GKE_CLUSTER_NAME,
        )
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            namespace=mock_job.metadata.namespace,
            job_name=mock_job.metadata.name,
        )
        mock_to_dict.assert_called_once_with(mock_job)
        assert result == expected_result


class TestGKEResumeJobOperator:
    def setup_method(self):
        self.operator = GKEResumeJobOperator(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            task_id=TEST_TASK_ID,
            name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
        )

    def test_template_fields(self):
        expected_template_fields = {"name", "namespace"} | set(GKEOperatorMixin.template_fields)
        assert set(GKEResumeJobOperator.template_fields) == expected_template_fields

    @mock.patch(GKE_OPERATORS_PATH.format("k8s.V1Job.to_dict"))
    @mock.patch(GKE_OPERATORS_PATH.format("KubernetesEngineJobLink"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEResumeJobOperator.log"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEKubernetesHook"))
    @mock.patch(GKE_OPERATORS_PATH.format("GKEHook"))
    def test_execute(self, mock_cluster_hook, mock_hook, mock_log, mock_link, mock_to_dict):
        mock_patch_namespaced_job = mock_hook.return_value.patch_namespaced_job
        mock_job = mock_patch_namespaced_job.return_value
        expected_result = mock_to_dict.return_value
        mock_context = mock.MagicMock()

        result = self.operator.execute(context=mock_context)

        mock_patch_namespaced_job.assert_called_once_with(
            job_name=K8S_JOB_NAME,
            namespace=K8S_NAMESPACE,
            body={"spec": {"suspend": False}},
        )
        mock_log.info.assert_called_once_with(
            "Job %s from cluster %s was resumed.",
            K8S_JOB_NAME,
            GKE_CLUSTER_NAME,
        )
        mock_link.persist.assert_called_once_with(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            namespace=mock_job.metadata.namespace,
            job_name=mock_job.metadata.name,
        )
        mock_to_dict.assert_called_once_with(mock_job)
        assert result == expected_result
