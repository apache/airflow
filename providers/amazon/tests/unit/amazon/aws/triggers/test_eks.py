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

import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateClusterTrigger,
    EksDeleteClusterTrigger,
    EksPodTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

EXCEPTION_MOCK = AirflowException("MOCK ERROR")
CLUSTER_NAME = "test_cluster"
WAITER_DELAY = 1
WAITER_MAX_ATTEMPTS = 10
AWS_CONN_ID = "test_conn_id"
REGION_NAME = "test-region"
FARGATE_PROFILES = ["p1", "p2"]


class TestEksTrigger:
    def setup_method(self):
        self.async_conn_patcher = patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_async_conn")
        self.mock_async_conn = self.async_conn_patcher.start()

        self.mock_client = AsyncMock()
        self.mock_async_conn.return_value.__aenter__.return_value = self.mock_client

        self.async_wait_patcher = patch(
            "airflow.providers.amazon.aws.triggers.eks.async_wait", return_value=True
        )
        self.mock_async_wait = self.async_wait_patcher.start()

    def teardown_method(self):
        self.async_conn_patcher.stop()
        self.async_wait_patcher.stop()


class TestEksCreateClusterTrigger(TestEksTrigger):
    def setup_method(self):
        super().setup_method()

        self.trigger = EksCreateClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )
        self.trigger.log.error = Mock()

    @pytest.mark.asyncio
    async def test_when_cluster_is_created_run_should_return_a_success_event(self):
        generator = self.trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    async def test_when_run_raises_exception_it_should_return_a_failure_event(self):
        self.mock_async_wait.side_effect = EXCEPTION_MOCK

        generator = self.trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "failed"})
        self.trigger.log.error.assert_called_once_with("Error creating cluster: %s", EXCEPTION_MOCK)

    @pytest.mark.asyncio
    async def test_run_parameterizes_async_wait_correctly(self):
        self.mock_client.get_waiter = Mock(return_value="waiter")

        generator = self.trigger.run()
        await generator.asend(None)

        self.mock_client.get_waiter.assert_called_once_with("cluster_active")
        self.mock_async_wait.assert_called_once_with(
            "waiter",
            WAITER_DELAY,
            WAITER_MAX_ATTEMPTS,
            {"name": CLUSTER_NAME},
            "Error checking Eks cluster",
            "Eks cluster status is",
            ["cluster.status"],
        )


class TestEksDeleteClusterTriggerRun(TestEksTrigger):
    def setup_method(self):
        super().setup_method()

        self.delete_any_nodegroups_patcher = patch.object(EksDeleteClusterTrigger, "delete_any_nodegroups")
        self.mock_delete_any_nodegroups = self.delete_any_nodegroups_patcher.start()

        self.delete_any_fargate_profiles_patcher = patch.object(
            EksDeleteClusterTrigger, "delete_any_fargate_profiles"
        )
        self.mock_delete_any_fargate_profiles = self.delete_any_fargate_profiles_patcher.start()

        self.trigger = EksDeleteClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            force_delete_compute=False,
        )
        self.trigger.log.info = Mock()

    def teardown_method(self):
        super().teardown_method()
        self.delete_any_nodegroups_patcher.stop()
        self.delete_any_fargate_profiles_patcher.stop()

    @pytest.mark.asyncio
    async def test_run_deletes_nodegroups_and_fargate_profiles(self):
        self.trigger.force_delete_compute = True
        generator = self.trigger.run()
        response = await generator.asend(None)

        self.mock_delete_any_nodegroups.assert_called_once_with(client=self.mock_client)
        self.mock_delete_any_fargate_profiles.assert_called_once_with(client=self.mock_client)

        assert response == TriggerEvent({"status": "deleted"})

    @pytest.mark.asyncio
    async def test_when_resource_is_not_found_it_should_return_status_deleted(self):
        delete_cluster_mock = AsyncMock(
            side_effect=ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "delete_eks_cluster")
        )
        self.mock_client.delete_cluster = delete_cluster_mock

        generator = self.trigger.run()
        response = await generator.asend(None)

        delete_cluster_mock.assert_called_once_with(name=CLUSTER_NAME)

        assert response == TriggerEvent({"status": "deleted"})

    @pytest.mark.asyncio
    async def test_run_raises_client_error(self):
        response = {"Error": {"Code": "OtherException"}}
        operation_name = "delete_eks_cluster"
        delete_cluster_mock = AsyncMock(side_effect=ClientError(response, "delete_eks_cluster"))
        self.mock_client.delete_cluster = delete_cluster_mock

        generator = self.trigger.run()

        with pytest.raises(ClientError) as exception:
            await generator.asend(None)

        delete_cluster_mock.assert_called_once_with(name=CLUSTER_NAME)
        assert exception._excinfo[1].response == response
        assert exception._excinfo[1].operation_name == operation_name

    @pytest.mark.asyncio
    async def test_run_parameterizes_async_wait_correctly(self):
        self.mock_client.get_waiter = Mock(return_value="waiter")

        generator = self.trigger.run()
        await generator.asend(None)

        self.mock_delete_any_fargate_profiles.assert_not_called()
        self.mock_delete_any_nodegroups.assert_not_called()
        self.mock_client.get_waiter.assert_called_once_with("cluster_deleted")
        self.mock_async_wait.assert_called_once_with(
            waiter="waiter",
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            args={"name": CLUSTER_NAME},
            failure_message="Error deleting cluster",
            status_message="Status of cluster is",
            status_args=["cluster.status"],
        )


class TestEksDeleteClusterTriggerDeleteNodegroupsAndFargateProfiles(TestEksTrigger):
    def setup_method(self):
        super().setup_method()

        self.get_waiter_patcher = patch(
            "airflow.providers.amazon.aws.hooks.eks.EksHook.get_waiter", return_value="waiter"
        )
        self.mock_waiter = self.get_waiter_patcher.start()

        self.trigger = EksDeleteClusterTrigger(
            cluster_name=CLUSTER_NAME,
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            force_delete_compute=False,
        )
        self.trigger.log.info = Mock()

    def teardown_method(self):
        super().teardown_method()
        self.get_waiter_patcher.stop()

    @pytest.mark.asyncio
    async def test_delete_nodegroups(self):
        mock_list_node_groups = AsyncMock(return_value={"nodegroups": ["g1", "g2"]})
        mock_delete_nodegroup = AsyncMock()
        mock_client = AsyncMock(list_nodegroups=mock_list_node_groups, delete_nodegroup=mock_delete_nodegroup)

        await self.trigger.delete_any_nodegroups(mock_client)

        mock_list_node_groups.assert_called_once_with(clusterName=CLUSTER_NAME)
        self.trigger.log.info.assert_has_calls([call("Deleting nodegroups"), call("All nodegroups deleted")])
        self.mock_waiter.assert_called_once_with(
            "all_nodegroups_deleted", deferrable=True, client=mock_client
        )
        mock_delete_nodegroup.assert_has_calls(
            [
                call(clusterName=CLUSTER_NAME, nodegroupName="g1"),
                call(clusterName=CLUSTER_NAME, nodegroupName="g2"),
            ]
        )
        self.mock_async_wait.assert_called_once_with(
            waiter="waiter",
            waiter_delay=WAITER_DELAY,
            waiter_max_attempts=WAITER_MAX_ATTEMPTS,
            args={"clusterName": CLUSTER_NAME},
            failure_message="Error deleting nodegroup for cluster test_cluster",
            status_message="Deleting nodegroups associated with the cluster",
            status_args=["nodegroups"],
        )

    @pytest.mark.asyncio
    async def test_when_there_are_no_nodegroups_it_should_only_log_message(self):
        mock_list_node_groups = AsyncMock(return_value={"nodegroups": []})
        mock_delete_nodegroup = AsyncMock()
        mock_client = AsyncMock(list_nodegroups=mock_list_node_groups, delete_nodegroup=mock_delete_nodegroup)

        await self.trigger.delete_any_nodegroups(mock_client)

        mock_list_node_groups.assert_called_once_with(clusterName=CLUSTER_NAME)
        self.mock_async_wait.assert_not_called()
        mock_delete_nodegroup.assert_not_called()
        self.trigger.log.info.assert_called_once_with(
            "No nodegroups associated with cluster %s", CLUSTER_NAME
        )

    @pytest.mark.asyncio
    async def test_delete_any_fargate_profiles(self):
        mock_list_fargate_profiles = AsyncMock(return_value={"fargateProfileNames": FARGATE_PROFILES})
        mock_delete_fargate_profile = AsyncMock()
        mock_client = AsyncMock(
            list_fargate_profiles=mock_list_fargate_profiles,
            delete_fargate_profile=mock_delete_fargate_profile,
            get_waiter=self.mock_waiter,
        )

        await self.trigger.delete_any_fargate_profiles(mock_client)

        mock_list_fargate_profiles.assert_called_once_with(clusterName=CLUSTER_NAME)
        self.trigger.log.info.assert_has_calls(
            [
                call("Waiting for Fargate profiles to delete.  This will take some time."),
                call("All Fargate profiles deleted"),
            ]
        )
        self.mock_waiter.assert_has_calls([call("fargate_profile_deleted"), call("fargate_profile_deleted")])
        mock_delete_fargate_profile.assert_has_calls(
            [
                call(clusterName=CLUSTER_NAME, fargateProfileName="p1"),
                call(clusterName=CLUSTER_NAME, fargateProfileName="p2"),
            ]
        )
        self.mock_async_wait.assert_has_calls(
            [
                call(
                    waiter="waiter",
                    waiter_delay=WAITER_DELAY,
                    waiter_max_attempts=WAITER_MAX_ATTEMPTS,
                    args={"clusterName": CLUSTER_NAME, "fargateProfileName": profile},
                    failure_message="Error deleting fargate profile for cluster test_cluster",
                    status_message="Status of fargate profile is",
                    status_args=["fargateProfile.status"],
                )
                for profile in FARGATE_PROFILES
            ]
        )

    @pytest.mark.asyncio
    async def test_when_there_are_no_fargate_profiles_it_should_only_log_message(self):
        mock_list_fargate_profiles = AsyncMock(return_value={"fargateProfileNames": []})
        mock_delete_fargate_profile = AsyncMock()
        mock_client = AsyncMock(
            list_fargate_profiles=mock_list_fargate_profiles,
            delete_fargate_profile=mock_delete_fargate_profile,
        )

        await self.trigger.delete_any_fargate_profiles(mock_client)

        mock_list_fargate_profiles.assert_called_once_with(clusterName=CLUSTER_NAME)
        self.mock_async_wait.assert_not_called()
        mock_delete_fargate_profile.assert_not_called()
        self.trigger.log.info.assert_called_once_with(
            "No Fargate profiles associated with cluster %s", CLUSTER_NAME
        )


class TestEksPodTrigger:
    """Tests for EksPodTrigger."""

    TRIGGER_START_TIME = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)

    def _create_trigger(self, **overrides):
        """Create an EksPodTrigger with sensible defaults."""
        defaults = {
            "eks_cluster_name": CLUSTER_NAME,
            "aws_conn_id": AWS_CONN_ID,
            "region": REGION_NAME,
            "pod_name": "test-pod",
            "pod_namespace": "default",
            "trigger_start_time": self.TRIGGER_START_TIME,
            "base_container_name": "base",
            "config_dict": {"old": "stale-config"},
        }
        defaults.update(overrides)
        return EksPodTrigger(**defaults)

    def test_serialize_includes_eks_fields(self):
        """serialize() should include eks_cluster_name, aws_conn_id, and region."""
        trigger = self._create_trigger()
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.amazon.aws.triggers.eks.EksPodTrigger"
        assert kwargs["eks_cluster_name"] == CLUSTER_NAME
        assert kwargs["aws_conn_id"] == AWS_CONN_ID
        assert kwargs["region"] == REGION_NAME
        # Also verify parent fields are present
        assert kwargs["pod_name"] == "test-pod"
        assert kwargs["pod_namespace"] == "default"

    def test_serialize_roundtrip(self):
        """A trigger created from serialized kwargs should serialize identically."""
        trigger = self._create_trigger()
        classpath, kwargs = trigger.serialize()

        trigger2 = EksPodTrigger(**kwargs)
        classpath2, kwargs2 = trigger2.serialize()

        assert classpath == classpath2
        assert kwargs == kwargs2

    @pytest.mark.asyncio
    @patch("airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger.run")
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.generate_config_file")
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook._secure_credential_context")
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_session")
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.__init__", return_value=None)
    async def test_run_generates_fresh_kubeconfig(
        self,
        mock_eks_hook_init,
        mock_get_session,
        mock_secure_credential_context,
        mock_generate_config_file,
        mock_parent_run,
    ):
        """run() should get fresh credentials, generate kubeconfig, and delegate to parent."""
        # Set up credential mocks
        mock_session = MagicMock()
        mock_credentials = MagicMock()
        mock_frozen = MagicMock()
        mock_frozen.access_key = "AKIATEST"
        mock_frozen.secret_key = "secret123"
        mock_frozen.token = "token456"
        mock_get_session.return_value = mock_session
        mock_session.get_credentials.return_value = mock_credentials
        mock_credentials.get_frozen_credentials.return_value = mock_frozen

        # Set up context manager mocks
        mock_secure_credential_context.return_value.__enter__.return_value = "/tmp/test.aws_creds"
        mock_generate_config_file.return_value.__enter__.return_value = "/tmp/test_kubeconfig"

        # Mock reading the kubeconfig file
        with patch("pathlib.Path.read_text", return_value="apiVersion: v1\nkind: Config\nclusters: []"):

            async def mock_gen():
                yield TriggerEvent({"status": "success"})

            mock_parent_run.return_value = mock_gen()

            trigger = self._create_trigger()
            events = []
            async for event in trigger.run():
                events.append(event)

        assert len(events) == 1
        assert events[0] == TriggerEvent({"status": "success"})

        # Verify credentials were fetched
        mock_eks_hook_init.assert_called_once_with(aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME)
        mock_get_session.assert_called_once()
        mock_session.get_credentials.assert_called_once()
        mock_credentials.get_frozen_credentials.assert_called_once()

        # Verify credential context and config generation
        mock_secure_credential_context.assert_called_once_with("AKIATEST", "secret123", "token456")
        mock_generate_config_file.assert_called_once_with(
            eks_cluster_name=CLUSTER_NAME,
            pod_namespace="default",
            credentials_file="/tmp/test.aws_creds",
        )

    @pytest.mark.asyncio
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_session")
    @patch("airflow.providers.amazon.aws.hooks.eks.EksHook.__init__", return_value=None)
    async def test_run_raises_when_credentials_unavailable(
        self,
        mock_eks_hook_init,
        mock_get_session,
    ):
        """run() should raise RuntimeError when credentials cannot be retrieved."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.get_credentials.return_value = None

        trigger = self._create_trigger()

        with pytest.raises(RuntimeError, match="Unable to retrieve AWS credentials"):
            async for _ in trigger.run():
                pass
