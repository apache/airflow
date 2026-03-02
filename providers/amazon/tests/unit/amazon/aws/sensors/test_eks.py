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

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.eks import (
    ClusterStates,
    EksHook,
    FargateProfileStates,
    NodegroupStates,
)
from airflow.providers.amazon.aws.sensors.eks import (
    CLUSTER_TERMINAL_STATES,
    FARGATE_TERMINAL_STATES,
    NODEGROUP_TERMINAL_STATES,
    EksClusterStateSensor,
    EksFargateProfileStateSensor,
    EksNodegroupStateSensor,
)
from airflow.providers.common.compat.sdk import AirflowException

CLUSTER_NAME = "test_cluster"
FARGATE_PROFILE_NAME = "test_profile"
NODEGROUP_NAME = "test_nodegroup"
TASK_ID = "test_eks_sensor"

# We need to sort the states as they are used in parameterized and python-xdist requires stable order
# and since Enums cannot be compared to each other, we need to use name
# See https://pytest-xdist.readthedocs.io/en/latest/known-limitations.html
CLUSTER_PENDING_STATES = sorted(
    frozenset(ClusterStates) - frozenset(CLUSTER_TERMINAL_STATES), key=lambda x: x.name
)
FARGATE_PENDING_STATES = sorted(
    frozenset(FargateProfileStates) - frozenset(FARGATE_TERMINAL_STATES), key=lambda x: x.name
)
NODEGROUP_PENDING_STATES = sorted(
    frozenset(NodegroupStates) - frozenset(NODEGROUP_TERMINAL_STATES), key=lambda x: x.name
)

CLUSTER_UNEXPECTED_TERMINAL_STATES = sorted(
    CLUSTER_TERMINAL_STATES - {ClusterStates.ACTIVE}, key=lambda x: x.name
)

FARGATE_UNEXPECTED_TERMINAL_STATES = sorted(
    FARGATE_TERMINAL_STATES - {FargateProfileStates.ACTIVE}, key=lambda x: x.name
)

NODEGROUP_UNEXPECTED_TERMINAL_STATES = sorted(
    NODEGROUP_TERMINAL_STATES - {NodegroupStates.ACTIVE}, key=lambda x: x.name
)


class TestEksClusterStateSensor:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.target_state = ClusterStates.ACTIVE
        self.sensor = EksClusterStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            target_state=self.target_state,
        )

    @mock.patch.object(EksHook, "get_cluster_state", return_value=ClusterStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_cluster_state):
        assert self.sensor.poke({}) is True
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_cluster_state")
    @pytest.mark.parametrize("pending_state", CLUSTER_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_cluster_state, pending_state):
        mock_get_cluster_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_cluster_state")
    @pytest.mark.parametrize("unexpected_terminal_state", CLUSTER_UNEXPECTED_TERMINAL_STATES)
    def test_poke_reached_unexpected_terminal_state(self, mock_get_cluster_state, unexpected_terminal_state):
        expected_message = (
            f"Terminal state reached. Current state: {unexpected_terminal_state}, "
            f"Expected state: {self.target_state}"
        )
        mock_get_cluster_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)

    @pytest.mark.db_test
    def test_region_argument(self, sdk_connection_not_found):
        with pytest.warns(AirflowProviderDeprecationWarning) as w:
            w.sensor = EksClusterStateSensor(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                target_state=self.target_state,
                aws_conn_id="test_conn_id",
                region="us-east-1",
            )
        assert w.sensor.hook.region_name == "us-east-1"


class TestEksFargateProfileStateSensor:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.target_state = FargateProfileStates.ACTIVE
        self.sensor = EksFargateProfileStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            fargate_profile_name=FARGATE_PROFILE_NAME,
            target_state=self.target_state,
        )

    @pytest.mark.db_test
    def test_region_argument(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as w:
            w.sensor = EksFargateProfileStateSensor(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                fargate_profile_name=FARGATE_PROFILE_NAME,
                target_state=self.target_state,
                region="us-east-2",
            )
        assert w.sensor.region_name == "us-east-2"

    @mock.patch.object(EksHook, "get_fargate_profile_state", return_value=FargateProfileStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_fargate_profile_state):
        assert self.sensor.poke({}) is True
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_fargate_profile_state")
    @pytest.mark.parametrize("pending_state", FARGATE_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_fargate_profile_state, pending_state):
        mock_get_fargate_profile_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_fargate_profile_state")
    @pytest.mark.parametrize("unexpected_terminal_state", FARGATE_UNEXPECTED_TERMINAL_STATES)
    def test_poke_reached_unexpected_terminal_state(
        self, mock_get_fargate_profile_state, unexpected_terminal_state
    ):
        expected_message = (
            f"Terminal state reached. Current state: {unexpected_terminal_state}, "
            f"Expected state: {self.target_state}"
        )
        mock_get_fargate_profile_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )


class TestEksNodegroupStateSensor:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.target_state = NodegroupStates.ACTIVE
        self.sensor = EksNodegroupStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            nodegroup_name=NODEGROUP_NAME,
            target_state=self.target_state,
        )

    @pytest.mark.db_test
    def test_region_argument(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as w:
            w.sensor = EksNodegroupStateSensor(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                nodegroup_name=NODEGROUP_NAME,
                target_state=self.target_state,
                region="us-east-2",
            )
        assert w.sensor.region_name == "us-east-2"

    @mock.patch.object(EksHook, "get_nodegroup_state", return_value=NodegroupStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_nodegroup_state):
        assert self.sensor.poke({}) is True
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_nodegroup_state")
    @pytest.mark.parametrize("pending_state", NODEGROUP_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_nodegroup_state, pending_state):
        mock_get_nodegroup_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_nodegroup_state")
    @pytest.mark.parametrize("unexpected_terminal_state", NODEGROUP_UNEXPECTED_TERMINAL_STATES)
    def test_poke_reached_unexpected_terminal_state(
        self, mock_get_nodegroup_state, unexpected_terminal_state
    ):
        expected_message = (
            f"Terminal state reached. Current state: {unexpected_terminal_state}, "
            f"Expected state: {self.target_state}"
        )
        mock_get_nodegroup_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )
