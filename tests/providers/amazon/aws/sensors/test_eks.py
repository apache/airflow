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

from airflow.exceptions import AirflowException
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
    UNEXPECTED_TERMINAL_STATE_MSG,
    EksClusterStateSensor,
    EksFargateProfileStateSensor,
    EksNodegroupStateSensor,
)

CLUSTER_NAME = "test_cluster"
FARGATE_PROFILE_NAME = "test_profile"
NODEGROUP_NAME = "test_nodegroup"
TASK_ID = "test_eks_sensor"

CLUSTER_PENDING_STATES = frozenset(frozenset({state for state in ClusterStates}) - CLUSTER_TERMINAL_STATES)
FARGATE_PENDING_STATES = frozenset(
    frozenset({state for state in FargateProfileStates}) - FARGATE_TERMINAL_STATES
)
NODEGROUP_PENDING_STATES = frozenset(
    frozenset({state for state in NodegroupStates}) - NODEGROUP_TERMINAL_STATES
)


class TestEksClusterStateSensor:
    @pytest.fixture(scope="function")
    def setUp(self):
        self.target_state = ClusterStates.ACTIVE
        self.sensor = EksClusterStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            target_state=self.target_state,
        )

    @mock.patch.object(EksHook, "get_cluster_state", return_value=ClusterStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_cluster_state, setUp):
        assert self.sensor.poke({}) is True
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_cluster_state")
    @pytest.mark.parametrize("pending_state", CLUSTER_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_cluster_state, setUp, pending_state):
        mock_get_cluster_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_cluster_state")
    @pytest.mark.parametrize("unexpected_terminal_state", CLUSTER_TERMINAL_STATES - {ClusterStates.ACTIVE})
    def test_poke_reached_unexpected_terminal_state(
        self, mock_get_cluster_state, setUp, unexpected_terminal_state
    ):
        expected_message = UNEXPECTED_TERMINAL_STATE_MSG.format(
            current_state=unexpected_terminal_state, target_state=self.target_state
        )
        mock_get_cluster_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_cluster_state.assert_called_once_with(clusterName=CLUSTER_NAME)


class TestEksFargateProfileStateSensor:
    @pytest.fixture(scope="function")
    def setUp(self):
        self.target_state = FargateProfileStates.ACTIVE
        self.sensor = EksFargateProfileStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            fargate_profile_name=FARGATE_PROFILE_NAME,
            target_state=self.target_state,
        )

    @mock.patch.object(EksHook, "get_fargate_profile_state", return_value=FargateProfileStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_fargate_profile_state, setUp):
        assert self.sensor.poke({}) is True
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_fargate_profile_state")
    @pytest.mark.parametrize("pending_state", FARGATE_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_fargate_profile_state, setUp, pending_state):
        mock_get_fargate_profile_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_fargate_profile_state")
    @pytest.mark.parametrize(
        "unexpected_terminal_state", FARGATE_TERMINAL_STATES - {FargateProfileStates.ACTIVE}
    )
    def test_poke_reached_unexpected_terminal_state(
        self, mock_get_fargate_profile_state, setUp, unexpected_terminal_state
    ):
        expected_message = UNEXPECTED_TERMINAL_STATE_MSG.format(
            current_state=unexpected_terminal_state, target_state=self.target_state
        )
        mock_get_fargate_profile_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_fargate_profile_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, fargateProfileName=FARGATE_PROFILE_NAME
        )


class TestEksNodegroupStateSensor:
    @pytest.fixture(scope="function")
    def setUp(self):
        self.target_state = NodegroupStates.ACTIVE
        self.sensor = EksNodegroupStateSensor(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            nodegroup_name=NODEGROUP_NAME,
            target_state=self.target_state,
        )

    @mock.patch.object(EksHook, "get_nodegroup_state", return_value=NodegroupStates.ACTIVE)
    def test_poke_reached_target_state(self, mock_get_nodegroup_state, setUp):
        assert self.sensor.poke({}) is True
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_nodegroup_state")
    @pytest.mark.parametrize("pending_state", NODEGROUP_PENDING_STATES)
    def test_poke_reached_pending_state(self, mock_get_nodegroup_state, setUp, pending_state):
        mock_get_nodegroup_state.return_value = pending_state

        assert self.sensor.poke({}) is False
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.get_nodegroup_state")
    @pytest.mark.parametrize(
        "unexpected_terminal_state", NODEGROUP_TERMINAL_STATES - {NodegroupStates.ACTIVE}
    )
    def test_poke_reached_unexpected_terminal_state(
        self, mock_get_nodegroup_state, setUp, unexpected_terminal_state
    ):
        expected_message = UNEXPECTED_TERMINAL_STATE_MSG.format(
            current_state=unexpected_terminal_state, target_state=self.target_state
        )
        mock_get_nodegroup_state.return_value = unexpected_terminal_state

        with pytest.raises(AirflowException) as raised_exception:
            self.sensor.poke({})

        assert str(raised_exception.value) == expected_message
        mock_get_nodegroup_state.assert_called_once_with(
            clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME
        )
