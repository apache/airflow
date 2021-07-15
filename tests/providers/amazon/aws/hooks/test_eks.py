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
#
from copy import deepcopy
from typing import Dict, List, Optional, Tuple, Type
from unittest import mock
from urllib.parse import ParseResult, urlparse

import pytest
from _pytest._code import ExceptionInfo
from botocore.exceptions import ClientError
from freezegun import freeze_time
from moto.core import ACCOUNT_ID
from moto.core.exceptions import AWSError
from moto.eks.exceptions import (
    InvalidParameterException,
    InvalidRequestException,
    ResourceInUseException,
    ResourceNotFoundException,
)
from moto.eks.models import (
    CLUSTER_EXISTS_MSG,
    CLUSTER_IN_USE_MSG,
    CLUSTER_NOT_FOUND_MSG,
    CLUSTER_NOT_READY_MSG,
    LAUNCH_TEMPLATE_WITH_DISK_SIZE_MSG,
    LAUNCH_TEMPLATE_WITH_REMOTE_ACCESS_MSG,
    NODEGROUP_EXISTS_MSG,
    NODEGROUP_NOT_FOUND_MSG,
)

from airflow.providers.amazon.aws.hooks.eks import EKSHook

from ..utils.eks_test_constants import (
    DEFAULT_CONN_ID,
    DISK_SIZE,
    FROZEN_TIME,
    INSTANCE_TYPES,
    LAUNCH_TEMPLATE,
    NON_EXISTING_CLUSTER_NAME,
    NON_EXISTING_NODEGROUP_NAME,
    PACKAGE_NOT_PRESENT_MSG,
    PARTITION,
    REGION,
    REMOTE_ACCESS,
    BatchCountSize,
    ClusterAttributes,
    ClusterInputs,
    ErrorAttributes,
    NodegroupAttributes,
    NodegroupInputs,
    PossibleTestResults,
    RegExTemplates,
    ResponseAttributes,
)
from ..utils.eks_test_utils import (
    attributes_to_test,
    generate_clusters,
    generate_nodegroups,
    iso_date,
    region_matches_partition,
)

try:
    from moto import mock_eks
except ImportError:
    mock_eks = None


@pytest.fixture(scope="function")
def cluster_builder():
    """A fixture to generate a batch of EKS Clusters on the mocked backend for testing."""

    class ClusterTestDataFactory:
        """A Factory class for building the Cluster objects."""

        def __init__(self, count: int, minimal: bool) -> None:
            # Generate 'count' number of random Cluster objects.
            self.cluster_names: List[str] = generate_clusters(
                eks_hook=eks_hook, num_clusters=count, minimal=minimal
            )

            self.existing_cluster_name: str = self.cluster_names[0]
            self.nonexistent_cluster_name: str = NON_EXISTING_CLUSTER_NAME

            # Collect the output of describe_cluster() for the first Cluster.
            self.cluster_describe_output: Dict = eks_hook.describe_cluster(name=self.existing_cluster_name)[
                ResponseAttributes.CLUSTER
            ]

            # Generate a list of the Cluster attributes to be tested when validating results.
            self.attributes_to_test: List[Tuple] = attributes_to_test(
                inputs=ClusterInputs, cluster_name=self.existing_cluster_name
            )

    def _execute(
        count: Optional[int] = 1, minimal: Optional[bool] = True
    ) -> Tuple[EKSHook, ClusterTestDataFactory]:
        return eks_hook, ClusterTestDataFactory(count=count, minimal=minimal)

    mock_eks().start()
    eks_hook = EKSHook(
        aws_conn_id=DEFAULT_CONN_ID,
        region_name=REGION,
    )
    yield _execute
    mock_eks().stop()


@pytest.fixture(scope="function")
def nodegroup_builder(cluster_builder):
    """A fixture to generate a batch of EKSManaged Nodegroups on the mocked backend for testing."""

    class NodegroupTestDataFactory:
        """A Factory class for building the Cluster objects."""

        def __init__(self, count: int, minimal: bool) -> None:
            self.cluster_name: str = cluster.existing_cluster_name

            # Generate 'count' number of random Nodegroup objects.
            self.nodegroup_names: List[str] = generate_nodegroups(
                eks_hook=eks_hook, cluster_name=self.cluster_name, num_nodegroups=count, minimal=minimal
            )

            # Get the name of the first generated Nodegroup.
            self.existing_nodegroup_name: str = self.nodegroup_names[0]
            self.nonexistent_nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
            self.nonexistent_cluster_name: str = NON_EXISTING_CLUSTER_NAME

            # Collect the output of describe_nodegroup() for the first Nodegroup.
            self.nodegroup_describe_output: Dict = eks_hook.describe_nodegroup(
                clusterName=self.cluster_name, nodegroupName=self.existing_nodegroup_name
            )[ResponseAttributes.NODEGROUP]

            # Generate a list of the Nodegroup attributes to be tested when validating results.
            self.attributes_to_test: List[Tuple] = attributes_to_test(
                inputs=NodegroupInputs,
                cluster_name=self.cluster_name,
                nodegroup_name=self.existing_nodegroup_name,
            )

    def _execute(
        count: Optional[int] = 1, minimal: Optional[bool] = True
    ) -> Tuple[EKSHook, NodegroupTestDataFactory]:
        return eks_hook, NodegroupTestDataFactory(count=count, minimal=minimal)

    eks_hook, cluster = cluster_builder()
    return _execute


@pytest.mark.skipif(mock_eks is None, reason=PACKAGE_NOT_PRESENT_MSG)
class TestEKSHooks:
    def test_hook(self, cluster_builder) -> None:
        eks_hook, _ = cluster_builder()
        assert eks_hook.get_conn() is not None
        assert eks_hook.aws_conn_id == DEFAULT_CONN_ID
        assert eks_hook.region_name == REGION

    ###
    # This specific test does not use the fixture since
    # it is intended to verify that there are no clusters
    # in the list at initialization, which means the mock
    # decorator must be used manually in this one case.
    ###
    @mock_eks
    def test_list_clusters_returns_empty_by_default(self) -> None:
        eks_hook: EKSHook = EKSHook(aws_conn_id=DEFAULT_CONN_ID, region_name=REGION)

        result: List = eks_hook.list_clusters()

        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_clusters_returns_sorted_cluster_names(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_result: List = sorted(generated_test_data.cluster_names)

        result: List = eks_hook.list_clusters()

        assert_result_matches_expected_list(result, expected_result, initial_batch_size)

    def test_list_clusters_returns_all_results(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.LARGE
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_result: List = sorted(generated_test_data.cluster_names)

        result: List = eks_hook.list_clusters()

        assert_result_matches_expected_list(result, expected_result)

    def test_create_cluster_throws_exception_when_cluster_exists(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_exception: Type[AWSError] = ResourceInUseException
        expected_msg: str = CLUSTER_EXISTS_MSG.format(
            clusterName=generated_test_data.existing_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_cluster(
                name=generated_test_data.existing_cluster_name, **dict(ClusterInputs.REQUIRED)
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new cluster was created.
        len_after_test: int = len(eks_hook.list_clusters())
        assert len_after_test == initial_batch_size

    def test_create_cluster_generates_valid_cluster_arn(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder()
        expected_arn_values: List = [
            PARTITION,
            REGION,
            ACCOUNT_ID,
            generated_test_data.cluster_names,
        ]

        assert_all_arn_values_are_valid(
            expected_arn_values=expected_arn_values,
            pattern=RegExTemplates.CLUSTER_ARN,
            arn_under_test=generated_test_data.cluster_describe_output[ClusterAttributes.ARN],
        )

    @freeze_time(FROZEN_TIME)
    def test_create_cluster_generates_valid_cluster_created_timestamp(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder()

        result_time: str = generated_test_data.cluster_describe_output[ClusterAttributes.CREATED_AT]

        assert iso_date(result_time) == FROZEN_TIME

    def test_create_cluster_generates_valid_cluster_endpoint(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder()

        result_endpoint: str = generated_test_data.cluster_describe_output[ClusterAttributes.ENDPOINT]

        assert_is_valid_uri(result_endpoint)

    def test_create_cluster_generates_valid_oidc_identity(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder()

        result_issuer: str = generated_test_data.cluster_describe_output[ClusterAttributes.IDENTITY][
            ClusterAttributes.OIDC
        ][ClusterAttributes.ISSUER]

        assert_is_valid_uri(result_issuer)

    def test_create_cluster_saves_provided_parameters(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder(minimal=False)

        for key, expected_value in generated_test_data.attributes_to_test:
            assert generated_test_data.cluster_describe_output[key] == expected_value

    def test_describe_cluster_throws_exception_when_cluster_not_found(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.describe_cluster(name=generated_test_data.nonexistent_cluster_name)

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_delete_cluster_returns_deleted_cluster(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size, minimal=False)

        result: Dict = eks_hook.delete_cluster(name=generated_test_data.existing_cluster_name)[
            ResponseAttributes.CLUSTER
        ]

        for key, expected_value in generated_test_data.attributes_to_test:
            assert result[key] == expected_value

    def test_delete_cluster_removes_deleted_cluster(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size, minimal=False)

        eks_hook.delete_cluster(name=generated_test_data.existing_cluster_name)
        result_cluster_list: List = eks_hook.list_clusters()

        assert len(result_cluster_list) == (initial_batch_size - 1)
        assert generated_test_data.existing_cluster_name not in result_cluster_list

    def test_delete_cluster_throws_exception_when_cluster_not_found(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_cluster(name=generated_test_data.nonexistent_cluster_name)

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify nothing was deleted.
        cluster_count_after_test: int = len(eks_hook.list_clusters())
        assert cluster_count_after_test == initial_batch_size

    def test_list_nodegroups_returns_empty_by_default(self, cluster_builder) -> None:
        eks_hook, generated_test_data = cluster_builder()

        result: List = eks_hook.list_nodegroups(clusterName=generated_test_data.existing_cluster_name)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_nodegroups_returns_sorted_nodegroup_names(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_result: List = sorted(generated_test_data.nodegroup_names)

        result: List = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result, initial_batch_size)

    def test_list_nodegroups_returns_all_results(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.LARGE
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_result: List = sorted(generated_test_data.nodegroup_names)

        result: List = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result)

    @mock_eks
    def test_create_nodegroup_throws_exception_when_cluster_not_found(self) -> None:
        eks_hook: EKSHook = EKSHook(aws_conn_id=DEFAULT_CONN_ID, region_name=REGION)
        non_existent_cluster_name: str = NON_EXISTING_CLUSTER_NAME
        non_existent_nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=non_existent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_nodegroup(
                clusterName=non_existent_cluster_name,
                nodegroupName=non_existent_nodegroup_name,
                **dict(NodegroupInputs.REQUIRED),
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_create_nodegroup_throws_exception_when_nodegroup_already_exists(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_exception: Type[AWSError] = ResourceInUseException
        expected_msg: str = NODEGROUP_EXISTS_MSG.format(
            clusterName=generated_test_data.cluster_name,
            nodegroupName=generated_test_data.existing_nodegroup_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_nodegroup(
                clusterName=generated_test_data.cluster_name,
                nodegroupName=generated_test_data.existing_nodegroup_name,
                **dict(NodegroupInputs.REQUIRED),
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new nodegroup was created.
        nodegroup_count_after_test = len(
            eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)
        )
        assert nodegroup_count_after_test == initial_batch_size

    def test_create_nodegroup_throws_exception_when_cluster_not_active(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        non_existent_nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
        expected_exception: Type[AWSError] = InvalidRequestException
        expected_msg: str = CLUSTER_NOT_READY_MSG.format(
            clusterName=generated_test_data.cluster_name,
        )

        with mock.patch("moto.eks.models.Cluster.isActive", return_value=False):
            with pytest.raises(ClientError) as raised_exception:
                eks_hook.create_nodegroup(
                    clusterName=generated_test_data.cluster_name,
                    nodegroupName=non_existent_nodegroup_name,
                    **dict(NodegroupInputs.REQUIRED),
                )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new nodegroup was created.
        nodegroup_count_after_test = len(
            eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)
        )
        assert nodegroup_count_after_test == initial_batch_size

    def test_create_nodegroup_generates_valid_nodegroup_arn(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        expected_arn_values: List = [
            PARTITION,
            REGION,
            ACCOUNT_ID,
            generated_test_data.cluster_name,
            generated_test_data.nodegroup_names,
            None,
        ]

        assert_all_arn_values_are_valid(
            expected_arn_values=expected_arn_values,
            pattern=RegExTemplates.NODEGROUP_ARN,
            arn_under_test=generated_test_data.nodegroup_describe_output[NodegroupAttributes.ARN],
        )

    @freeze_time(FROZEN_TIME)
    def test_create_nodegroup_generates_valid_nodegroup_created_timestamp(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()

        result_time: str = generated_test_data.nodegroup_describe_output[NodegroupAttributes.CREATED_AT]

        assert iso_date(result_time) == FROZEN_TIME

    @freeze_time(FROZEN_TIME)
    def test_create_nodegroup_generates_valid_nodegroup_modified_timestamp(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()

        result_time: str = generated_test_data.nodegroup_describe_output[NodegroupAttributes.MODIFIED_AT]

        assert iso_date(result_time) == FROZEN_TIME

    def test_create_nodegroup_generates_valid_autoscaling_group_name(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        result_resources: Dict = generated_test_data.nodegroup_describe_output[NodegroupAttributes.RESOURCES]

        result_asg_name: str = result_resources[NodegroupAttributes.AUTOSCALING_GROUPS][0][
            NodegroupAttributes.NAME
        ]

        assert RegExTemplates.NODEGROUP_ASG_NAME_PATTERN.match(result_asg_name)

    def test_create_nodegroup_generates_valid_security_group_name(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        result_resources: Dict = generated_test_data.nodegroup_describe_output[NodegroupAttributes.RESOURCES]

        result_security_group: str = result_resources[NodegroupAttributes.REMOTE_ACCESS_SG]

        assert RegExTemplates.NODEGROUP_SECURITY_GROUP_NAME_PATTERN.match(result_security_group)

    def test_create_nodegroup_saves_provided_parameters(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder(minimal=False)

        for key, expected_value in generated_test_data.attributes_to_test:
            assert generated_test_data.nodegroup_describe_output[key] == expected_value

    def test_describe_nodegroup_throws_exception_when_cluster_not_found(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.describe_nodegroup(
                clusterName=generated_test_data.nonexistent_cluster_name,
                nodegroupName=generated_test_data.existing_nodegroup_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_describe_nodegroup_throws_exception_when_nodegroup_not_found(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = NODEGROUP_NOT_FOUND_MSG.format(
            nodegroupName=generated_test_data.nonexistent_nodegroup_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.describe_nodegroup(
                clusterName=generated_test_data.cluster_name,
                nodegroupName=generated_test_data.nonexistent_nodegroup_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_delete_cluster_throws_exception_when_nodegroups_exist(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: Type[AWSError] = ResourceInUseException
        expected_msg: str = CLUSTER_IN_USE_MSG

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_cluster(name=generated_test_data.cluster_name)

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no clusters were deleted.
        cluster_count_after_test: int = len(eks_hook.list_clusters())
        assert cluster_count_after_test == BatchCountSize.SINGLE

    def test_delete_nodegroup_removes_deleted_nodegroup(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)

        eks_hook.delete_nodegroup(
            clusterName=generated_test_data.cluster_name,
            nodegroupName=generated_test_data.existing_nodegroup_name,
        )
        result_nodegroup_list: List = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert len(result_nodegroup_list) == (initial_batch_size - 1)
        assert generated_test_data.existing_nodegroup_name not in result_nodegroup_list

    def test_delete_nodegroup_returns_deleted_nodegroup(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size, minimal=False)

        result: Dict = eks_hook.delete_nodegroup(
            clusterName=generated_test_data.cluster_name,
            nodegroupName=generated_test_data.existing_nodegroup_name,
        )[ResponseAttributes.NODEGROUP]

        for key, expected_value in generated_test_data.attributes_to_test:
            assert result[key] == expected_value

    def test_delete_nodegroup_throws_exception_when_cluster_not_found(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_nodegroup(
                clusterName=generated_test_data.nonexistent_cluster_name,
                nodegroupName=generated_test_data.existing_nodegroup_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_delete_nodegroup_throws_exception_when_nodegroup_not_found(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_exception: Type[AWSError] = ResourceNotFoundException
        expected_msg: str = NODEGROUP_NOT_FOUND_MSG.format(
            nodegroupName=generated_test_data.nonexistent_nodegroup_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_nodegroup(
                clusterName=generated_test_data.cluster_name,
                nodegroupName=generated_test_data.nonexistent_nodegroup_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new nodegroup was created.
        nodegroup_count_after_test: int = len(
            eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)
        )
        assert nodegroup_count_after_test == initial_batch_size

    # If launch_template is specified, you can not specify instanceTypes, diskSize, or remoteAccess.
    test_cases = [
        # Happy Paths
        (LAUNCH_TEMPLATE, None, None, None, PossibleTestResults.SUCCESS),
        (None, INSTANCE_TYPES, DISK_SIZE, REMOTE_ACCESS, PossibleTestResults.SUCCESS),
        (None, None, DISK_SIZE, REMOTE_ACCESS, PossibleTestResults.SUCCESS),
        (None, INSTANCE_TYPES, None, REMOTE_ACCESS, PossibleTestResults.SUCCESS),
        (None, INSTANCE_TYPES, DISK_SIZE, None, PossibleTestResults.SUCCESS),
        (None, INSTANCE_TYPES, None, None, PossibleTestResults.SUCCESS),
        (None, None, DISK_SIZE, None, PossibleTestResults.SUCCESS),
        (None, None, None, REMOTE_ACCESS, PossibleTestResults.SUCCESS),
        (None, None, None, None, PossibleTestResults.SUCCESS),
        # Unhappy Paths
        (LAUNCH_TEMPLATE, INSTANCE_TYPES, None, None, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, None, DISK_SIZE, None, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, None, None, REMOTE_ACCESS, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, INSTANCE_TYPES, DISK_SIZE, None, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, INSTANCE_TYPES, None, REMOTE_ACCESS, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, None, DISK_SIZE, REMOTE_ACCESS, PossibleTestResults.FAILURE),
        (LAUNCH_TEMPLATE, INSTANCE_TYPES, DISK_SIZE, REMOTE_ACCESS, PossibleTestResults.FAILURE),
    ]

    @pytest.mark.parametrize(
        "launch_template, instance_types, disk_size, remote_access, expected_result",
        test_cases,
    )
    def test_create_nodegroup_handles_launch_template_combinations(
        self,
        cluster_builder,
        launch_template,
        instance_types,
        disk_size,
        remote_access,
        expected_result,
    ):
        eks_hook, generated_test_data = cluster_builder()
        nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
        expected_exception: Type[AWSError] = InvalidParameterException
        expected_message: str = ""

        test_inputs = dict(
            deepcopy(
                # Required Constants
                NodegroupInputs.REQUIRED
                # Required Variables
                + [
                    (
                        ClusterAttributes.CLUSTER_NAME,
                        generated_test_data.existing_cluster_name,
                    ),
                    (NodegroupAttributes.NODEGROUP_NAME, nodegroup_name),
                ]
                # Test Case Values
                + [_ for _ in [launch_template, instance_types, disk_size, remote_access] if _]
            )
        )

        if expected_result == PossibleTestResults.SUCCESS:
            result: Dict = eks_hook.create_nodegroup(**test_inputs)[ResponseAttributes.NODEGROUP]

            for key, expected_value in test_inputs.items():
                assert result[key] == expected_value
        else:
            if launch_template and disk_size:
                expected_message = LAUNCH_TEMPLATE_WITH_DISK_SIZE_MSG
            elif launch_template and remote_access:
                expected_message = LAUNCH_TEMPLATE_WITH_REMOTE_ACCESS_MSG
            # Docs say this combination throws an exception but testing shows that
            # instanceTypes overrides the launchTemplate instance values instead.
            # Leaving here for easier correction if/when that gets fixed.
            elif launch_template and instance_types:
                pass

        if expected_message:
            with pytest.raises(ClientError) as raised_exception:
                eks_hook.create_nodegroup(**test_inputs)
                assert_client_error_exception_thrown(
                    expected_exception=expected_exception,
                    expected_msg=expected_message,
                    raised_exception=raised_exception,
                )


# Helper methods for repeated assert combinations.
def assert_all_arn_values_are_valid(expected_arn_values, pattern, arn_under_test) -> None:
    """
    Applies regex `pattern` to `arn_under_test` and asserts
    that each group matches the provided expected value.
    A list entry of None in the 'expected_arn_values' will
    assert that the value exists but not match a specific value.
    """
    findall: List = pattern.findall(arn_under_test)[0]
    # findall() returns a list of matches from right to left so it must be reversed
    # in order to match the logical order of the 'expected_arn_values' list.
    for value in reversed(findall):
        expected_value = expected_arn_values.pop()
        if expected_value:
            assert value in expected_value
        else:
            assert value
    assert region_matches_partition(findall[1], findall[0])


def assert_client_error_exception_thrown(
    expected_exception: Type[AWSError], expected_msg: str, raised_exception: ExceptionInfo
) -> None:
    """
    Asserts that the raised exception is of the expected type
    and the resulting message matches the expected format.
    """
    response = raised_exception.value.response[ErrorAttributes.ERROR]
    assert response[ErrorAttributes.CODE] == expected_exception.TYPE
    assert response[ErrorAttributes.MESSAGE] == expected_msg


def assert_result_matches_expected_list(
    result: List, expected_result: List, expected_len: Optional[int] = None
) -> None:
    assert result == expected_result
    assert len(result) == expected_len or len(expected_result)


def assert_is_valid_uri(value: str) -> None:
    result: ParseResult = urlparse(value)

    assert all([result.scheme, result.netloc, result.path])
    assert REGION in value
