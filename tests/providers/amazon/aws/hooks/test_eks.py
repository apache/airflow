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

import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest import mock
from urllib.parse import ParseResult, urlsplit

import pytest
import time_machine
import yaml
from _pytest._code import ExceptionInfo
from botocore.exceptions import ClientError
from moto import mock_eks
from moto.core import DEFAULT_ACCOUNT_ID
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
    FARGATE_PROFILE_EXISTS_MSG,
    FARGATE_PROFILE_NEEDS_SELECTOR_MSG,
    FARGATE_PROFILE_NOT_FOUND_MSG,
    FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE,
    FARGATE_PROFILE_TOO_MANY_LABELS,
    LAUNCH_TEMPLATE_WITH_DISK_SIZE_MSG,
    LAUNCH_TEMPLATE_WITH_REMOTE_ACCESS_MSG,
    NODEGROUP_EXISTS_MSG,
    NODEGROUP_NOT_FOUND_MSG,
)

from airflow.providers.amazon.aws.hooks.eks import EksHook

from ..utils.eks_test_constants import (
    DEFAULT_CONN_ID,
    DEFAULT_NAMESPACE,
    DISK_SIZE,
    FROZEN_TIME,
    INSTANCE_TYPES,
    LAUNCH_TEMPLATE,
    MAX_FARGATE_LABELS,
    NODEGROUP_OWNERSHIP_TAG_DEFAULT_VALUE,
    NODEGROUP_OWNERSHIP_TAG_KEY,
    NON_EXISTING_CLUSTER_NAME,
    NON_EXISTING_FARGATE_PROFILE_NAME,
    NON_EXISTING_NODEGROUP_NAME,
    PARTITION,
    POD_EXECUTION_ROLE_ARN,
    REGION,
    REMOTE_ACCESS,
    BatchCountSize,
    ClusterAttributes,
    ClusterInputs,
    ErrorAttributes,
    FargateProfileAttributes,
    FargateProfileInputs,
    NodegroupAttributes,
    NodegroupInputs,
    PossibleTestResults,
    RegExTemplates,
    ResponseAttributes,
)
from ..utils.eks_test_utils import (
    attributes_to_test,
    generate_clusters,
    generate_dict,
    generate_fargate_profiles,
    generate_nodegroups,
    iso_date,
    region_matches_partition,
)


@pytest.fixture(scope="function")
def cluster_builder():
    """A fixture to generate a batch of EKS Clusters on the mocked backend for testing."""

    class ClusterTestDataFactory:
        """A Factory class for building the Cluster objects."""

        def __init__(self, count: int, minimal: bool) -> None:
            # Generate 'count' number of Cluster objects.
            self.cluster_names: list[str] = generate_clusters(
                eks_hook=eks_hook, num_clusters=count, minimal=minimal
            )

            self.existing_cluster_name: str = self.cluster_names[0]
            self.nonexistent_cluster_name: str = NON_EXISTING_CLUSTER_NAME

            # Collect the output of describe_cluster() for the first Cluster.
            self.cluster_describe_output: dict = eks_hook.describe_cluster(name=self.existing_cluster_name)[
                ResponseAttributes.CLUSTER
            ]

            # Generate a list of the Cluster attributes to be tested when validating results.
            self.attributes_to_test: list[tuple] = attributes_to_test(
                inputs=ClusterInputs, cluster_name=self.existing_cluster_name
            )

    def _execute(count: int = 1, minimal: bool = True) -> tuple[EksHook, ClusterTestDataFactory]:
        return eks_hook, ClusterTestDataFactory(count=count, minimal=minimal)

    mock_eks().start()
    eks_hook = EksHook(
        aws_conn_id=DEFAULT_CONN_ID,
        region_name=REGION,
    )
    yield _execute
    mock_eks().stop()


@pytest.fixture(scope="function")
def fargate_profile_builder(cluster_builder):
    """A fixture to generate a batch of EKS Fargate profiles on the mocked backend for testing."""

    class FargateProfileTestDataFactory:
        """A Factory class for building the Fargate profile objects."""

        def __init__(self, count: int, minimal: bool) -> None:
            self.cluster_name = cluster.existing_cluster_name

            # Generate 'count' number of FargateProfile objects.
            self.fargate_profile_names = generate_fargate_profiles(
                eks_hook=eks_hook,
                cluster_name=self.cluster_name,
                num_profiles=count,
                minimal=minimal,
            )

            # Get the name of the first generated profile.
            self.existing_fargate_profile_name: str = self.fargate_profile_names[0]
            self.nonexistent_fargate_profile_name: str = NON_EXISTING_FARGATE_PROFILE_NAME
            self.nonexistent_cluster_name: str = NON_EXISTING_CLUSTER_NAME

            # Collect the output of describe_fargate_profiles() for the first profile.
            self.fargate_describe_output: dict = eks_hook.describe_fargate_profile(
                clusterName=self.cluster_name, fargateProfileName=self.existing_fargate_profile_name
            )[ResponseAttributes.FARGATE_PROFILE]

            # Generate a list of the Fargate Profile attributes to be tested when validating results.
            self.attributes_to_test: list[tuple] = attributes_to_test(
                inputs=FargateProfileInputs,
                cluster_name=self.cluster_name,
                fargate_profile_name=self.existing_fargate_profile_name,
            )

    def _execute(count: int = 1, minimal: bool = True) -> tuple[EksHook, FargateProfileTestDataFactory]:
        return eks_hook, FargateProfileTestDataFactory(count=count, minimal=minimal)

    eks_hook, cluster = cluster_builder()
    return _execute


@pytest.fixture(scope="function")
def nodegroup_builder(cluster_builder):
    """A fixture to generate a batch of EKS Managed Nodegroups on the mocked backend for testing."""

    class NodegroupTestDataFactory:
        """A Factory class for building the Nodegroup objects."""

        def __init__(self, count: int, minimal: bool) -> None:
            self.cluster_name: str = cluster.existing_cluster_name

            # Generate 'count' number of Nodegroup objects.
            self.nodegroup_names: list[str] = generate_nodegroups(
                eks_hook=eks_hook,
                cluster_name=self.cluster_name,
                num_nodegroups=count,
                minimal=minimal,
            )

            # Get the name of the first generated Nodegroup.
            self.existing_nodegroup_name: str = self.nodegroup_names[0]
            self.nonexistent_nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
            self.nonexistent_cluster_name: str = NON_EXISTING_CLUSTER_NAME

            # Collect the output of describe_nodegroup() for the first Nodegroup.
            self.nodegroup_describe_output: dict = eks_hook.describe_nodegroup(
                clusterName=self.cluster_name, nodegroupName=self.existing_nodegroup_name
            )[ResponseAttributes.NODEGROUP]

            # Generate a list of the Nodegroup attributes to be tested when validating results.
            self.attributes_to_test: list[tuple] = attributes_to_test(
                inputs=NodegroupInputs,
                cluster_name=self.cluster_name,
                nodegroup_name=self.existing_nodegroup_name,
            )

    def _execute(count: int = 1, minimal: bool = True) -> tuple[EksHook, NodegroupTestDataFactory]:
        return eks_hook, NodegroupTestDataFactory(count=count, minimal=minimal)

    eks_hook, cluster = cluster_builder()
    return _execute


class TestEksHooks:
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
        eks_hook: EksHook = EksHook(aws_conn_id=DEFAULT_CONN_ID, region_name=REGION)

        result: list = eks_hook.list_clusters()

        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_clusters_returns_sorted_cluster_names(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.cluster_names)

        result: list = eks_hook.list_clusters()

        assert_result_matches_expected_list(result, expected_result, initial_batch_size)

    def test_list_clusters_returns_all_results(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.LARGE
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.cluster_names)

        result: list = eks_hook.list_clusters()

        assert_result_matches_expected_list(result, expected_result)

    def test_create_cluster_throws_exception_when_cluster_exists(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_exception: type[AWSError] = ResourceInUseException
        expected_msg: str = CLUSTER_EXISTS_MSG.format(
            clusterName=generated_test_data.existing_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_cluster(
                name=generated_test_data.existing_cluster_name, **dict(ClusterInputs.REQUIRED)  # type: ignore
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
        expected_arn_values: list = [
            PARTITION,
            REGION,
            DEFAULT_ACCOUNT_ID,
            generated_test_data.cluster_names,
        ]

        assert_all_arn_values_are_valid(
            expected_arn_values=expected_arn_values,
            pattern=RegExTemplates.CLUSTER_ARN,
            arn_under_test=generated_test_data.cluster_describe_output[ClusterAttributes.ARN],
        )

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_create_cluster_generates_valid_cluster_created_timestamp(self, cluster_builder) -> None:
        _, generated_test_data = cluster_builder()

        result_time: datetime = generated_test_data.cluster_describe_output[ClusterAttributes.CREATED_AT]

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
        expected_exception: type[AWSError] = ResourceNotFoundException
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

        result: dict = eks_hook.delete_cluster(name=generated_test_data.existing_cluster_name)[
            ResponseAttributes.CLUSTER
        ]

        for key, expected_value in generated_test_data.attributes_to_test:
            assert result[key] == expected_value

    def test_delete_cluster_removes_deleted_cluster(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size, minimal=False)

        eks_hook.delete_cluster(name=generated_test_data.existing_cluster_name)
        result_cluster_list: list = eks_hook.list_clusters()

        assert len(result_cluster_list) == (initial_batch_size - 1)
        assert generated_test_data.existing_cluster_name not in result_cluster_list

    def test_delete_cluster_throws_exception_when_cluster_not_found(
        self, cluster_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = cluster_builder(count=initial_batch_size)
        expected_exception: type[AWSError] = ResourceNotFoundException
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

        result: list = eks_hook.list_nodegroups(clusterName=generated_test_data.existing_cluster_name)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_nodegroups_returns_sorted_nodegroup_names(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.nodegroup_names)

        result: list = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result, initial_batch_size)

    def test_list_nodegroups_returns_all_results(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.LARGE
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.nodegroup_names)

        result: list = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result)

    @mock_eks
    def test_create_nodegroup_throws_exception_when_cluster_not_found(self) -> None:
        eks_hook: EksHook = EksHook(aws_conn_id=DEFAULT_CONN_ID, region_name=REGION)
        non_existent_cluster_name: str = NON_EXISTING_CLUSTER_NAME
        non_existent_nodegroup_name: str = NON_EXISTING_NODEGROUP_NAME
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=non_existent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_nodegroup(
                clusterName=non_existent_cluster_name,
                nodegroupName=non_existent_nodegroup_name,
                **dict(NodegroupInputs.REQUIRED),  # type: ignore
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
        expected_exception: type[AWSError] = ResourceInUseException
        expected_msg: str = NODEGROUP_EXISTS_MSG.format(
            clusterName=generated_test_data.cluster_name,
            nodegroupName=generated_test_data.existing_nodegroup_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_nodegroup(
                clusterName=generated_test_data.cluster_name,
                nodegroupName=generated_test_data.existing_nodegroup_name,
                **dict(NodegroupInputs.REQUIRED),  # type: ignore
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
        expected_exception: type[AWSError] = InvalidRequestException
        expected_msg: str = CLUSTER_NOT_READY_MSG.format(
            clusterName=generated_test_data.cluster_name,
        )

        with mock.patch("moto.eks.models.Cluster.isActive", return_value=False):
            with pytest.raises(ClientError) as raised_exception:
                eks_hook.create_nodegroup(
                    clusterName=generated_test_data.cluster_name,
                    nodegroupName=non_existent_nodegroup_name,
                    **dict(NodegroupInputs.REQUIRED),  # type: ignore
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
        expected_arn_values: list = [
            PARTITION,
            REGION,
            DEFAULT_ACCOUNT_ID,
            generated_test_data.cluster_name,
            generated_test_data.nodegroup_names,
            None,
        ]

        assert_all_arn_values_are_valid(
            expected_arn_values=expected_arn_values,
            pattern=RegExTemplates.NODEGROUP_ARN,
            arn_under_test=generated_test_data.nodegroup_describe_output[NodegroupAttributes.ARN],
        )

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_create_nodegroup_generates_valid_nodegroup_created_timestamp(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()

        result_time: datetime = generated_test_data.nodegroup_describe_output[NodegroupAttributes.CREATED_AT]

        assert iso_date(result_time) == FROZEN_TIME

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_create_nodegroup_generates_valid_nodegroup_modified_timestamp(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()

        result_time: datetime = generated_test_data.nodegroup_describe_output[NodegroupAttributes.MODIFIED_AT]

        assert iso_date(result_time) == FROZEN_TIME

    def test_create_nodegroup_generates_valid_autoscaling_group_name(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        result_resources: dict = generated_test_data.nodegroup_describe_output[NodegroupAttributes.RESOURCES]

        result_asg_name: str = result_resources[NodegroupAttributes.AUTOSCALING_GROUPS][0][
            NodegroupAttributes.NAME
        ]

        assert RegExTemplates.NODEGROUP_ASG_NAME_PATTERN.match(result_asg_name)

    def test_create_nodegroup_generates_valid_security_group_name(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        result_resources: dict = generated_test_data.nodegroup_describe_output[NodegroupAttributes.RESOURCES]

        result_security_group: str = result_resources[NodegroupAttributes.REMOTE_ACCESS_SG]

        assert RegExTemplates.NODEGROUP_SECURITY_GROUP_NAME_PATTERN.match(result_security_group)

    def test_create_nodegroup_saves_provided_parameters(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder(minimal=False)

        for key, expected_value in generated_test_data.attributes_to_test:
            assert generated_test_data.nodegroup_describe_output[key] == expected_value

    def test_create_nodegroup_without_tags_uses_default(self, nodegroup_builder) -> None:
        _, generated_test_data = nodegroup_builder()
        tag_list: dict = generated_test_data.nodegroup_describe_output[NodegroupAttributes.TAGS]
        ownership_tag_key: str = NODEGROUP_OWNERSHIP_TAG_KEY.format(
            cluster_name=generated_test_data.cluster_name
        )

        assert tag_list.get(ownership_tag_key) == NODEGROUP_OWNERSHIP_TAG_DEFAULT_VALUE

    def test_create_nodegroup_with_ownership_tag_uses_provided_value(self, cluster_builder) -> None:
        eks_hook, generated_test_data = cluster_builder()
        cluster_name: str = generated_test_data.existing_cluster_name
        ownership_tag_key: str = NODEGROUP_OWNERSHIP_TAG_KEY.format(cluster_name=cluster_name)
        provided_tag_value: str = "shared"

        created_nodegroup: dict = eks_hook.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName="nodegroup",
            tags={ownership_tag_key: provided_tag_value},
            **dict(deepcopy(NodegroupInputs.REQUIRED)),
        )[ResponseAttributes.NODEGROUP]

        tags = created_nodegroup.get(NodegroupAttributes.TAGS)
        assert tags is not None
        assert tags.get(ownership_tag_key) == provided_tag_value

    def test_describe_nodegroup_throws_exception_when_cluster_not_found(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: type[AWSError] = ResourceNotFoundException
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
        expected_exception: type[AWSError] = ResourceNotFoundException
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
        expected_exception: type[AWSError] = ResourceInUseException
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
        result_nodegroup_list: list = eks_hook.list_nodegroups(clusterName=generated_test_data.cluster_name)

        assert len(result_nodegroup_list) == (initial_batch_size - 1)
        assert generated_test_data.existing_nodegroup_name not in result_nodegroup_list

    def test_delete_nodegroup_returns_deleted_nodegroup(
        self, nodegroup_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = nodegroup_builder(count=initial_batch_size, minimal=False)

        result: dict = eks_hook.delete_nodegroup(
            clusterName=generated_test_data.cluster_name,
            nodegroupName=generated_test_data.existing_nodegroup_name,
        )[ResponseAttributes.NODEGROUP]

        for key, expected_value in generated_test_data.attributes_to_test:
            assert result[key] == expected_value

    def test_delete_nodegroup_throws_exception_when_cluster_not_found(self, nodegroup_builder) -> None:
        eks_hook, generated_test_data = nodegroup_builder()
        expected_exception: type[AWSError] = ResourceNotFoundException
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
        expected_exception: type[AWSError] = ResourceNotFoundException
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
        expected_exception: type[AWSError] = InvalidParameterException
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
            result: dict = eks_hook.create_nodegroup(**test_inputs)[ResponseAttributes.NODEGROUP]

            expected_output = deepcopy(test_inputs)
            # The Create Nodegroup hook magically adds the required
            # cluster/owned tag, so add that to the expected outputs.
            expected_output["tags"] = {
                f"kubernetes.io/cluster/{generated_test_data.existing_cluster_name}": "owned"
            }

            for key, expected_value in expected_output.items():
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

    def test_list_fargate_profiles_returns_empty_by_default(self, cluster_builder) -> None:
        eks_hook, generated_test_data = cluster_builder()

        result: list = eks_hook.list_fargate_profiles(clusterName=generated_test_data.existing_cluster_name)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_fargate_profiles_returns_sorted_profile_names(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.fargate_profile_names)

        result: list = eks_hook.list_fargate_profiles(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result, initial_batch_size)

    def test_list_fargate_profiles_returns_all_results(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.LARGE
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size)
        expected_result: list = sorted(generated_test_data.fargate_profile_names)

        result: list = eks_hook.list_fargate_profiles(clusterName=generated_test_data.cluster_name)

        assert_result_matches_expected_list(result, expected_result)

    @mock_eks
    def test_create_fargate_profile_throws_exception_when_cluster_not_found(self) -> None:
        eks_hook: EksHook = EksHook(aws_conn_id=DEFAULT_CONN_ID, region_name=REGION)
        non_existent_cluster_name: str = NON_EXISTING_CLUSTER_NAME
        non_existent_fargate_profile_name: str = NON_EXISTING_FARGATE_PROFILE_NAME
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(clusterName=non_existent_cluster_name)

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_fargate_profile(
                clusterName=non_existent_cluster_name,
                fargateProfileName=non_existent_fargate_profile_name,
                **dict(FargateProfileInputs.REQUIRED),  # type: ignore
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_create_fargate_profile_throws_exception_when_fargate_profile_already_exists(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size)
        expected_exception: type[AWSError] = ResourceInUseException
        expected_msg: str = FARGATE_PROFILE_EXISTS_MSG

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.create_fargate_profile(
                clusterName=generated_test_data.cluster_name,
                fargateProfileName=generated_test_data.existing_fargate_profile_name,
                **dict(FargateProfileInputs.REQUIRED),  # type: ignore
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new Fargate profile was created.
        fargate_profile_count_after_test: int = len(
            eks_hook.list_fargate_profiles(clusterName=generated_test_data.cluster_name)
        )
        assert fargate_profile_count_after_test == initial_batch_size

    def test_create_fargate_profile_throws_exception_when_cluster_not_active(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size)
        non_existent_fargate_profile_name: str = NON_EXISTING_FARGATE_PROFILE_NAME
        expected_exception: type[AWSError] = InvalidRequestException
        expected_msg: str = CLUSTER_NOT_READY_MSG.format(
            clusterName=generated_test_data.cluster_name,
        )

        with mock.patch("moto.eks.models.Cluster.isActive", return_value=False):
            with pytest.raises(ClientError) as raised_exception:
                eks_hook.create_fargate_profile(
                    clusterName=generated_test_data.cluster_name,
                    fargateProfileName=non_existent_fargate_profile_name,
                    **dict(FargateProfileInputs.REQUIRED),  # type: ignore
                )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new Fargate profile was created.
        fargate_profile_count_after_test: int = len(
            eks_hook.list_fargate_profiles(clusterName=generated_test_data.cluster_name)
        )
        assert fargate_profile_count_after_test == initial_batch_size

    def test_create_fargate_profile_generates_valid_profile_arn(self, fargate_profile_builder) -> None:
        _, generated_test_data = fargate_profile_builder()
        expected_arn_values: list = [
            PARTITION,
            REGION,
            DEFAULT_ACCOUNT_ID,
            generated_test_data.cluster_name,
            generated_test_data.fargate_profile_names,
            None,
        ]

        assert_all_arn_values_are_valid(
            expected_arn_values=expected_arn_values,
            pattern=RegExTemplates.FARGATE_PROFILE_ARN,
            arn_under_test=generated_test_data.fargate_describe_output[FargateProfileAttributes.ARN],
        )

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_create_fargate_profile_generates_valid_created_timestamp(self, fargate_profile_builder) -> None:
        _, generated_test_data = fargate_profile_builder()

        result_time: datetime = generated_test_data.fargate_describe_output[
            FargateProfileAttributes.CREATED_AT
        ]

        assert iso_date(result_time) == FROZEN_TIME

    def test_create_fargate_profile_saves_provided_parameters(self, fargate_profile_builder) -> None:
        _, generated_test_data = fargate_profile_builder(minimal=False)

        for key, expected_value in generated_test_data.attributes_to_test:
            assert generated_test_data.fargate_describe_output[key] == expected_value

    def test_describe_fargate_profile_throws_exception_when_cluster_not_found(
        self, fargate_profile_builder
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder()
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.describe_fargate_profile(
                clusterName=generated_test_data.nonexistent_cluster_name,
                fargateProfileName=generated_test_data.existing_fargate_profile_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_describe_fargate_profile_throws_exception_when_profile_not_found(
        self, fargate_profile_builder
    ) -> None:
        client, generated_test_data = fargate_profile_builder()
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = FARGATE_PROFILE_NOT_FOUND_MSG.format(
            fargateProfileName=generated_test_data.nonexistent_fargate_profile_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            client.describe_fargate_profile(
                clusterName=generated_test_data.cluster_name,
                fargateProfileName=generated_test_data.nonexistent_fargate_profile_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_delete_fargate_profile_removes_deleted_fargate_profile(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(initial_batch_size)

        eks_hook.delete_fargate_profile(
            clusterName=generated_test_data.cluster_name,
            fargateProfileName=generated_test_data.existing_fargate_profile_name,
        )
        result_fargate_profile_list: list = eks_hook.list_fargate_profiles(
            clusterName=generated_test_data.cluster_name
        )

        assert len(result_fargate_profile_list) == (initial_batch_size - 1)
        assert generated_test_data.existing_fargate_profile_name not in result_fargate_profile_list

    def test_delete_fargate_profile_returns_deleted_fargate_profile(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size, minimal=False)

        result: dict = eks_hook.delete_fargate_profile(
            clusterName=generated_test_data.cluster_name,
            fargateProfileName=generated_test_data.existing_fargate_profile_name,
        )[ResponseAttributes.FARGATE_PROFILE]

        for key, expected_value in generated_test_data.attributes_to_test:
            assert result[key] == expected_value

    def test_delete_fargate_profile_throws_exception_when_cluster_not_found(
        self, fargate_profile_builder
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder()
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = CLUSTER_NOT_FOUND_MSG.format(
            clusterName=generated_test_data.nonexistent_cluster_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_fargate_profile(
                clusterName=generated_test_data.nonexistent_cluster_name,
                fargateProfileName=generated_test_data.existing_fargate_profile_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )

    def test_delete_fargate_profile_throws_exception_when_fargate_profile_not_found(
        self, fargate_profile_builder, initial_batch_size: int = BatchCountSize.SMALL
    ) -> None:
        eks_hook, generated_test_data = fargate_profile_builder(count=initial_batch_size)
        expected_exception: type[AWSError] = ResourceNotFoundException
        expected_msg: str = FARGATE_PROFILE_NOT_FOUND_MSG.format(
            fargateProfileName=generated_test_data.nonexistent_fargate_profile_name,
        )

        with pytest.raises(ClientError) as raised_exception:
            eks_hook.delete_fargate_profile(
                clusterName=generated_test_data.cluster_name,
                fargateProfileName=generated_test_data.nonexistent_fargate_profile_name,
            )

        assert_client_error_exception_thrown(
            expected_exception=expected_exception,
            expected_msg=expected_msg,
            raised_exception=raised_exception,
        )
        # Verify no new Fargate profile was created.
        fargate_profile_count_after_test: int = len(
            eks_hook.list_fargate_profiles(clusterName=generated_test_data.cluster_name)
        )
        assert fargate_profile_count_after_test == initial_batch_size

    # The following Selector test cases have all been verified against the AWS API using cURL.
    selector_formatting_test_cases = [
        # Format is ([Selector(s), expected_message, expected_result])
        # Happy Paths
        # Selector with a Namespace and no Labels
        (
            [{FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE}],
            None,
            PossibleTestResults.SUCCESS,
        ),
        # Selector with a Namespace and an empty collection of Labels
        (
            [
                {
                    FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE,
                    FargateProfileAttributes.LABELS: generate_dict("label", 0),
                }
            ],
            None,
            PossibleTestResults.SUCCESS,
        ),
        # Selector with a Namespace and one valid Label
        (
            [
                {
                    FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE,
                    FargateProfileAttributes.LABELS: generate_dict("label", 1),
                }
            ],
            None,
            PossibleTestResults.SUCCESS,
        ),
        # Selector with a Namespace and the maximum number of Labels
        (
            [
                {
                    FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE,
                    FargateProfileAttributes.LABELS: generate_dict("label", MAX_FARGATE_LABELS),
                }
            ],
            None,
            PossibleTestResults.SUCCESS,
        ),
        # Two valid Selectors
        (
            [
                {FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE},
                {FargateProfileAttributes.NAMESPACE: f"{DEFAULT_NAMESPACE}_2"},
            ],
            None,
            PossibleTestResults.SUCCESS,
        ),
        # Unhappy Cases
        # No Selectors provided
        ([], FARGATE_PROFILE_NEEDS_SELECTOR_MSG, PossibleTestResults.FAILURE),
        # Empty Selector / Selector without a Namespace or Labels
        ([{}], FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE, PossibleTestResults.FAILURE),
        # Selector with labels but no Namespace
        (
            [{FargateProfileAttributes.LABELS: generate_dict("label", 1)}],
            FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE,
            PossibleTestResults.FAILURE,
        ),
        # Selector with Namespace but too many Labels
        (
            [
                {
                    FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE,
                    FargateProfileAttributes.LABELS: generate_dict("label", MAX_FARGATE_LABELS + 1),
                }
            ],
            FARGATE_PROFILE_TOO_MANY_LABELS,
            PossibleTestResults.FAILURE,
        ),
        # Valid Selector followed by Empty Selector
        (
            [{FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE}, {}],
            FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE,
            PossibleTestResults.FAILURE,
        ),
        # Empty Selector followed by Valid Selector
        (
            [{}, {FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE}],
            FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE,
            PossibleTestResults.FAILURE,
        ),
        # Empty Selector followed by Empty Selector
        ([{}, {}], FARGATE_PROFILE_SELECTOR_NEEDS_NAMESPACE, PossibleTestResults.FAILURE),
        # Valid Selector followed by Selector with Namespace but too many Labels
        (
            [
                {FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE},
                {
                    FargateProfileAttributes.NAMESPACE: DEFAULT_NAMESPACE,
                    FargateProfileAttributes.LABELS: generate_dict("label", MAX_FARGATE_LABELS + 1),
                },
            ],
            FARGATE_PROFILE_TOO_MANY_LABELS,
            PossibleTestResults.FAILURE,
        ),
    ]

    @pytest.mark.parametrize(
        "selectors, expected_message, expected_result",
        selector_formatting_test_cases,
    )
    @mock_eks
    def test_create_fargate_selectors(self, cluster_builder, selectors, expected_message, expected_result):
        client, generated_test_data = cluster_builder()
        cluster_name: str = generated_test_data.existing_cluster_name
        fargate_profile_name: str = NON_EXISTING_FARGATE_PROFILE_NAME
        expected_exception: type[AWSError] = InvalidParameterException

        test_inputs = dict(
            deepcopy(
                # Required Constants
                [POD_EXECUTION_ROLE_ARN]
                # Required Variables
                + [
                    (ClusterAttributes.CLUSTER_NAME, cluster_name),
                    (FargateProfileAttributes.FARGATE_PROFILE_NAME, fargate_profile_name),
                ]
                # Test Case Values
                + [(FargateProfileAttributes.SELECTORS, selectors)]
            )
        )

        if expected_result == PossibleTestResults.SUCCESS:
            result: list = client.create_fargate_profile(**test_inputs)[ResponseAttributes.FARGATE_PROFILE]
            for key, expected_value in test_inputs.items():
                assert result[key] == expected_value
        else:
            with pytest.raises(ClientError) as raised_exception:
                client.create_fargate_profile(**test_inputs)

            assert_client_error_exception_thrown(
                expected_exception=expected_exception,
                expected_msg=expected_message,
                raised_exception=raised_exception,
            )


class TestEksHook:
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    @pytest.mark.parametrize(
        "aws_conn_id, region_name, expected_args",
        [
            [
                "test-id",
                "test-region",
                [
                    "-m",
                    "airflow.providers.amazon.aws.utils.eks_get_token",
                    "--region-name",
                    "test-region",
                    "--aws-conn-id",
                    "test-id",
                    "--cluster-name",
                    "test-cluster",
                ],
            ],
            [
                None,
                "test-region",
                [
                    "-m",
                    "airflow.providers.amazon.aws.utils.eks_get_token",
                    "--region-name",
                    "test-region",
                    "--cluster-name",
                    "test-cluster",
                ],
            ],
            [
                None,
                None,
                ["-m", "airflow.providers.amazon.aws.utils.eks_get_token", "--cluster-name", "test-cluster"],
            ],
        ],
    )
    def test_generate_config_file(self, mock_conn, aws_conn_id, region_name, expected_args):
        mock_conn.describe_cluster.return_value = {
            "cluster": {"certificateAuthority": {"data": "test-cert"}, "endpoint": "test-endpoint"}
        }
        hook = EksHook(aws_conn_id=aws_conn_id, region_name=region_name)
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        hook.get_connection = lambda _: None
        with hook.generate_config_file(
            eks_cluster_name="test-cluster", pod_namespace="k8s-namespace"
        ) as config_file:
            config = yaml.safe_load(Path(config_file).read_text())
            assert config == {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "cluster": {"server": "test-endpoint", "certificate-authority-data": "test-cert"},
                        "name": "test-cluster",
                    }
                ],
                "contexts": [
                    {
                        "context": {"cluster": "test-cluster", "namespace": "k8s-namespace", "user": "aws"},
                        "name": "aws",
                    }
                ],
                "current-context": "aws",
                "preferences": {},
                "users": [
                    {
                        "name": "aws",
                        "user": {
                            "exec": {
                                "apiVersion": "client.authentication.k8s.io/v1alpha1",
                                "args": expected_args,
                                "command": sys.executable,
                                "env": [{"name": "AIRFLOW__LOGGING__LOGGING_LEVEL", "value": "FATAL"}],
                                "interactiveMode": "Never",
                            }
                        },
                    }
                ],
            }

    @mock.patch("airflow.providers.amazon.aws.hooks.eks.RequestSigner")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_session")
    def test_fetch_access_token_for_cluster(self, mock_get_session, mock_conn, mock_signer):
        mock_signer.return_value.generate_presigned_url.return_value = "http://example.com"
        mock_get_session.return_value.region_name = "us-east-1"
        hook = EksHook()
        token = hook.fetch_access_token_for_cluster(eks_cluster_name="test-cluster")
        mock_signer.assert_called_once_with(
            service_id=mock_conn.meta.service_model.service_id,
            region_name="us-east-1",
            signing_name="sts",
            signature_version="v4",
            credentials=mock_get_session.return_value.get_credentials.return_value,
            event_emitter=mock_get_session.return_value.events,
        )
        mock_signer.return_value.generate_presigned_url.assert_called_once_with(
            request_dict={
                "method": "GET",
                "url": "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                "body": {},
                "headers": {"x-k8s-aws-id": "test-cluster"},
                "context": {},
            },
            region_name="us-east-1",
            expires_in=60,
            operation_name="",
        )
        assert token == "k8s-aws-v1.aHR0cDovL2V4YW1wbGUuY29t"


# Helper methods for repeated assert combinations.
def assert_all_arn_values_are_valid(expected_arn_values, pattern, arn_under_test) -> None:
    """
    Applies regex `pattern` to `arn_under_test` and asserts
    that each group matches the provided expected value.
    A list entry of None in the 'expected_arn_values' will
    assert that the value exists but not match a specific value.
    """
    findall: list = pattern.findall(arn_under_test)[0]
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
    expected_exception: type[AWSError], expected_msg: str, raised_exception: ExceptionInfo
) -> None:
    """
    Asserts that the raised exception is of the expected type
    and the resulting message matches the expected format.
    """
    response = raised_exception.value.response[ErrorAttributes.ERROR]
    assert response[ErrorAttributes.CODE] == expected_exception.TYPE
    assert response[ErrorAttributes.MESSAGE] == expected_msg


def assert_result_matches_expected_list(
    result: list, expected_result: list, expected_len: int | None = None
) -> None:
    assert result == expected_result
    assert len(result) == expected_len or len(expected_result)


def assert_is_valid_uri(value: str) -> None:
    result: ParseResult = urlsplit(value)

    assert all([result.scheme, result.netloc, result.path])
    assert REGION in value
