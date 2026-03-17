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

"""Internal execution client for SageMaker Unified Studio."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from boto3 import Session
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.utils.sagemaker_unified_studio import (
    ErrorType,
    InternalServerError,
    ResourceNotFoundError,
    ValidationError,
    convert_timestamp_to_epoch_millis,
    find_default_tooling_environment,
    get_domain_id,
    get_domain_region,
    get_project_id,
    is_git_project,
    map_training_job_status_to_status,
    pack_full_path_for_input_file,
    pack_s3_path_for_input_file,
    pack_s3_path_for_output_file,
    unpack_path_file,
    validate_execution_name,
    validate_image_version,
    validate_input_config,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

# Default instance types for remote execution
DEFAULT_INSTANCE_TYPE = "ml.m6i.xlarge"
FALLBACK_INSTANCE_TYPE = "ml.m5.xlarge"
DEFAULT_IMAGE_VERSION = "latest"


class SageMakerUnifiedStudioExecutionClient:
    """
    Internal client for executing notebooks in SageMaker Unified Studio.

    This class handles the low-level interactions with AWS services (DataZone, SageMaker,
    EC2, SSM) to start and monitor notebook executions.
    """

    def __init__(
        self,
        domain_id: str | None = None,
        project_id: str | None = None,
        domain_region: str | None = None,
    ):
        """
        Initialize the execution client.

        :param domain_id: The DataZone domain ID
        :param project_id: The DataZone project ID
        :param domain_region: The DataZone domain region
        """
        self.domain_id = domain_id
        self.project_id = project_id
        self.domain_region = domain_region

        # Lazily initialized
        self._session: Session | None = None
        self._datazone_client: BaseClient | None = None
        self._sagemaker_client: BaseClient | None = None
        self._ec2_client: BaseClient | None = None
        self._ssm_client: BaseClient | None = None
        self._default_tooling_environment: dict | None = None
        self._sagemaker_environment: dict | None = None
        self._project_s3_path: str | None = None
        self._security_group: str | None = None
        self._subnets: list[str] | None = None
        self._user_role_arn: str | None = None
        self._kms_key_identifier: str | None = None
        self._is_setup: bool = False

    @property
    def session(self) -> Session:
        """Get or create a boto3 session."""
        if self._session is None:
            self._session = Session()
        return self._session

    def _get_datazone_client(self, region: str) -> BaseClient:
        """Get or create a DataZone client."""
        if self._datazone_client is None:
            self._datazone_client = self.session.client(
                service_name="datazone",
                region_name=region,
            )
        return self._datazone_client

    def setup(self) -> None:
        """Set up all required AWS clients and configurations for execution."""
        if self._is_setup:
            return

        # Resolve domain_id from environment if not provided
        if not self.domain_id:
            self.domain_id = get_domain_id()

        # Resolve domain_region from environment if not provided
        if not self.domain_region:
            self.domain_region = get_domain_region() or "us-east-1"

        # Get DataZone client
        datazone_client = self._get_datazone_client(self.domain_region)

        # Resolve project_id from environment if not provided
        if not self.project_id:
            self.project_id = get_project_id(datazone_client, self.domain_id)

        if not self.domain_id or not self.project_id:
            raise InternalServerError("Domain identifier and project identifier are required")

        # Get default tooling environment
        if self._default_tooling_environment is None:
            self._default_tooling_environment = self._get_project_default_environment(
                datazone_client, self.domain_id, self.project_id
            )

        # Get SageMaker environment
        if self._sagemaker_environment is None:
            self._sagemaker_environment = self._get_project_sagemaker_environment(
                datazone_client, self.domain_id, self.project_id
            )

        self._validate_environment(self._default_tooling_environment)

        # Set up AWS clients
        sagemaker_region = self._sagemaker_environment["awsAccountRegion"]

        if self._sagemaker_client is None:
            self._sagemaker_client = self.session.client(
                service_name="sagemaker",
                region_name=sagemaker_region,
            )

        if self._ec2_client is None:
            self._ec2_client = self.session.client(
                service_name="ec2",
                region_name=sagemaker_region,
            )

        if self._ssm_client is None:
            self._ssm_client = self.session.client(
                service_name="ssm",
                region_name=sagemaker_region,
            )

        # Set up stack resources
        self._setup_stack_resources()

        # Set up project S3 path
        if self._project_s3_path is None:
            self._project_s3_path = self._get_project_s3_path_from_environment()

        self._is_setup = True

    def _get_project_default_environment(
        self, datazone_client: BaseClient, domain_id: str, project_id: str
    ) -> dict:
        """Retrieve the default tooling environment for the project."""
        try:
            # Try Tooling blueprint first
            tooling_blueprints = datazone_client.list_environment_blueprints(
                domainIdentifier=domain_id,
                managed=True,
                name="Tooling",
                provider="Amazon SageMaker",
            ).get("items", [])

            if tooling_blueprints:
                tooling_env_blueprint = next(
                    (bp for bp in tooling_blueprints if bp["name"] == "Tooling"), None
                )
                if tooling_env_blueprint:
                    tooling_environments = datazone_client.list_environments(
                        domainIdentifier=domain_id,
                        projectIdentifier=project_id,
                        environmentBlueprintIdentifier=tooling_env_blueprint.get("id"),
                    ).get("items", [])

                    default_environment = find_default_tooling_environment(tooling_environments)
                    if default_environment:
                        return default_environment

            # Try ToolingLite blueprint as fallback
            tooling_lite_blueprints = datazone_client.list_environment_blueprints(
                domainIdentifier=domain_id,
                managed=True,
                name="ToolingLite",
                provider="Amazon SageMaker",
            ).get("items", [])

            if not tooling_lite_blueprints:
                raise ValueError("ToolingLite environment blueprint not found")

            tooling_lite_env_blueprint = tooling_lite_blueprints[0]
            tooling_lite_environments = datazone_client.list_environments(
                domainIdentifier=domain_id,
                projectIdentifier=project_id,
                environmentBlueprintIdentifier=tooling_lite_env_blueprint.get("id"),
            ).get("items", [])

            default_environment = find_default_tooling_environment(tooling_lite_environments)
            if not default_environment:
                raise ValueError("ToolingLite environment not found")

            return default_environment

        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise ValueError(f"Invalid input parameters: {e}") from e
            raise

    def _get_project_sagemaker_environment(
        self, datazone_client: BaseClient, domain_id: str, project_id: str
    ) -> dict:
        """Retrieve the SageMaker environment for the project."""
        try:
            project_environments = datazone_client.list_environments(
                domainIdentifier=domain_id,
                projectIdentifier=project_id,
                name="Tooling",
            ).get("items", [])
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise ValueError(f"Invalid input parameters: {e}") from e
            raise

        if project_environments:
            return project_environments[0]
        return self._get_project_default_environment(datazone_client, domain_id, project_id)

    def _validate_environment(self, environment: dict | None) -> None:
        """Validate that the environment has all required fields."""
        if environment is None:
            raise RuntimeError("Default environment not found")
        required_fields = ["awsAccountRegion", "awsAccountId", "id", "provisionedResources"]
        for field in required_fields:
            if field not in environment:
                raise RuntimeError(f"Default environment {field} not found")

    def _setup_stack_resources(self) -> None:
        """Extract stack resources from the default tooling environment."""
        provisioned_resources = self._default_tooling_environment.get("provisionedResources", [])

        for resource in provisioned_resources:
            name = resource.get("name")
            value = resource.get("value")
            if name == "securityGroup":
                self._security_group = value
            elif name == "privateSubnets":
                self._subnets = value.split(",") if value else None
            elif name == "userRoleArn":
                self._user_role_arn = value
            elif name == "kmsKeyArn":
                self._kms_key_identifier = value

    def _get_project_s3_path_from_environment(self) -> str:
        """Get the S3 path for the project from provisioned resources."""
        provisioned_resources = self._default_tooling_environment.get("provisionedResources", [])
        for resource in provisioned_resources:
            if resource.get("name") == "s3BucketPath":
                return resource.get("value", "")
        raise RuntimeError("s3BucketPath not found in provisioned resources")

    def _get_available_instance_type_with_info(
        self, preferred_instance_type: str, use_fallback: bool = True
    ) -> dict[str, Any]:
        """Get available instance type and its GPU info."""

        def get_instance_info(instance_type: str) -> dict[str, Any] | None:
            try:
                ec2_type = instance_type
                if instance_type.startswith("ml."):
                    ec2_type = ".".join(instance_type.split(".")[1:])

                response = self._ec2_client.describe_instance_types(InstanceTypes=[ec2_type])
                if not response.get("InstanceTypes"):
                    return None

                instance_info = response["InstanceTypes"][0]
                has_gpu = bool(instance_info.get("GpuInfo", {}).get("Gpus"))
                return {"instance_type": instance_type, "has_gpu": has_gpu}
            except ClientError:
                return None

        info = get_instance_info(preferred_instance_type)
        if info:
            return info

        if use_fallback:
            info = get_instance_info(FALLBACK_INSTANCE_TYPE)
            if info:
                return info
            raise ValidationError(
                f"Neither '{preferred_instance_type}' nor '{FALLBACK_INSTANCE_TYPE}' "
                f"is available in the current region."
            )
        raise ValidationError(
            f"Instance type '{preferred_instance_type}' is not available in the current region."
        )

    def start_execution(
        self,
        execution_name: str,
        input_config: dict[str, Any],
        output_config: dict[str, Any],
        compute: dict[str, Any] | None = None,
        termination_condition: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> dict:
        """
        Start a notebook execution.

        :param execution_name: Name for the execution
        :param input_config: Input configuration with notebook path and parameters
        :param output_config: Output configuration with formats
        :param compute: Compute configuration (instance type, image details)
        :param termination_condition: Termination conditions
        :param tags: Tags for the execution
        :return: Execution details including execution_id
        """
        self.setup()

        # Convert underscores to hyphens in execution_name
        execution_name = execution_name.replace("_", "-")

        # Validate inputs
        validate_execution_name(execution_name)
        validate_input_config(input_config)

        if self._user_role_arn is None:
            raise RuntimeError("Default stack user_role_arn not found")

        # Determine instance type and GPU info
        compute = compute or {}
        if not compute:
            instance_type_info = self._get_available_instance_type_with_info(DEFAULT_INSTANCE_TYPE)
            compute = {"instance_type": instance_type_info["instance_type"]}
            has_gpu = instance_type_info["has_gpu"]
        else:
            instance_type_info = self._get_available_instance_type_with_info(
                compute["instance_type"], use_fallback=False
            )
            has_gpu = instance_type_info["has_gpu"]

        # Determine ECR URI for the image
        ecr_uri = self._resolve_ecr_uri(compute, has_gpu)

        # Prepare S3 paths
        provisioned_resources = self._default_tooling_environment.get("provisionedResources", [])
        is_git = is_git_project(provisioned_resources)

        local_input_file_path = input_config.get("notebook_config", {}).get("input_path", "")
        input_s3_location = pack_s3_path_for_input_file(self._project_s3_path, local_input_file_path, is_git)
        output_s3_location = pack_s3_path_for_output_file(
            self._project_s3_path, local_input_file_path, is_git
        )
        local_full_input_file_path = pack_full_path_for_input_file(local_input_file_path)

        input_info = unpack_path_file(input_s3_location)
        output_info = unpack_path_file(output_s3_location)

        # Prepare tags
        execution_tags = [
            {"Key": "sagemaker-notebook-execution", "Value": "TRUE"},
            {"Key": "sagemaker-notebook-name", "Value": input_info["file"]},
        ]
        if tags:
            execution_tags.extend({"Key": k, "Value": v} for k, v in tags.items())

        # Prepare output formats
        output_formats = output_config.get("notebook_config", {}).get("output_formats", ["NOTEBOOK"])
        output_formats_lowercase = [x.lower() for x in output_formats]
        if "notebook" in output_formats_lowercase:
            output_formats_lowercase.remove("notebook")

        # Build training job parameters
        training_job_params = self._build_training_job_params(
            execution_name=execution_name,
            ecr_uri=ecr_uri,
            input_info=input_info,
            output_info=output_info,
            input_config=input_config,
            compute=compute,
            termination_condition=termination_condition or {},
            execution_tags=execution_tags,
            local_full_input_file_path=local_full_input_file_path,
            output_formats_lowercase=output_formats_lowercase,
        )

        try:
            response = self._sagemaker_client.create_training_job(**training_job_params)
            training_job_arn = response.get("TrainingJobArn", "")
            split_arn = training_job_arn.split(":training-job/")

            if len(split_arn) != 2:
                raise RuntimeError("Remote executionId not available")

            return {
                "execution_id": split_arn[1],
                "execution_name": execution_name,
            }

        except ClientError as e:
            raise RuntimeError(f"Error starting remote execution: {e}") from e

    def _resolve_ecr_uri(self, compute: dict, has_gpu: bool) -> str:
        """Resolve the ECR URI for the container image."""
        image_details = compute.get("image_details", {})
        if "ecr_uri" in image_details:
            return image_details["ecr_uri"]

        image_name = image_details.get("image_name", "sagemaker-distribution-prod")
        image_version = image_details.get("image_version", DEFAULT_IMAGE_VERSION)
        image_variant = "gpu" if has_gpu else "cpu"

        if image_name != "sagemaker-distribution-prod":
            # BYOI case
            response = self._sagemaker_client.describe_image_version(
                ImageName=image_name,
                Version=int(image_details.get("image_version")),
            )
            return response.get("BaseImage")
        # Using SMD image
        if not validate_image_version(image_version):
            raise ValidationError(f"Invalid image version: {image_version}")

        account_id = self._ssm_client.get_parameter(
            Name="/aws/service/sagemaker-distribution/ecr-account-id"
        )["Parameter"]["Value"]

        if not account_id:
            raise InternalServerError("Account ID not found in SSM parameter store")

        region = self._default_tooling_environment["awsAccountRegion"]
        return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{image_name}:{image_version}-{image_variant}"

    def _build_training_job_params(
        self,
        execution_name: str,
        ecr_uri: str,
        input_info: dict,
        output_info: dict,
        input_config: dict,
        compute: dict,
        termination_condition: dict,
        execution_tags: list,
        local_full_input_file_path: str,
        output_formats_lowercase: list,
    ) -> dict:
        """Build the parameters for create_training_job."""
        # Prepare KMS keys
        kms_key_id = {}
        volume_kms_key_id = {}
        if self._kms_key_identifier:
            kms_key_id = {"KmsKeyId": self._kms_key_identifier}
            volume_kms_key_id = {"VolumeKmsKeyId": self._kms_key_identifier}

        # Environment variables for the training job
        environment_variables = {
            "SM_EFS_MOUNT_GID": "100",
            "SM_EFS_MOUNT_PATH": "/home/sagemaker-user",
            "SM_EFS_MOUNT_UID": "1000",
            "SM_ENV_NAME": "sagemaker-workflows-default-env",
            "SM_EXECUTION_INPUT_PATH": "/opt/ml/input/data/sagemaker_workflows",
            "SM_EXECUTION_SYSTEM_PATH": "/opt/ml/input/data/sagemaker_workflows_system",
            "SM_INPUT_NOTEBOOK_NAME": input_info["file"],
            "SM_JOB_DEF_VERSION": "1.0",
            "SM_KERNEL_NAME": "python3",
            "SM_OUTPUT_NOTEBOOK_NAME": output_info["file"],
            "SM_SKIP_EFS_SIMULATION": "true",
            "DataZoneDomainId": self.domain_id,
            "DataZoneProjectId": self.project_id,
            "DataZoneDomainRegion": self.domain_region or "",
            "ProjectS3Path": self._project_s3_path,
            "InputNotebookPath": local_full_input_file_path,
            "SM_OUTPUT_FORMATS": ",".join(output_formats_lowercase) if output_formats_lowercase else "",
            "SM_INIT_SCRIPT": "../../../../../etc/sagemaker-ui/workflows/sm_init_script.sh",
        }

        training_job_params = {
            "TrainingJobName": f"{execution_name}-{uuid.uuid4()}",
            "AlgorithmSpecification": {
                "TrainingImage": ecr_uri,
                "TrainingInputMode": "File",
                "EnableSageMakerMetricsTimeSeries": False,
                "ContainerEntrypoint": ["amazon_sagemaker_scheduler"],
                "MetricDefinitions": [
                    {"Name": "cells:complete", "Regex": r".*Executing:.*?\|.*?\|\s+(\d+)\/\d+\s+\["},
                    {"Name": "cells:total", "Regex": r".*Executing:.*?\|.*?\|\s+\d+\/(\d+)\s+\["},
                ],
            },
            "RoleArn": self._user_role_arn,
            "OutputDataConfig": {"S3OutputPath": output_info["path"], **kms_key_id},
            "ResourceConfig": {
                "InstanceCount": 1,
                "InstanceType": compute.get("instance_type", ""),
                "VolumeSizeInGB": compute.get("volume_size_in_gb", 30),
                **volume_kms_key_id,
            },
            "InputDataConfig": [
                {
                    "ChannelName": "sagemaker_workflows",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": input_info["path"],
                            "S3DataDistributionType": "FullyReplicated",
                        }
                    },
                    "ContentType": "text/csv",
                    "CompressionType": "None",
                }
            ],
            "HyperParameters": input_config.get("notebook_config", {}).get("input_parameters", {}) or {},
            "StoppingCondition": {
                "MaxRuntimeInSeconds": termination_condition.get("max_runtime_in_seconds", 86400)
            },
            "EnableManagedSpotTraining": False,
            "EnableNetworkIsolation": False,
            "EnableInterContainerTrafficEncryption": True,
            "Environment": environment_variables,
            "RetryStrategy": {"MaximumRetryAttempts": 1},
            "Tags": execution_tags,
        }

        # Add VPC config if available
        if self._security_group and self._subnets:
            training_job_params["VpcConfig"] = {
                "SecurityGroupIds": [self._security_group],
                "Subnets": self._subnets,
            }

        return training_job_params

    def get_execution(self, execution_id: str) -> dict:
        """
        Get details of a specific execution.

        :param execution_id: The unique identifier of the execution
        :return: Execution details including status, times, and error info
        :raises ResourceNotFoundError: If execution not found
        """
        self.setup()

        training_job_arn = (
            f"arn:aws:sagemaker:{self._default_tooling_environment['awsAccountRegion']}:"
            f"{self._default_tooling_environment['awsAccountId']}:training-job/{execution_id}"
        )

        try:
            response = self._sagemaker_client.describe_training_job(TrainingJobName=execution_id)

            if "CreationTime" not in response or "TrainingJobStatus" not in response:
                raise RuntimeError(f"Error getting execution with id: {execution_id}")

            # Get tags
            execution_tags = []
            list_tags_paginator = self._sagemaker_client.get_paginator("list_tags")
            for page in list_tags_paginator.paginate(ResourceArn=training_job_arn):
                for tag in page.get("Tags", []):
                    if "Key" in tag and "Value" in tag:
                        execution_tags.append({"Key": tag["Key"], "Value": tag["Value"]})

            result = {
                "execution_id": execution_id,
                "status": map_training_job_status_to_status(response["TrainingJobStatus"]).value,
                "start_time": (
                    convert_timestamp_to_epoch_millis(response["TrainingStartTime"])
                    if "TrainingStartTime" in response
                    else None
                ),
                "end_time": (
                    convert_timestamp_to_epoch_millis(response["TrainingEndTime"])
                    if "TrainingEndTime" in response
                    else None
                ),
                "s3_path": response.get("ModelArtifacts", {}).get("S3ModelArtifacts"),
                "tags": execution_tags,
            }

            if "FailureReason" in response:
                result["error_details"] = {
                    "error_message": response["FailureReason"],
                    "error_type": ErrorType.SERVER_ERROR,
                }

            return {k: v for k, v in result.items() if v is not None}

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ValidationException" and "Requested resource not found" in error_message:
                raise ResourceNotFoundError(f"Execution with id {execution_id} not found.") from e
            raise InternalServerError(f"Error getting execution: {e}") from e
