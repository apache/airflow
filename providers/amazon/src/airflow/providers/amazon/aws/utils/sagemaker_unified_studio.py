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

"""Utilities, models, and exceptions for the Amazon SageMaker Unified Studio integration."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from botocore.client import BaseClient

log = logging.getLogger(__name__)

# Environment variable keys
WORKFLOWS_ENV_KEY = "WORKFLOWS_ENV"
AIRFLOW_PREFIX = "AIRFLOW__WORKFLOWS__"
SAGEMAKER_METADATA_JSON_PATH = "/opt/ml/metadata/resource-metadata.json"

# Mapping from SageMaker Space environment variables to MWAA environment variables
SPACE_ENV_VARIABLES_TO_MWAA_ENV_VARIABLES = {
    "DataZoneDomainId": f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_ID",
    "DataZoneProjectId": f"{AIRFLOW_PREFIX}DATAZONE_PROJECT_ID",
    "ProjectS3Path": f"{AIRFLOW_PREFIX}PROJECT_S3_PATH",
    "DataZoneDomainRegion": f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_REGION",
}


# ============================================================================
# Enums
# ============================================================================


class Status(Enum):
    """Execution status values."""

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class SortBy(Enum):
    """Sort by options for listing executions."""

    NAME = "NAME"
    STATUS = "STATUS"
    START_TIME = "START_TIME"
    END_TIME = "END_TIME"


class SortOrder(Enum):
    """Sort order options."""

    ASCENDING = "ASCENDING"
    DESCENDING = "DESCENDING"


class ErrorType(Enum):
    """Error type classification."""

    CLIENT_ERROR = "CLIENT_ERROR"
    SERVER_ERROR = "SERVER_ERROR"


class S3PathForProject(Enum):
    """S3 path prefixes for project resources."""

    DATALAKE_CONSUMER_GLUE_DB = "/data/catalogs/"
    DATALAKE_ATHENA_WORKGROUP = "/sys/athena/"
    WORKFLOW_TEMP_STORAGE = "/workflows/tmp/"
    WORKFLOW_OUTPUT_LOCATION = "/workflows/output/"
    EMR_EC2_LOG_DEST = "/sys/emr"
    EMR_EC2_CERTS = "/sys/emr/certs"
    EMR_EC2_LOG_BOOTSTRAP = "/sys/emr/boot-strap"
    WORKFLOW_PROJECT_FILES_LOCATION = "/workflows/project-files/"
    WORKFLOW_CONFIG_FILES_LOCATION = "/workflows/config-files/"


# ============================================================================
# Exceptions
# ============================================================================


class SageMakerUnifiedStudioError(Exception):
    """Base exception for SageMaker Unified Studio errors."""


class ValidationError(SageMakerUnifiedStudioError):
    """Raised when input validation fails."""


class ResourceNotFoundError(SageMakerUnifiedStudioError):
    """Raised when a requested resource cannot be found."""


class InternalServerError(SageMakerUnifiedStudioError):
    """Raised when an internal server error occurs."""


class ConflictError(SageMakerUnifiedStudioError):
    """Raised when updating or deleting a resource would cause an inconsistent state."""

    def __init__(self, message: str = "An error occurred"):
        self.message = message
        super().__init__(self.message)


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class ClientConfig:
    """Configuration for the SageMaker Unified Studio client."""

    profile_name: str | None = None
    region: str | None = None
    overrides: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Ensure all expected override keys exist
        default_overrides = {
            "datazone": {},
            "glue": {},
            "sagemaker": {},
            "execution": {},
            "secretsmanager": {},
        }
        for key, default_value in default_overrides.items():
            if key not in self.overrides:
                self.overrides[key] = default_value


@dataclass
class ExecutionConfig:
    """Configuration for execution."""

    local: bool = False
    domain_identifier: str | None = None
    project_identifier: str | None = None
    datazone_domain_region: str | None = None
    project_s3_path: str | None = None


# ============================================================================
# Utility Functions
# ============================================================================


def get_field_from_environment(field_name: str, default: str = "") -> str:
    """
    Fetch a field from either the resource-metadata.json file or environment variables.

    :param field_name: The name of the field to retrieve
    :param default: Default value if field is not found
    :return: The field value or default
    """
    # Try to get from SageMaker Space metadata JSON file
    if os.path.exists(SAGEMAKER_METADATA_JSON_PATH):
        try:
            return _get_field_from_sagemaker_space_metadata_json(field_name)
        except Exception as e:
            log.info("Error fetching %s from resource-metadata JSON file: %s", field_name, e)

    # Look for a SM space environment variable first
    if field_name in os.environ:
        return os.environ[field_name]

    # Then check if this is running in MWAA env
    mwaa_var = SPACE_ENV_VARIABLES_TO_MWAA_ENV_VARIABLES.get(field_name)
    if mwaa_var and mwaa_var in os.environ:
        return os.environ[mwaa_var]

    return default


def _get_field_from_sagemaker_space_metadata_json(field_name: str) -> str:
    """Read a field from the SageMaker Space metadata JSON file."""
    try:
        with open(SAGEMAKER_METADATA_JSON_PATH) as f:
            metadata = json.load(f)
            # Could be a top-level field or inside AdditionalMetadata
            return metadata.get(field_name) or metadata.get("AdditionalMetadata", {}).get(field_name)
    except json.JSONDecodeError as e:
        raise RuntimeError("JSON decoding error when parsing resource-metadata file") from e
    except Exception as e:
        raise RuntimeError("Error accessing SageMaker resource metadata") from e


def get_domain_region() -> str:
    """Get the DataZone domain region from environment."""
    return get_field_from_environment("DataZoneDomainRegion")


def get_domain_id() -> str:
    """Get the DataZone domain ID from environment."""
    return get_field_from_environment("DataZoneDomainId")


def get_project_id_from_env() -> str:
    """Get the DataZone project ID from environment."""
    return get_field_from_environment("DataZoneProjectId")


def get_project_id(
    datazone_client: BaseClient,
    domain_id: str | None = None,
    project_name: str | None = None,
) -> str:
    """
    Get the project ID either from environment or by looking up by name.

    :param datazone_client: The DataZone boto3 client
    :param domain_id: The domain ID (required if project_name is provided)
    :param project_name: The project name to look up
    :return: The project ID
    :raises ValueError: If project ID cannot be determined
    :raises RuntimeError: If project lookup fails
    """
    from botocore.exceptions import ClientError

    if project_name and domain_id:
        project_paginator = datazone_client.get_paginator("list_projects")
        try:
            for page in project_paginator.paginate(domainIdentifier=domain_id):
                for project in page.get("items", []):
                    if project.get("name") == project_name:
                        return project.get("id")
            raise RuntimeError(f"Project {project_name} not found")
        except ClientError as e:
            raise RuntimeError(f"Error getting project ID for project {project_name}: {e}") from e
    elif not project_name:
        project_id = get_project_id_from_env()
        if not project_id:
            raise ValueError("Project ID not found in environment. Please specify a project name.")
        return project_id
    elif project_name and not domain_id:
        raise ValueError("If specifying project name, a domain ID must also be specified.")
    else:
        raise RuntimeError("Error getting project ID")


def find_default_tooling_environment(environments: list[dict]) -> dict | None:
    """Find the default tooling environment from a list of environments."""
    valid_envs = [env for env in environments if env.get("deploymentOrder") is not None]
    if not valid_envs:
        return None
    return min(valid_envs, key=lambda env: env["deploymentOrder"])


# ============================================================================
# Execution Utilities
# ============================================================================


def map_sort_by(sort_by: SortBy) -> str:
    """Map SortBy enum to SageMaker search sort field."""
    mapping = {
        SortBy.NAME: "TrainingJobName",
        SortBy.STATUS: "TrainingJobStatus",
        SortBy.START_TIME: "TrainingStartTime",
        SortBy.END_TIME: "TrainingEndTime",
    }
    return mapping.get(sort_by, "TrainingStartTime")


def map_sort_order(sort_order: SortOrder) -> str:
    """Map SortOrder enum to SageMaker search sort order."""
    if sort_order == SortOrder.ASCENDING:
        return "Ascending"
    if sort_order == SortOrder.DESCENDING:
        return "Descending"
    raise RuntimeError(f"Invalid sort order: {sort_order}")


def map_training_job_status_to_status(training_job_status: str) -> Status:
    """Map SageMaker training job status to Status enum."""
    mapping = {
        "InProgress": Status.IN_PROGRESS,
        "Completed": Status.COMPLETED,
        "Failed": Status.FAILED,
        "Stopping": Status.STOPPING,
        "Stopped": Status.STOPPED,
    }
    if training_job_status not in mapping:
        raise RuntimeError(f"Invalid training job status: {training_job_status}")
    return mapping[training_job_status]


def map_status_to_training_job_status(status: Status) -> str:
    """Map Status enum to SageMaker training job status."""
    mapping = {
        Status.IN_PROGRESS: "InProgress",
        Status.COMPLETED: "Completed",
        Status.FAILED: "Failed",
        Status.STOPPING: "Stopping",
        Status.STOPPED: "Stopped",
    }
    if status not in mapping:
        raise RuntimeError(f"Invalid status: {status}")
    return mapping[status]


def unpack_path_file(path_file: str) -> dict[str, str]:
    """Split a path into directory and filename components."""
    split_path = path_file.split("/")
    if len(split_path) < 2:
        raise ValueError(f"Path and filename required: {path_file}")
    return {
        "path": "/".join(split_path[:-1]),
        "file": split_path[-1],
    }


def convert_timestamp_to_epoch_millis(timestamp: datetime) -> int:
    """Convert a datetime to epoch milliseconds."""
    return int(timestamp.timestamp() * 1000)


def create_sagemaker_search_expression_for_training(
    domain_identifier: str,
    project_identifier: str,
    start_time_after: int | None = None,
    name_contains: str | None = None,
    search_status: str | None = None,
    filter_by_tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Create a SageMaker search expression for training jobs."""
    filters: list[dict[str, Any]] = [
        {"Name": "Tags.sagemaker-notebook-execution", "Operator": "Equals", "Value": "TRUE"},
        {"Name": "Tags.AmazonDataZoneProject", "Operator": "Equals", "Value": project_identifier},
        {"Name": "Tags.AmazonDataZoneDomain", "Operator": "Equals", "Value": domain_identifier},
    ]

    if name_contains:
        filters.append({"Name": "TrainingJobName", "Operator": "Contains", "Value": name_contains})

    if start_time_after:
        filters.append(
            {
                "Name": "TrainingStartTime",
                "Operator": "GreaterThanOrEqualTo",
                "Value": datetime.fromtimestamp(start_time_after / 1000.0).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    if search_status:
        filters.append({"Name": "TrainingJobStatus", "Operator": "Equals", "Value": search_status})

    if filter_by_tags:
        for tag_key, tag_value in filter_by_tags.items():
            filters.append({"Name": f"Tags.{tag_key}", "Operator": "Equals", "Value": tag_value})

    return {"Filters": filters} if filters else {}


# ============================================================================
# Remote Execution Utilities
# ============================================================================

SAGEMAKER_USER_HOME = "/home/sagemaker-user"


def is_git_project(provisioned_resources: list[dict]) -> bool:
    """Check if the project uses Git storage based on provisioned resources."""
    for resource in provisioned_resources:
        if resource.get("name") in ["gitConnectionArn", "codeRepositoryName"]:
            return True
    return False


def pack_s3_path_for_input_file(project_s3_path: str, local_file_path: str, is_git: bool) -> str:
    """
    Generate the S3 path for the input file.

    :param project_s3_path: The project S3 path
    :param local_file_path: Path to the local input file (e.g., 'src/getting_started.ipynb')
    :param is_git: Whether the project uses Git storage
    :return: The S3 path for the input file
    """
    project_s3_path = project_s3_path.rstrip("/")

    if is_git:
        local_file_path = re.sub(r"^/?src/", "", local_file_path)
        return f"{project_s3_path}{S3PathForProject.WORKFLOW_PROJECT_FILES_LOCATION.value}{local_file_path}"
    # S3 storage
    project_s3_path = project_s3_path.replace("/dev", "/shared")
    local_file_path = re.sub(r"^/?shared/", "", local_file_path)
    return f"{project_s3_path}/{local_file_path}"


def pack_s3_path_for_output_file(project_s3_path: str, local_input_file_path: str, is_git: bool) -> str:
    """
    Generate the S3 path for the output file.

    :param project_s3_path: The project S3 path
    :param local_input_file_path: Path to the local input file
    :param is_git: Whether the project uses Git storage
    :return: The S3 path for the output file
    """
    project_s3_path = project_s3_path.rstrip("/")

    if is_git:
        local_input_file_path = re.sub(r"^/?src/", "", local_input_file_path)
    else:
        local_input_file_path = re.sub(r"^/?shared/", "", local_input_file_path)

    directory, file_name = os.path.split(local_input_file_path)
    output_file_path = os.path.join(directory, f"_{file_name}")

    return f"{project_s3_path}{S3PathForProject.WORKFLOW_OUTPUT_LOCATION.value}{output_file_path}"


def pack_full_path_for_input_file(local_input_file_path: str) -> str:
    """Get the full path for the input file in the SageMaker container."""
    cleaned_path = local_input_file_path.lstrip("/")
    return os.path.join(SAGEMAKER_USER_HOME, cleaned_path)


def validate_execution_name(execution_name: str) -> None:
    """Validate the execution name format."""
    if not re.match(r"^[a-zA-Z0-9]([-a-zA-Z0-9]){0,25}$", execution_name):
        raise ValidationError(
            f"Execution name {execution_name} does not match required pattern "
            "'^[a-zA-Z0-9]([-a-zA-Z0-9]){{0,25}}$'"
        )


def validate_input_config(input_config: dict) -> None:
    """Validate the input configuration for remote execution."""
    if not input_config:
        raise ValidationError("InputConfig is required for remote execution")
    if "notebook_config" not in input_config:
        raise ValidationError("notebookConfig in InputConfig is required for remote execution")
    if "input_path" not in input_config["notebook_config"]:
        raise ValidationError("'inputPath' in InputConfig notebookConfig is required for remote execution")

    input_parameters = input_config["notebook_config"].get("input_parameters")
    if input_parameters:
        if len(input_parameters) > 100:
            raise ValidationError(
                "inputParameters in InputConfig notebookConfig cannot have more than 100 entries"
            )
        for key, value in input_parameters.items():
            if len(key) > 256:
                raise ValidationError("The input parameters key length cannot exceed 256 characters.")
            if len(str(value)) > 2500:
                raise ValidationError("The input parameters value length cannot exceed 2500 characters.")


def validate_image_version(sem_ver: str) -> bool:
    """Validate the SageMaker Distribution image version."""
    from packaging.version import InvalidVersion, Version

    # Allow "latest" as a valid version
    if sem_ver == "latest":
        return True

    try:
        parsed_version = Version(sem_ver)

        # Valid for any 3.x version (major version 3)
        if parsed_version.major == 3:
            return True

        if parsed_version.major == 2 and (parsed_version.base_version == "2" or parsed_version.minor >= 10):
            return True
        return False
    except InvalidVersion:
        raise ValidationError(f"Invalid image version {sem_ver}")
