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
import json
import time
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Sequence

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.sagemaker import (
    LogState,
    SageMakerHook,
    secondary_training_status_message,
)
from airflow.providers.amazon.aws.triggers.sagemaker import (
    SageMakerPipelineTrigger,
    SageMakerTrigger,
)
from airflow.providers.amazon.aws.utils import trim_none_values, validate_execute_complete_event
from airflow.providers.amazon.aws.utils.sagemaker import ApprovalStatus
from airflow.providers.amazon.aws.utils.tags import format_tags
from airflow.utils.helpers import prune_dict
from airflow.utils.json import AirflowJsonEncoder

if TYPE_CHECKING:
    from airflow.providers.common.compat.openlineage.facet import Dataset
    from airflow.providers.openlineage.extractors.base import OperatorLineage
    from airflow.utils.context import Context

DEFAULT_CONN_ID: str = "aws_default"
CHECK_INTERVAL_SECOND: int = 30


def serialize(result: dict) -> dict:
    return json.loads(json.dumps(result, cls=AirflowJsonEncoder))


class SageMakerBaseOperator(BaseOperator):
    """
    This is the base operator for all SageMaker operators.

    :param config: The configuration necessary to start a training job (templated)
    """

    template_fields: Sequence[str] = ("config",)
    template_ext: Sequence[str] = ()
    template_fields_renderers: dict = {"config": "json"}
    ui_color: str = "#ededed"
    integer_fields: list[list[Any]] = []

    def __init__(self, *, config: dict, aws_conn_id: str | None = DEFAULT_CONN_ID, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.aws_conn_id = aws_conn_id

    def parse_integer(self, config: dict, field: list[str] | str) -> None:
        """Recursive method for parsing string fields holding integer values to integers."""
        if len(field) == 1:
            if isinstance(config, list):
                for sub_config in config:
                    self.parse_integer(sub_config, field)
                return
            head = field[0]
            if head in config:
                config[head] = int(config[head])
            return
        if isinstance(config, list):
            for sub_config in config:
                self.parse_integer(sub_config, field)
            return
        (head, tail) = (field[0], field[1:])
        if head in config:
            self.parse_integer(config[head], tail)
        return

    def parse_config_integers(self) -> None:
        """Parse the integer fields to ints in case the config is rendered by Jinja and all fields are str."""
        for field in self.integer_fields:
            self.parse_integer(self.config, field)

    def expand_role(self) -> None:
        """Call boto3's `expand_role`, which expands an IAM role name into an ARN."""

    def preprocess_config(self) -> None:
        """Process the config into a usable form."""
        self._create_integer_fields()
        self.log.info("Preprocessing the config and doing required s3_operations")
        self.hook.configure_s3_resources(self.config)
        self.parse_config_integers()
        self.expand_role()
        self.log.info(
            "After preprocessing the config is:\n %s",
            json.dumps(self.config, sort_keys=True, indent=4, separators=(",", ": ")),
        )

    def _create_integer_fields(self) -> None:
        """
        Set fields which should be cast to integers.

        Child classes should override this method if they need integer fields parsed.
        """
        self.integer_fields = []

    def _get_unique_job_name(
        self, proposed_name: str, fail_if_exists: bool, describe_func: Callable[[str], Any]
    ) -> str:
        """
        Return the proposed name if it doesn't already exist, otherwise returns it with a timestamp suffix.

        :param proposed_name: Base name.
        :param fail_if_exists: Will throw an error if a job with that name already exists
            instead of finding a new name.
        :param describe_func: The `describe_` function for that kind of job.
            We use it as an O(1) way to check if a job exists.
        """
        return self._get_unique_name(
            proposed_name, fail_if_exists, describe_func, self._check_if_job_exists, "job"
        )

    def _get_unique_name(
        self,
        proposed_name: str,
        fail_if_exists: bool,
        describe_func: Callable[[str], Any],
        check_exists_func: Callable[[str, Callable[[str], Any]], bool],
        resource_type: str,
    ) -> str:
        """
        Return the proposed name if it doesn't already exist, otherwise returns it with a timestamp suffix.

        :param proposed_name: Base name.
        :param fail_if_exists: Will throw an error if a resource with that name already exists
            instead of finding a new name.
        :param check_exists_func: The function to check if the resource exists.
            It should take the resource name and a describe function as arguments.
        :param resource_type: Type of the resource (e.g., "model", "job").
        """
        self._check_resource_type(resource_type)
        name = proposed_name
        while check_exists_func(name, describe_func):
            # this while should loop only once in most cases, just setting it this way to regenerate a name
            # in case there is collision.
            if fail_if_exists:
                raise AirflowException(f"A SageMaker {resource_type} with name {name} already exists.")
            else:
                name = f"{proposed_name}-{time.time_ns()//1000000}"
                self.log.info("Changed %s name to '%s' to avoid collision.", resource_type, name)
        return name

    def _check_resource_type(self, resource_type: str):
        """Raise exception if resource type is not 'model' or 'job'."""
        if resource_type not in ("model", "job"):
            raise AirflowException(
                "Argument resource_type accepts only 'model' and 'job'. "
                f"Provided value: '{resource_type}'."
            )

    def _check_if_job_exists(self, job_name: str, describe_func: Callable[[str], Any]) -> bool:
        """Return True if job exists, False otherwise."""
        return self._check_if_resource_exists(job_name, "job", describe_func)

    def _check_if_resource_exists(
        self, resource_name: str, resource_type: str, describe_func: Callable[[str], Any]
    ) -> bool:
        """Return True if resource exists, False otherwise."""
        self._check_resource_type(resource_type)
        try:
            describe_func(resource_name)
            self.log.info("Found existing %s with name '%s'.", resource_type, resource_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                return False  # ValidationException is thrown when the resource could not be found
            else:
                raise e

    def execute(self, context: Context):
        raise NotImplementedError("Please implement execute() in sub class!")

    @cached_property
    def hook(self):
        """Return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    @staticmethod
    def path_to_s3_dataset(path) -> Dataset:
        from airflow.providers.common.compat.openlineage.facet import Dataset

        path = path.replace("s3://", "")
        split_path = path.split("/")
        return Dataset(namespace=f"s3://{split_path[0]}", name="/".join(split_path[1:]), facets={})


class SageMakerProcessingOperator(SageMakerBaseOperator):
    """
    Use Amazon SageMaker Processing to analyze data and evaluate machine learning models on Amazon SageMaker.

    With Processing, you can use a simplified, managed experience on SageMaker
    to run your data processing workloads, such as feature engineering, data
    validation, model evaluation, and model interpretation.

     .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerProcessingOperator`

    :param config: The configuration necessary to start a processing job (templated).
        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_processing_job`
    :param aws_conn_id: The AWS connection ID to use.
    :param wait_for_completion: If wait is set to True, the time interval, in seconds,
        that the operation waits to check the status of the processing job.
    :param print_log: if the operator should print the cloudwatch log during processing
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the processing job
    :param max_attempts: Number of times to poll for query state before returning the current state,
        defaults to None.
    :param max_ingestion_time: If wait is set to True, the operation fails if the processing job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
        (default), "increment" (deprecated) and "fail".
    :param deferrable: Run operator in the deferrable mode. This is only effective if wait_for_completion is
        set to True.
    :return Dict: Returns The ARN of the processing job created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        print_log: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        action_if_job_exists: str = "timestamp",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        if action_if_job_exists not in ("increment", "fail", "timestamp"):
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'timestamp', 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )
        if action_if_job_exists == "increment":
            warnings.warn(
                "Action 'increment' on job name conflict has been deprecated for performance reasons."
                "The alternative to 'fail' is now 'timestamp'.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        self.action_if_job_exists = action_if_job_exists
        self.wait_for_completion = wait_for_completion
        self.print_log = print_log
        self.check_interval = check_interval
        self.max_attempts = max_attempts or 60
        self.max_ingestion_time = max_ingestion_time
        self.deferrable = deferrable
        self.serialized_job: dict

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str] | list[list[str]]] = [
            ["ProcessingResources", "ClusterConfig", "InstanceCount"],
            ["ProcessingResources", "ClusterConfig", "VolumeSizeInGB"],
        ]
        if "StoppingCondition" in self.config:
            self.integer_fields.append(["StoppingCondition", "MaxRuntimeInSeconds"])

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "RoleArn" in self.config:
            hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
            self.config["RoleArn"] = hook.expand_role(self.config["RoleArn"])

    def execute(self, context: Context) -> dict:
        self.preprocess_config()

        self.config["ProcessingJobName"] = self._get_unique_job_name(
            self.config["ProcessingJobName"],
            self.action_if_job_exists == "fail",
            self.hook.describe_processing_job,
        )

        if self.deferrable and not self.wait_for_completion:
            self.log.warning(
                "Setting deferrable to True does not have effect when wait_for_completion is set to False."
            )

        wait_for_completion = self.wait_for_completion
        if self.deferrable and self.wait_for_completion:
            # Set wait_for_completion to False so that it waits for the status in the deferred task.
            wait_for_completion = False

        response = self.hook.create_processing_job(
            self.config,
            wait_for_completion=wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Processing Job creation failed: {response}")

        if self.deferrable and self.wait_for_completion:
            response = self.hook.describe_processing_job(self.config["ProcessingJobName"])
            status = response["ProcessingJobStatus"]
            if status in self.hook.failed_states:
                raise AirflowException(f"SageMaker job failed because {response['FailureReason']}")
            elif status == "Completed":
                self.log.info("%s completed successfully.", self.task_id)
                return {"Processing": serialize(response)}

            timeout = self.execution_timeout
            if self.max_ingestion_time:
                timeout = datetime.timedelta(seconds=self.max_ingestion_time)

            self.defer(
                timeout=timeout,
                trigger=SageMakerTrigger(
                    job_name=self.config["ProcessingJobName"],
                    job_type="Processing",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        self.serialized_job = serialize(self.hook.describe_processing_job(self.config["ProcessingJobName"]))
        return {"Processing": self.serialized_job}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, dict]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info(event["message"])
        self.serialized_job = serialize(self.hook.describe_processing_job(event["job_name"]))
        self.log.info("%s completed successfully.", self.task_id)
        return {"Processing": self.serialized_job}

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """Return OpenLineage data gathered from SageMaker's API response saved by processing job."""
        from airflow.providers.openlineage.extractors.base import OperatorLineage

        inputs = []
        outputs = []
        try:
            inputs, outputs = self._extract_s3_dataset_identifiers(
                processing_inputs=self.serialized_job["ProcessingInputs"],
                processing_outputs=self.serialized_job["ProcessingOutputConfig"]["Outputs"],
            )
        except KeyError:
            self.log.exception("Could not find input/output information in Xcom.")

        return OperatorLineage(inputs=inputs, outputs=outputs)

    def _extract_s3_dataset_identifiers(self, processing_inputs, processing_outputs):
        inputs = []
        outputs = []
        try:
            for processing_input in processing_inputs:
                inputs.append(self.path_to_s3_dataset(processing_input["S3Input"]["S3Uri"]))
        except KeyError:
            self.log.exception("Cannot find S3 input details")

        try:
            for processing_output in processing_outputs:
                outputs.append(self.path_to_s3_dataset(processing_output["S3Output"]["S3Uri"]))
        except KeyError:
            self.log.exception("Cannot find S3 output details.")
        return inputs, outputs


class SageMakerEndpointConfigOperator(SageMakerBaseOperator):
    """
    Creates an endpoint configuration that Amazon SageMaker hosting services uses to deploy models.

    In the configuration, you identify one or more models, created using the CreateModel API, to deploy and
    the resources that you want Amazon SageMaker to provision.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerEndpointConfigOperator`

    :param config: The configuration necessary to create an endpoint config.

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_endpoint_config`
    :param aws_conn_id: The AWS connection ID to use.
    :return Dict: Returns The ARN of the endpoint config created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str]] = [["ProductionVariants", "InitialInstanceCount"]]

    def execute(self, context: Context) -> dict:
        self.preprocess_config()
        self.log.info("Creating SageMaker Endpoint Config %s.", self.config["EndpointConfigName"])
        response = self.hook.create_endpoint_config(self.config)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker endpoint config creation failed: {response}")
        else:
            return {
                "EndpointConfig": serialize(
                    self.hook.describe_endpoint_config(self.config["EndpointConfigName"])
                )
            }


class SageMakerEndpointOperator(SageMakerBaseOperator):
    """
    When you create a serverless endpoint, SageMaker provisions and manages the compute resources for you.

    Then, you can make inference requests to the endpoint and receive model predictions
    in response. SageMaker scales the compute resources up and down as needed to handle
    your request traffic.

    Requires an Endpoint Config.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerEndpointOperator`

    :param config:
        The configuration necessary to create an endpoint.

        If you need to create a SageMaker endpoint based on an existed
        SageMaker model and an existed SageMaker endpoint config::

            config = endpoint_configuration

        If you need to create all of SageMaker model, SageMaker endpoint-config and SageMaker endpoint::

            config = {
                "Model": model_configuration,
                "EndpointConfig": endpoint_config_configuration,
                "Endpoint": endpoint_configuration,
            }

        For details of the configuration parameter of model_configuration see
        :py:meth:`SageMaker.Client.create_model`

        For details of the configuration parameter of endpoint_config_configuration see
        :py:meth:`SageMaker.Client.create_endpoint_config`

        For details of the configuration parameter of endpoint_configuration see
        :py:meth:`SageMaker.Client.create_endpoint`

    :param wait_for_completion: Whether the operator should wait until the endpoint creation finishes.
    :param check_interval: If wait is set to True, this is the time interval, in seconds, that this operation
        waits before polling the status of the endpoint creation.
    :param max_ingestion_time: If wait is set to True, this operation fails if the endpoint creation doesn't
        finish within max_ingestion_time seconds. If you set this parameter to None it never times out.
    :param operation: Whether to create an endpoint or update an endpoint. Must be either 'create or 'update'.
    :param aws_conn_id: The AWS connection ID to use.
    :param deferrable:  Will wait asynchronously for completion.
    :return Dict: Returns The ARN of the endpoint created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_ingestion_time: int | None = None,
        operation: str = "create",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time or 3600 * 10
        self.operation = operation.lower()
        if self.operation not in ["create", "update"]:
            raise ValueError('Invalid value! Argument operation has to be one of "create" and "update"')
        self.deferrable = deferrable

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        if "EndpointConfig" in self.config:
            self.integer_fields: list[list[str]] = [
                ["EndpointConfig", "ProductionVariants", "InitialInstanceCount"]
            ]

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "Model" not in self.config:
            return
        hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
        config = self.config["Model"]
        if "ExecutionRoleArn" in config:
            config["ExecutionRoleArn"] = hook.expand_role(config["ExecutionRoleArn"])

    def execute(self, context: Context) -> dict:
        self.preprocess_config()
        model_info = self.config.get("Model")
        endpoint_config_info = self.config.get("EndpointConfig")
        endpoint_info = self.config.get("Endpoint", self.config)
        if model_info:
            self.log.info("Creating SageMaker model %s.", model_info["ModelName"])
            self.hook.create_model(model_info)
        if endpoint_config_info:
            self.log.info("Creating endpoint config %s.", endpoint_config_info["EndpointConfigName"])
            self.hook.create_endpoint_config(endpoint_config_info)
        if self.operation == "create":
            sagemaker_operation = self.hook.create_endpoint
            log_str = "Creating"
        elif self.operation == "update":
            sagemaker_operation = self.hook.update_endpoint
            log_str = "Updating"
        else:
            raise ValueError('Invalid value! Argument operation has to be one of "create" and "update"')
        self.log.info("%s SageMaker endpoint %s.", log_str, endpoint_info["EndpointName"])
        try:
            response = sagemaker_operation(
                endpoint_info,
                wait_for_completion=False,  # waiting for completion is handled here in the operator
            )
        except ClientError as ce:
            if self.operation == "create" and ce.response["Error"]["Message"].startswith(
                "Cannot create already existing endpoint"
            ):
                # if we get an error because the endpoint already exists, we try to update it instead
                self.operation = "update"
                sagemaker_operation = self.hook.update_endpoint
                self.log.warning(
                    "cannot create already existing endpoint %s, "
                    "updating it with the given config instead",
                    endpoint_info["EndpointName"],
                )
                if "Tags" in endpoint_info:
                    self.log.warning(
                        "Provided tags will be ignored in the update operation "
                        "(tags on the existing endpoint will be unchanged)"
                    )
                    endpoint_info.pop("Tags")
                response = sagemaker_operation(
                    endpoint_info,
                    wait_for_completion=False,
                )
            else:
                raise

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker endpoint creation failed: {response}")

        if self.deferrable:
            self.defer(
                trigger=SageMakerTrigger(
                    job_name=endpoint_info["EndpointName"],
                    job_type="endpoint",
                    poke_interval=self.check_interval,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
                timeout=datetime.timedelta(seconds=self.max_ingestion_time),
            )
        elif self.wait_for_completion:
            self.hook.get_waiter("endpoint_in_service").wait(
                EndpointName=endpoint_info["EndpointName"],
                WaiterConfig={"Delay": self.check_interval, "MaxAttempts": self.max_ingestion_time},
            )

        return {
            "EndpointConfig": serialize(
                self.hook.describe_endpoint_config(endpoint_info["EndpointConfigName"])
            ),
            "Endpoint": serialize(self.hook.describe_endpoint(endpoint_info["EndpointName"])),
        }

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, dict]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        response = self.hook.describe_endpoint(event["job_name"])
        return {
            "EndpointConfig": serialize(self.hook.describe_endpoint_config(response["EndpointConfigName"])),
            "Endpoint": serialize(self.hook.describe_endpoint(response["EndpointName"])),
        }


class SageMakerTransformOperator(SageMakerBaseOperator):
    """
    Starts a transform job.

    A transform job uses a trained model to get inferences on a dataset
    and saves these results to an Amazon S3 location that you specify.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerTransformOperator`

    :param config: The configuration necessary to start a transform job (templated).

        If you need to create a SageMaker transform job based on an existed SageMaker model::

            config = transform_config

        If you need to create both SageMaker model and SageMaker Transform job::

            config = {"Model": model_config, "Transform": transform_config}

        For details of the configuration parameter of transform_config see
        :py:meth:`SageMaker.Client.create_transform_job`

        For details of the configuration parameter of model_config, See:
        :py:meth:`SageMaker.Client.create_model`

    :param aws_conn_id: The AWS connection ID to use.
    :param wait_for_completion: Set to True to wait until the transform job finishes.
    :param check_interval: If wait is set to True, the time interval, in seconds,
        that this operation waits to check the status of the transform job.
    :param max_attempts: Number of times to poll for query state before returning the current state,
        defaults to None.
    :param max_ingestion_time: If wait is set to True, the operation fails
        if the transform job doesn't finish within max_ingestion_time seconds. If you
        set this parameter to None, the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a transform job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
        (default), "increment" (deprecated) and "fail".
        This is only relevant if check_if_job_exists is True.
    :return Dict: Returns The ARN of the model created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "timestamp",
        check_if_model_exists: bool = True,
        action_if_model_exists: str = "timestamp",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_attempts = max_attempts or 60
        self.max_ingestion_time = max_ingestion_time
        self.check_if_job_exists = check_if_job_exists
        if action_if_job_exists in ("increment", "fail", "timestamp"):
            if action_if_job_exists == "increment":
                warnings.warn(
                    "Action 'increment' on job name conflict has been deprecated for performance reasons."
                    "The alternative to 'fail' is now 'timestamp'.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            self.action_if_job_exists = action_if_job_exists
        else:
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'timestamp', 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )
        self.check_if_model_exists = check_if_model_exists
        if action_if_model_exists in ("fail", "timestamp"):
            self.action_if_model_exists = action_if_model_exists
        else:
            raise AirflowException(
                f"Argument action_if_model_exists accepts only 'timestamp' and 'fail'. \
                Provided value: '{action_if_model_exists}'."
            )
        self.deferrable = deferrable
        self.serialized_model: dict
        self.serialized_transform: dict

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str]] = [
            ["Transform", "TransformResources", "InstanceCount"],
            ["Transform", "MaxConcurrentTransforms"],
            ["Transform", "MaxPayloadInMB"],
        ]
        if "Transform" not in self.config:
            for field in self.integer_fields:
                field.pop(0)

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "Model" not in self.config:
            return
        config = self.config["Model"]
        if "ExecutionRoleArn" in config:
            hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
            config["ExecutionRoleArn"] = hook.expand_role(config["ExecutionRoleArn"])

    def execute(self, context: Context) -> dict:
        self.preprocess_config()

        transform_config = self.config.get("Transform", self.config)
        if self.check_if_job_exists:
            transform_config["TransformJobName"] = self._get_unique_job_name(
                transform_config["TransformJobName"],
                self.action_if_job_exists == "fail",
                self.hook.describe_transform_job,
            )

        model_config = self.config.get("Model")
        if model_config:
            if self.check_if_model_exists:
                model_config["ModelName"] = self._get_unique_model_name(
                    model_config["ModelName"],
                    self.action_if_model_exists == "fail",
                    self.hook.describe_model,
                )
                if "ModelName" in self.config["Transform"].keys():
                    self.config["Transform"]["ModelName"] = model_config["ModelName"]
            self.log.info("Creating SageMaker Model %s for transform job", model_config["ModelName"])
            self.hook.create_model(model_config)

        self.log.info("Creating SageMaker transform Job %s.", transform_config["TransformJobName"])

        if self.deferrable and not self.wait_for_completion:
            self.log.warning(
                "Setting deferrable to True does not have effect when wait_for_completion is set to False."
            )

        wait_for_completion = self.wait_for_completion
        if self.deferrable and self.wait_for_completion:
            # Set wait_for_completion to False so that it waits for the status in the deferred task.
            wait_for_completion = False

        response = self.hook.create_transform_job(
            transform_config,
            wait_for_completion=wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker transform Job creation failed: {response}")

        if self.deferrable and self.wait_for_completion:
            response = self.hook.describe_transform_job(transform_config["TransformJobName"])
            status = response["TransformJobStatus"]
            if status in self.hook.failed_states:
                raise AirflowException(f"SageMaker job failed because {response['FailureReason']}")

            if status == "Completed":
                self.log.info("%s completed successfully.", self.task_id)
                return {
                    "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
                    "Transform": serialize(response),
                }

            timeout = self.execution_timeout
            if self.max_ingestion_time:
                timeout = datetime.timedelta(seconds=self.max_ingestion_time)

            self.defer(
                timeout=timeout,
                trigger=SageMakerTrigger(
                    job_name=transform_config["TransformJobName"],
                    job_type="Transform",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        return self.serialize_result(transform_config["TransformJobName"])

    def _get_unique_model_name(
        self, proposed_name: str, fail_if_exists: bool, describe_func: Callable[[str], Any]
    ) -> str:
        return self._get_unique_name(
            proposed_name, fail_if_exists, describe_func, self._check_if_model_exists, "model"
        )

    def _check_if_model_exists(self, model_name: str, describe_func: Callable[[str], Any]) -> bool:
        """Return True if model exists, False otherwise."""
        return self._check_if_resource_exists(model_name, "model", describe_func)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, dict]:
        event = validate_execute_complete_event(event)

        self.log.info(event["message"])
        return self.serialize_result(event["job_name"])

    def serialize_result(self, job_name: str) -> dict[str, dict]:
        job_description = self.hook.describe_transform_job(job_name)
        self.serialized_model = serialize(self.hook.describe_model(job_description["ModelName"]))
        self.serialized_transform = serialize(job_description)
        return {"Model": self.serialized_model, "Transform": self.serialized_transform}

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """Return OpenLineage data gathered from SageMaker's API response saved by transform job."""
        from airflow.providers.openlineage.extractors import OperatorLineage

        model_package_arn = None
        transform_input = None
        transform_output = None

        try:
            model_package_arn = self.serialized_model["PrimaryContainer"]["ModelPackageName"]
        except KeyError:
            self.log.exception("Cannot find Model Package Name.")

        try:
            transform_input = self.serialized_transform["TransformInput"]["DataSource"]["S3DataSource"][
                "S3Uri"
            ]
            transform_output = self.serialized_transform["TransformOutput"]["S3OutputPath"]
        except KeyError:
            self.log.exception("Cannot find some required input/output details.")

        inputs = []

        if transform_input is not None:
            inputs.append(self.path_to_s3_dataset(transform_input))

        if model_package_arn is not None:
            model_data_urls = self._get_model_data_urls(model_package_arn)
            for model_data_url in model_data_urls:
                inputs.append(self.path_to_s3_dataset(model_data_url))

        outputs = []
        if transform_output is not None:
            outputs.append(self.path_to_s3_dataset(transform_output))

        return OperatorLineage(inputs=inputs, outputs=outputs)

    def _get_model_data_urls(self, model_package_arn) -> list:
        model_data_urls = []
        try:
            model_containers = self.hook.get_conn().describe_model_package(
                ModelPackageName=model_package_arn
            )["InferenceSpecification"]["Containers"]

            for container in model_containers:
                model_data_urls.append(container["ModelDataUrl"])
        except KeyError:
            self.log.exception("Cannot retrieve model details.")

        return model_data_urls


class SageMakerTuningOperator(SageMakerBaseOperator):
    """
    Starts a hyperparameter tuning job.

    A hyperparameter tuning job finds the best version of a model by running
    many training jobs on your dataset using the algorithm you choose and
    values for hyperparameters within ranges that you specify. It then chooses
    the hyperparameter values that result in a model that performs the best,
    as measured by an objective metric that you choose.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerTuningOperator`

    :param config: The configuration necessary to start a tuning job (templated).

        For details of the configuration parameter see
        :py:meth:`SageMaker.Client.create_hyper_parameter_tuning_job`
    :param aws_conn_id: The AWS connection ID to use.
    :param wait_for_completion: Set to True to wait until the tuning job finishes.
    :param check_interval: If wait is set to True, the time interval, in seconds,
        that this operation waits to check the status of the tuning job.
    :param max_ingestion_time: If wait is set to True, the operation fails
        if the tuning job doesn't finish within max_ingestion_time seconds. If you
        set this parameter to None, the operation does not timeout.
    :param deferrable: Will wait asynchronously for completion.
    :return Dict: Returns The ARN of the tuning job created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_ingestion_time: int | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.deferrable = deferrable

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "TrainingJobDefinition" in self.config:
            config = self.config["TrainingJobDefinition"]
            if "RoleArn" in config:
                hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
                config["RoleArn"] = hook.expand_role(config["RoleArn"])

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str]] = [
            ["HyperParameterTuningJobConfig", "ResourceLimits", "MaxNumberOfTrainingJobs"],
            ["HyperParameterTuningJobConfig", "ResourceLimits", "MaxParallelTrainingJobs"],
            ["TrainingJobDefinition", "ResourceConfig", "InstanceCount"],
            ["TrainingJobDefinition", "ResourceConfig", "VolumeSizeInGB"],
            ["TrainingJobDefinition", "StoppingCondition", "MaxRuntimeInSeconds"],
        ]

    def execute(self, context: Context) -> dict:
        self.preprocess_config()
        self.log.info(
            "Creating SageMaker Hyper-Parameter Tuning Job %s",
            self.config["HyperParameterTuningJobName"],
        )
        response = self.hook.create_tuning_job(
            self.config,
            wait_for_completion=False,  # we handle this here
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Tuning Job creation failed: {response}")

        if self.deferrable:
            self.defer(
                trigger=SageMakerTrigger(
                    job_name=self.config["HyperParameterTuningJobName"],
                    job_type="tuning",
                    poke_interval=self.check_interval,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
                timeout=(
                    datetime.timedelta(seconds=self.max_ingestion_time)
                    if self.max_ingestion_time is not None
                    else None
                ),
            )
            description = {}  # never executed but makes static checkers happy
        elif self.wait_for_completion:
            description = self.hook.check_status(
                self.config["HyperParameterTuningJobName"],
                "HyperParameterTuningJobStatus",
                self.hook.describe_tuning_job,
                self.check_interval,
                self.max_ingestion_time,
            )
        else:
            description = self.hook.describe_tuning_job(self.config["HyperParameterTuningJobName"])

        return {"Tuning": serialize(description)}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, dict]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")
        return {"Tuning": serialize(self.hook.describe_tuning_job(event["job_name"]))}


class SageMakerModelOperator(SageMakerBaseOperator):
    """
    Creates a model in Amazon SageMaker.

    In the request, you name the model and describe a primary container. For the
    primary container, you specify the Docker image that contains inference code,
    artifacts (from prior training), and a custom environment map that the inference
    code uses when you deploy the model for predictions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerModelOperator`

    :param config: The configuration necessary to create a model.

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_model`
    :param aws_conn_id: The AWS connection ID to use.
    :return Dict: Returns The ARN of the model created in Amazon SageMaker.
    """

    def __init__(self, *, config: dict, aws_conn_id: str | None = DEFAULT_CONN_ID, **kwargs):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "ExecutionRoleArn" in self.config:
            hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
            self.config["ExecutionRoleArn"] = hook.expand_role(self.config["ExecutionRoleArn"])

    def execute(self, context: Context) -> dict:
        self.preprocess_config()
        self.log.info("Creating SageMaker Model %s.", self.config["ModelName"])
        response = self.hook.create_model(self.config)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker model creation failed: {response}")
        else:
            return {"Model": serialize(self.hook.describe_model(self.config["ModelName"]))}


class SageMakerTrainingOperator(SageMakerBaseOperator):
    """
    Starts a model training job.

    After training completes, Amazon SageMaker saves the resulting
    model artifacts to an Amazon S3 location that you specify.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerTrainingOperator`

    :param config: The configuration necessary to start a training job (templated).

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_training_job`
    :param aws_conn_id: The AWS connection ID to use.
    :param wait_for_completion: If wait is set to True, the time interval, in seconds,
        that the operation waits to check the status of the training job.
    :param print_log: if the operator should print the cloudwatch log during training
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the training job
    :param max_attempts: Number of times to poll for query state before returning the current state,
        defaults to None.
    :param max_ingestion_time: If wait is set to True, the operation fails if the training job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a training job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "timestamp"
        (default), "increment" (deprecated) and "fail".
        This is only relevant if check_if_job_exists is True.
    :param deferrable: Run operator in the deferrable mode. This is only effective if wait_for_completion is
        set to True.
    :return Dict: Returns The ARN of the training job created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        print_log: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "timestamp",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.print_log = print_log
        self.check_interval = check_interval
        self.max_attempts = max_attempts or 60
        self.max_ingestion_time = max_ingestion_time
        self.check_if_job_exists = check_if_job_exists
        if action_if_job_exists in {"timestamp", "increment", "fail"}:
            if action_if_job_exists == "increment":
                warnings.warn(
                    "Action 'increment' on job name conflict has been deprecated for performance reasons."
                    "The alternative to 'fail' is now 'timestamp'.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            self.action_if_job_exists = action_if_job_exists
        else:
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'timestamp', 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )
        self.deferrable = deferrable
        self.serialized_training_data: dict

    def expand_role(self) -> None:
        """Expand an IAM role name into an ARN."""
        if "RoleArn" in self.config:
            hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
            self.config["RoleArn"] = hook.expand_role(self.config["RoleArn"])

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str]] = [
            ["ResourceConfig", "InstanceCount"],
            ["ResourceConfig", "VolumeSizeInGB"],
            ["StoppingCondition", "MaxRuntimeInSeconds"],
        ]

    def execute(self, context: Context) -> dict:
        self.preprocess_config()

        if self.check_if_job_exists:
            self.config["TrainingJobName"] = self._get_unique_job_name(
                self.config["TrainingJobName"],
                self.action_if_job_exists == "fail",
                self.hook.describe_training_job,
            )

        self.log.info("Creating SageMaker training job %s.", self.config["TrainingJobName"])

        if self.deferrable and not self.wait_for_completion:
            self.log.warning(
                "Setting deferrable to True does not have effect when wait_for_completion is set to False."
            )

        wait_for_completion = self.wait_for_completion
        if self.deferrable and self.wait_for_completion:
            # Set wait_for_completion to False so that it waits for the status in the deferred task.
            wait_for_completion = False

        response = self.hook.create_training_job(
            self.config,
            wait_for_completion=wait_for_completion,
            print_log=self.print_log,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Training Job creation failed: {response}")

        if self.deferrable and self.wait_for_completion:
            description = self.hook.describe_training_job(self.config["TrainingJobName"])
            status = description["TrainingJobStatus"]

            if self.print_log:
                instance_count = description["ResourceConfig"]["InstanceCount"]
                last_describe_job_call = time.monotonic()
                job_already_completed = status not in self.hook.non_terminal_states
                _, description, last_describe_job_call = self.hook.describe_training_job_with_log(
                    self.config["TrainingJobName"],
                    {},
                    [],
                    instance_count,
                    LogState.COMPLETE if job_already_completed else LogState.TAILING,
                    description,
                    last_describe_job_call,
                )
                self.log.info(secondary_training_status_message(description, None))

            if status in self.hook.failed_states:
                reason = description.get("FailureReason", "(No reason provided)")
                raise AirflowException(f"SageMaker job failed because {reason}")
            elif status == "Completed":
                log_message = f"{self.task_id} completed successfully."
                if self.print_log:
                    billable_seconds = SageMakerHook.count_billable_seconds(
                        training_start_time=description["TrainingStartTime"],
                        training_end_time=description["TrainingEndTime"],
                        instance_count=instance_count,
                    )
                    log_message = f"Billable seconds: {billable_seconds}\n{log_message}"
                self.log.info(log_message)
                return {"Training": serialize(description)}

            timeout = self.execution_timeout
            if self.max_ingestion_time:
                timeout = datetime.timedelta(seconds=self.max_ingestion_time)

            self.defer(
                timeout=timeout,
                trigger=SageMakerTrigger(
                    job_name=self.config["TrainingJobName"],
                    job_type="Training",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        return self.serialize_result(self.config["TrainingJobName"])

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, dict]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")

        self.log.info(event["message"])
        return self.serialize_result(event["job_name"])

    def serialize_result(self, job_name: str) -> dict[str, dict]:
        self.serialized_training_data = serialize(self.hook.describe_training_job(job_name))
        return {"Training": self.serialized_training_data}

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """Return OpenLineage data gathered from SageMaker's API response saved by training job."""
        from airflow.providers.openlineage.extractors import OperatorLineage

        inputs = []
        outputs = []
        try:
            for input_data in self.serialized_training_data["InputDataConfig"]:
                inputs.append(self.path_to_s3_dataset(input_data["DataSource"]["S3DataSource"]["S3Uri"]))
        except KeyError:
            self.log.exception("Issues extracting inputs.")

        try:
            outputs.append(
                self.path_to_s3_dataset(self.serialized_training_data["ModelArtifacts"]["S3ModelArtifacts"])
            )
        except KeyError:
            self.log.exception("Issues extracting inputs.")
        return OperatorLineage(inputs=inputs, outputs=outputs)


class SageMakerDeleteModelOperator(SageMakerBaseOperator):
    """
    Deletes a SageMaker model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerDeleteModelOperator`

    :param config: The configuration necessary to delete the model.
        For details of the configuration parameter see :py:meth:`SageMaker.Client.delete_model`
    :param aws_conn_id: The AWS connection ID to use.
    """

    def __init__(self, *, config: dict, aws_conn_id: str | None = DEFAULT_CONN_ID, **kwargs):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)

    def execute(self, context: Context) -> Any:
        sagemaker_hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
        sagemaker_hook.delete_model(model_name=self.config["ModelName"])
        self.log.info("Model %s deleted successfully.", self.config["ModelName"])


class SageMakerStartPipelineOperator(SageMakerBaseOperator):
    """
    Starts a SageMaker pipeline execution.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStartPipelineOperator`

    :param config: The configuration to start the pipeline execution.
    :param aws_conn_id: The AWS connection ID to use.
    :param pipeline_name: Name of the pipeline to start.
    :param display_name: The name this pipeline execution will have in the UI. Doesn't need to be unique.
    :param pipeline_params: Optional parameters for the pipeline.
        All parameters supplied need to already be present in the pipeline definition.
    :param wait_for_completion: If true, this operator will only complete once the pipeline is complete.
    :param check_interval: How long to wait between checks for pipeline status when waiting for completion.
    :param waiter_max_attempts: How many times to check the status before failing.
    :param verbose: Whether to print steps details when waiting for completion.
        Defaults to true, consider turning off for pipelines that have thousands of steps.
    :param deferrable: Run operator in the deferrable mode.

    :return str: Returns The ARN of the pipeline execution created in Amazon SageMaker.
    """

    template_fields: Sequence[str] = (
        "aws_conn_id",
        "pipeline_name",
        "display_name",
        "pipeline_params",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        pipeline_name: str,
        display_name: str = "airflow-triggered-execution",
        pipeline_params: dict | None = None,
        wait_for_completion: bool = False,
        check_interval: int = CHECK_INTERVAL_SECOND,
        waiter_max_attempts: int = 9999,
        verbose: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config={}, aws_conn_id=aws_conn_id, **kwargs)
        self.pipeline_name = pipeline_name
        self.display_name = display_name
        self.pipeline_params = pipeline_params
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.waiter_max_attempts = waiter_max_attempts
        self.verbose = verbose
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        arn = self.hook.start_pipeline(
            pipeline_name=self.pipeline_name,
            display_name=self.display_name,
            pipeline_params=self.pipeline_params,
        )
        self.log.info(
            "Starting a new execution for pipeline %s, running with ARN %s", self.pipeline_name, arn
        )
        if self.deferrable:
            self.defer(
                trigger=SageMakerPipelineTrigger(
                    waiter_type=SageMakerPipelineTrigger.Type.COMPLETE,
                    pipeline_execution_arn=arn,
                    waiter_delay=self.check_interval,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.hook.check_status(
                arn,
                "PipelineExecutionStatus",
                lambda p: self.hook.describe_pipeline_exec(p, self.verbose),
                self.check_interval,
                non_terminal_states=self.hook.pipeline_non_terminal_states,
                max_ingestion_time=self.waiter_max_attempts * self.check_interval,
            )
        return arn

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Failure during pipeline execution: {event}")
        return event["value"]


class SageMakerStopPipelineOperator(SageMakerBaseOperator):
    """
    Stops a SageMaker pipeline execution.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStopPipelineOperator`

    :param config: The configuration to start the pipeline execution.
    :param aws_conn_id: The AWS connection ID to use.
    :param pipeline_exec_arn: Amazon Resource Name of the pipeline execution to stop.
    :param wait_for_completion: If true, this operator will only complete once the pipeline is fully stopped.
    :param check_interval: How long to wait between checks for pipeline status when waiting for completion.
    :param verbose: Whether to print steps details when waiting for completion.
        Defaults to true, consider turning off for pipelines that have thousands of steps.
    :param fail_if_not_running: raises an exception if the pipeline stopped or succeeded before this was run
    :param deferrable: Run operator in the deferrable mode.

    :return str: Returns the status of the pipeline execution after the operation has been done.
    """

    template_fields: Sequence[str] = (
        "aws_conn_id",
        "pipeline_exec_arn",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        pipeline_exec_arn: str,
        wait_for_completion: bool = False,
        check_interval: int = CHECK_INTERVAL_SECOND,
        waiter_max_attempts: int = 9999,
        verbose: bool = True,
        fail_if_not_running: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(config={}, aws_conn_id=aws_conn_id, **kwargs)
        self.pipeline_exec_arn = pipeline_exec_arn
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.waiter_max_attempts = waiter_max_attempts
        self.verbose = verbose
        self.fail_if_not_running = fail_if_not_running
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        status = self.hook.stop_pipeline(
            pipeline_exec_arn=self.pipeline_exec_arn,
            fail_if_not_running=self.fail_if_not_running,
        )
        self.log.info(
            "Stop requested for pipeline execution with ARN %s. Status is now %s",
            self.pipeline_exec_arn,
            status,
        )

        if status not in self.hook.pipeline_non_terminal_states:
            # pipeline already stopped
            return status

        # else, eventually wait for completion
        if self.deferrable:
            self.defer(
                trigger=SageMakerPipelineTrigger(
                    waiter_type=SageMakerPipelineTrigger.Type.STOPPED,
                    pipeline_execution_arn=self.pipeline_exec_arn,
                    waiter_delay=self.check_interval,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            status = self.hook.check_status(
                self.pipeline_exec_arn,
                "PipelineExecutionStatus",
                lambda p: self.hook.describe_pipeline_exec(p, self.verbose),
                self.check_interval,
                non_terminal_states=self.hook.pipeline_non_terminal_states,
                max_ingestion_time=self.waiter_max_attempts * self.check_interval,
            )["PipelineExecutionStatus"]

        return status

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Failure during pipeline execution: {event}")

        # theoretically we should do a `describe` call to know this,
        # but if we reach this point, this is the only possible status
        return "Stopped"


class SageMakerRegisterModelVersionOperator(SageMakerBaseOperator):
    """
    Register a SageMaker model by creating a model version that specifies the model group to which it belongs.

    Will create the model group if it does not exist already.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerRegisterModelVersionOperator`

    :param image_uri: The Amazon EC2 Container Registry (Amazon ECR) path where inference code is stored.
    :param model_url: The Amazon S3 path where the model artifacts (the trained weights of the model), which
        result from model training, are stored. This path must point to a single gzip compressed tar archive
        (.tar.gz suffix).
    :param package_group_name: The name of the model package group that the model is going to be registered
        to. Will be created if it doesn't already exist.
    :param package_group_desc: Description of the model package group, if it was to be created (optional).
    :param package_desc: Description of the model package (optional).
    :param model_approval: Approval status of the model package. Defaults to PendingManualApproval
    :param extras: Can contain extra parameters for the boto call to create_model_package, and/or overrides
        for any parameter defined above. For a complete list of available parameters, see
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_model_package

    :return str: Returns the ARN of the model package created.
    """

    template_fields: Sequence[str] = (
        "image_uri",
        "model_url",
        "package_group_name",
        "package_group_desc",
        "package_desc",
        "model_approval",
    )

    def __init__(
        self,
        *,
        image_uri: str,
        model_url: str,
        package_group_name: str,
        package_group_desc: str = "",
        package_desc: str = "",
        model_approval: ApprovalStatus = ApprovalStatus.PENDING_MANUAL_APPROVAL,
        extras: dict | None = None,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        config: dict | None = None,
        **kwargs,
    ):
        super().__init__(config=config or {}, aws_conn_id=aws_conn_id, **kwargs)
        self.image_uri = image_uri
        self.model_url = model_url
        self.package_group_name = package_group_name
        self.package_group_desc = package_group_desc
        self.package_desc = package_desc
        self.model_approval = model_approval
        self.extras = extras

    def execute(self, context: Context):
        # create a model package group if it does not exist
        group_created = self.hook.create_model_package_group(self.package_group_name, self.package_desc)

        # then create a model package in that group
        input_dict = {
            "InferenceSpecification": {
                "Containers": [
                    {
                        "Image": self.image_uri,
                        "ModelDataUrl": self.model_url,
                    }
                ],
                "SupportedContentTypes": ["text/csv"],
                "SupportedResponseMIMETypes": ["text/csv"],
            },
            "ModelPackageGroupName": self.package_group_name,
            "ModelPackageDescription": self.package_desc,
            "ModelApprovalStatus": self.model_approval.value,
        }
        if self.extras:
            input_dict.update(self.extras)  # overrides config above if keys are redefined in extras
        try:
            res = self.hook.conn.create_model_package(**input_dict)
            return res["ModelPackageArn"]
        except ClientError:
            # rollback group creation if adding the model to it was not successful
            if group_created:
                self.hook.conn.delete_model_package_group(ModelPackageGroupName=self.package_group_name)
            raise


class SageMakerAutoMLOperator(SageMakerBaseOperator):
    """
    Creates an auto ML job, learning to predict the given column from the data provided through S3.

    The learning output is written to the specified S3 location.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerAutoMLOperator`

    :param job_name: Name of the job to create, needs to be unique within the account.
    :param s3_input: The S3 location (folder or file) where to fetch the data.
        By default, it expects csv with headers.
    :param target_attribute: The name of the column containing the values to predict.
    :param s3_output: The S3 folder where to write the model artifacts. Must be 128 characters or fewer.
    :param role_arn: The ARN of the IAM role to use when interacting with S3.
        Must have read access to the input, and write access to the output folder.
    :param compressed_input: Set to True if the input is gzipped.
    :param time_limit: The maximum amount of time in seconds to spend training the model(s).
    :param autodeploy_endpoint_name: If specified, the best model will be deployed to an endpoint with
        that name. No deployment made otherwise.
    :param extras: Use this dictionary to set any variable input variable for job creation that is not
        offered through the parameters of this function. The format is described in:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_auto_ml_job
    :param wait_for_completion: Whether to wait for the job to finish before returning. Defaults to True.
    :param check_interval: Interval in seconds between 2 status checks when waiting for completion.

    :returns: Only if waiting for completion, a dictionary detailing the best model. The structure is that of
        the "BestCandidate" key in:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
    """

    template_fields: Sequence[str] = (
        "job_name",
        "s3_input",
        "target_attribute",
        "s3_output",
        "role_arn",
        "compressed_input",
        "time_limit",
        "autodeploy_endpoint_name",
        "extras",
    )

    def __init__(
        self,
        *,
        job_name: str,
        s3_input: str,
        target_attribute: str,
        s3_output: str,
        role_arn: str,
        compressed_input: bool = False,
        time_limit: int | None = None,
        autodeploy_endpoint_name: str | None = None,
        extras: dict | None = None,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        config: dict | None = None,
        **kwargs,
    ):
        super().__init__(config=config or {}, aws_conn_id=aws_conn_id, **kwargs)
        self.job_name = job_name
        self.s3_input = s3_input
        self.target_attribute = target_attribute
        self.s3_output = s3_output
        self.role_arn = role_arn
        self.compressed_input = compressed_input
        self.time_limit = time_limit
        self.autodeploy_endpoint_name = autodeploy_endpoint_name
        self.extras = extras
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval

    def execute(self, context: Context) -> dict | None:
        best = self.hook.create_auto_ml_job(
            self.job_name,
            self.s3_input,
            self.target_attribute,
            self.s3_output,
            self.role_arn,
            self.compressed_input,
            self.time_limit,
            self.autodeploy_endpoint_name,
            self.extras,
            self.wait_for_completion,
            self.check_interval,
        )
        return best


class SageMakerCreateExperimentOperator(SageMakerBaseOperator):
    """
    Creates a SageMaker experiment, to be then associated to jobs etc.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerCreateExperimentOperator`

    :param name: name of the experiment, must be unique within the AWS account
    :param description: description of the experiment, optional
    :param tags: tags to attach to the experiment, optional
    :param aws_conn_id: The AWS connection ID to use.

    :returns: the ARN of the experiment created, though experiments are referred to by name
    """

    template_fields: Sequence[str] = (
        "name",
        "description",
        "tags",
    )

    def __init__(
        self,
        *,
        name: str,
        description: str | None = None,
        tags: dict | None = None,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        **kwargs,
    ):
        super().__init__(config={}, aws_conn_id=aws_conn_id, **kwargs)
        self.name = name
        self.description = description
        self.tags = tags or {}

    def execute(self, context: Context) -> str:
        sagemaker_hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
        params = {
            "ExperimentName": self.name,
            "Description": self.description,
            "Tags": format_tags(self.tags),
        }
        ans = sagemaker_hook.conn.create_experiment(**trim_none_values(params))
        arn = ans["ExperimentArn"]
        self.log.info("Experiment %s created successfully with ARN %s.", self.name, arn)
        return arn


class SageMakerCreateNotebookOperator(BaseOperator):
    """
    Create a SageMaker notebook.

    More information regarding parameters of this operator can be found here
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker/client/create_notebook_instance.html.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerCreateNotebookOperator`

    :param instance_name: The name of the notebook instance.
    :param instance_type: The type of instance to create.
    :param role_arn: The Amazon Resource Name (ARN) of the IAM role that SageMaker can assume to access
    :param volume_size_in_gb: Size in GB of the EBS root device volume of the notebook instance.
    :param volume_kms_key_id: The KMS key ID for the EBS root device volume.
    :param lifecycle_config_name: The name of the lifecycle configuration to associate with the notebook
    :param direct_internet_access: Whether to enable direct internet access for the notebook instance.
    :param root_access: Whether to give the notebook instance root access to the Amazon S3 bucket.
    :param wait_for_completion: Whether or not to wait for the notebook to be InService before returning
    :param create_instance_kwargs: Additional configuration options for the create call.
    :param aws_conn_id: The AWS connection ID to use.

    :return: The ARN of the created notebook.
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "instance_type",
        "role_arn",
        "volume_size_in_gb",
        "volume_kms_key_id",
        "lifecycle_config_name",
        "direct_internet_access",
        "root_access",
        "wait_for_completion",
        "create_instance_kwargs",
    )

    ui_color = "#ff7300"

    def __init__(
        self,
        *,
        instance_name: str,
        instance_type: str,
        role_arn: str,
        volume_size_in_gb: int | None = None,
        volume_kms_key_id: str | None = None,
        lifecycle_config_name: str | None = None,
        direct_internet_access: str | None = None,
        root_access: str | None = None,
        create_instance_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.role_arn = role_arn
        self.volume_size_in_gb = volume_size_in_gb
        self.volume_kms_key_id = volume_kms_key_id
        self.lifecycle_config_name = lifecycle_config_name
        self.direct_internet_access = direct_internet_access
        self.root_access = root_access
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
        self.create_instance_kwargs = create_instance_kwargs or {}

        if self.create_instance_kwargs.get("tags") is not None:
            self.create_instance_kwargs["tags"] = format_tags(self.create_instance_kwargs["tags"])

    @cached_property
    def hook(self) -> SageMakerHook:
        """Create and return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        create_notebook_instance_kwargs = {
            "NotebookInstanceName": self.instance_name,
            "InstanceType": self.instance_type,
            "RoleArn": self.role_arn,
            "VolumeSizeInGB": self.volume_size_in_gb,
            "KmsKeyId": self.volume_kms_key_id,
            "LifecycleConfigName": self.lifecycle_config_name,
            "DirectInternetAccess": self.direct_internet_access,
            "RootAccess": self.root_access,
        }
        if self.create_instance_kwargs:
            create_notebook_instance_kwargs.update(self.create_instance_kwargs)

        self.log.info("Creating SageMaker notebook %s.", self.instance_name)
        response = self.hook.conn.create_notebook_instance(**prune_dict(create_notebook_instance_kwargs))

        self.log.info("SageMaker notebook created: %s", response["NotebookInstanceArn"])

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to be in service", self.instance_name)
            waiter = self.hook.conn.get_waiter("notebook_instance_in_service")
            waiter.wait(NotebookInstanceName=self.instance_name)

        return response["NotebookInstanceArn"]


class SageMakerStopNotebookOperator(BaseOperator):
    """
    Stop a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStopNotebookOperator`

    :param instance_name: The name of the notebook instance to stop.
    :param wait_for_completion: Whether or not to wait for the notebook to be stopped before returning
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> SageMakerHook:
        """Create and return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Stopping SageMaker notebook %s.", self.instance_name)
        self.hook.conn.stop_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to stop", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_stopped").wait(
                NotebookInstanceName=self.instance_name
            )


class SageMakerDeleteNotebookOperator(BaseOperator):
    """
    Delete a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerDeleteNotebookOperator`

    :param instance_name: The name of the notebook instance to delete.
    :param wait_for_completion: Whether or not to wait for the notebook to delete before returning.
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    @cached_property
    def hook(self) -> SageMakerHook:
        """Create and return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Deleting SageMaker notebook %s....", self.instance_name)
        self.hook.conn.delete_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to delete...", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_deleted").wait(
                NotebookInstanceName=self.instance_name
            )


class SageMakerStartNoteBookOperator(BaseOperator):
    """
    Start a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStartNotebookOperator`

    :param instance_name: The name of the notebook instance to start.
    :param wait_for_completion: Whether or not to wait for notebook to be InService before returning
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    @cached_property
    def hook(self) -> SageMakerHook:
        """Create and return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Starting SageMaker notebook %s....", self.instance_name)
        self.hook.conn.start_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to start...", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_in_service").wait(
                NotebookInstanceName=self.instance_name
            )
