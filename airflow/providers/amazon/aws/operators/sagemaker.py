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

import json
import time
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Sequence

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerTrigger
from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.providers.amazon.aws.utils.sagemaker import ApprovalStatus
from airflow.providers.amazon.aws.utils.tags import format_tags
from airflow.utils.json import AirflowJsonEncoder

if TYPE_CHECKING:
    from airflow.utils.context import Context

DEFAULT_CONN_ID: str = "aws_default"
CHECK_INTERVAL_SECOND: int = 30


def serialize(result: dict) -> str:
    return json.loads(json.dumps(result, cls=AirflowJsonEncoder))


class SageMakerBaseOperator(BaseOperator):
    """This is the base operator for all SageMaker operators.

    :param config: The configuration necessary to start a training job (templated)
    """

    template_fields: Sequence[str] = ("config",)
    template_ext: Sequence[str] = ()
    template_fields_renderers: dict = {"config": "json"}
    ui_color: str = "#ededed"
    integer_fields: list[list[Any]] = []

    def __init__(self, *, config: dict, aws_conn_id: str = DEFAULT_CONN_ID, **kwargs):
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
        """Placeholder for calling boto3's `expand_role`, which expands an IAM role name into an ARN."""

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
        Returns the proposed name if it doesn't already exist, otherwise returns it with a timestamp suffix.

        :param proposed_name: Base name.
        :param fail_if_exists: Will throw an error if a job with that name already exists
            instead of finding a new name.
        :param describe_func: The `describe_` function for that kind of job.
            We use it as an O(1) way to check if a job exists.
        """
        job_name = proposed_name
        while self._check_if_job_exists(job_name, describe_func):
            # this while should loop only once in most cases, just setting it this way to regenerate a name
            # in case there is collision.
            if fail_if_exists:
                raise AirflowException(f"A SageMaker job with name {job_name} already exists.")
            else:
                job_name = f"{proposed_name}-{time.time_ns()//1000000}"
                self.log.info("Changed job name to '%s' to avoid collision.", job_name)
        return job_name

    def _check_if_job_exists(self, job_name, describe_func: Callable[[str], Any]) -> bool:
        """Returns True if job exists, False otherwise."""
        try:
            describe_func(job_name)
            self.log.info("Found existing job with name '%s'.", job_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                return False  # ValidationException is thrown when the job could not be found
            else:
                raise e

    def execute(self, context: Context):
        raise NotImplementedError("Please implement execute() in sub class!")

    @cached_property
    def hook(self):
        """Return SageMakerHook."""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)


class SageMakerProcessingOperator(SageMakerBaseOperator):
    """
    Use Amazon SageMaker Processing to analyze data and evaluate machine learning
    models on Amazon SageMake. With Processing, you can use a simplified, managed
    experience on SageMaker to run your data processing workloads, such as feature
    engineering, data validation, model evaluation, and model interpretation.

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
        aws_conn_id: str = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        print_log: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        action_if_job_exists: str = "timestamp",
        deferrable: bool = False,
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

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        self.integer_fields: list[list[str] | list[list[str]]] = [
            ["ProcessingResources", "ClusterConfig", "InstanceCount"],
            ["ProcessingResources", "ClusterConfig", "VolumeSizeInGB"],
        ]
        if "StoppingCondition" in self.config:
            self.integer_fields.append(["StoppingCondition", "MaxRuntimeInSeconds"])

    def expand_role(self) -> None:
        """Expands an IAM role name into an ARN."""
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
            self.defer(
                timeout=self.execution_timeout,
                trigger=SageMakerTrigger(
                    job_name=self.config["ProcessingJobName"],
                    job_type="Processing",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        return {"Processing": serialize(self.hook.describe_processing_job(self.config["ProcessingJobName"]))}

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")
        else:
            self.log.info(event["message"])
        return {"Processing": serialize(self.hook.describe_processing_job(self.config["ProcessingJobName"]))}


class SageMakerEndpointConfigOperator(SageMakerBaseOperator):
    """
    Creates an endpoint configuration that Amazon SageMaker hosting
    services uses to deploy models. In the configuration, you identify
    one or more models, created using the CreateModel API, to deploy and
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
        aws_conn_id: str = DEFAULT_CONN_ID,
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
    When you create a serverless endpoint, SageMaker provisions and manages
    the compute resources for you. Then, you can make inference requests to
    the endpoint and receive model predictions in response. SageMaker scales
    the compute resources up and down as needed to handle your request traffic.

    Requires an Endpoint Config.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerEndpointOperator`

    :param config:
        The configuration necessary to create an endpoint.

        If you need to create a SageMaker endpoint based on an existed
        SageMaker model and an existed SageMaker endpoint config::

            config = endpoint_configuration;

        If you need to create all of SageMaker model, SageMaker endpoint-config and SageMaker endpoint::

            config = {
                'Model': model_configuration,
                'EndpointConfig': endpoint_config_configuration,
                'Endpoint': endpoint_configuration
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
    :return Dict: Returns The ARN of the endpoint created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_ingestion_time: int | None = None,
        operation: str = "create",
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.operation = operation.lower()
        if self.operation not in ["create", "update"]:
            raise ValueError('Invalid value! Argument operation has to be one of "create" and "update"')

    def _create_integer_fields(self) -> None:
        """Set fields which should be cast to integers."""
        if "EndpointConfig" in self.config:
            self.integer_fields: list[list[str]] = [
                ["EndpointConfig", "ProductionVariants", "InitialInstanceCount"]
            ]

    def expand_role(self) -> None:
        """Expands an IAM role name into an ARN."""
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
                wait_for_completion=self.wait_for_completion,
                check_interval=self.check_interval,
                max_ingestion_time=self.max_ingestion_time,
            )
        except ClientError:
            self.operation = "update"
            sagemaker_operation = self.hook.update_endpoint
            log_str = "Updating"
            response = sagemaker_operation(
                endpoint_info,
                wait_for_completion=self.wait_for_completion,
                check_interval=self.check_interval,
                max_ingestion_time=self.max_ingestion_time,
            )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker endpoint creation failed: {response}")
        else:
            return {
                "EndpointConfig": serialize(
                    self.hook.describe_endpoint_config(endpoint_info["EndpointConfigName"])
                ),
                "Endpoint": serialize(self.hook.describe_endpoint(endpoint_info["EndpointName"])),
            }


class SageMakerTransformOperator(SageMakerBaseOperator):
    """
    Starts a transform job. A transform job uses a trained model to get inferences
    on a dataset and saves these results to an Amazon S3 location that you specify.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerTransformOperator`

    :param config: The configuration necessary to start a transform job (templated).

        If you need to create a SageMaker transform job based on an existed SageMaker model::

            config = transform_config

        If you need to create both SageMaker model and SageMaker Transform job::

            config = {
                'Model': model_config,
                'Transform': transform_config
            }

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
        aws_conn_id: str = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "timestamp",
        deferrable: bool = False,
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
        self.deferrable = deferrable

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
        """Expands an IAM role name into an ARN."""
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
            self.defer(
                timeout=self.execution_timeout,
                trigger=SageMakerTrigger(
                    job_name=transform_config["TransformJobName"],
                    job_type="Transform",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        return {
            "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
            "Transform": serialize(self.hook.describe_transform_job(transform_config["TransformJobName"])),
        }

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")
        else:
            self.log.info(event["message"])
        transform_config = self.config.get("Transform", self.config)
        return {
            "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
            "Transform": serialize(self.hook.describe_transform_job(transform_config["TransformJobName"])),
        }


class SageMakerTuningOperator(SageMakerBaseOperator):
    """
    Starts a hyperparameter tuning job. A hyperparameter tuning job finds the
    best version of a model by running many training jobs on your dataset using
    the algorithm you choose and values for hyperparameters within ranges that
    you specify. It then chooses the hyperparameter values that result in a model
    that performs the best, as measured by an objective metric that you choose.

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
    :return Dict: Returns The ARN of the tuning job created in Amazon SageMaker.
    """

    def __init__(
        self,
        *,
        config: dict,
        aws_conn_id: str = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_ingestion_time: int | None = None,
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def expand_role(self) -> None:
        """Expands an IAM role name into an ARN."""
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
            "Creating SageMaker Hyper-Parameter Tuning Job %s", self.config["HyperParameterTuningJobName"]
        )
        response = self.hook.create_tuning_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Tuning Job creation failed: {response}")
        else:
            return {
                "Tuning": serialize(self.hook.describe_tuning_job(self.config["HyperParameterTuningJobName"]))
            }


class SageMakerModelOperator(SageMakerBaseOperator):
    """
    Creates a model in Amazon SageMaker. In the request, you name the model and
    describe a primary container. For the primary container, you specify the Docker
    image that contains inference code, artifacts (from prior training), and a custom
    environment map that the inference code uses when you deploy the model for predictions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerModelOperator`

    :param config: The configuration necessary to create a model.

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_model`
    :param aws_conn_id: The AWS connection ID to use.
    :return Dict: Returns The ARN of the model created in Amazon SageMaker.
    """

    def __init__(self, *, config: dict, aws_conn_id: str = DEFAULT_CONN_ID, **kwargs):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)

    def expand_role(self) -> None:
        """Expands an IAM role name into an ARN."""
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
    Starts a model training job. After training completes, Amazon SageMaker saves
    the resulting model artifacts to an Amazon S3 location that you specify.

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
        aws_conn_id: str = DEFAULT_CONN_ID,
        wait_for_completion: bool = True,
        print_log: bool = True,
        check_interval: int = CHECK_INTERVAL_SECOND,
        max_attempts: int | None = None,
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "timestamp",
        deferrable: bool = False,
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

    def expand_role(self) -> None:
        """Expands an IAM role name into an ARN."""
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
            self.defer(
                timeout=self.execution_timeout,
                trigger=SageMakerTrigger(
                    job_name=self.config["TrainingJobName"],
                    job_type="Training",
                    poke_interval=self.check_interval,
                    max_attempts=self.max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        result = {"Training": serialize(self.hook.describe_training_job(self.config["TrainingJobName"]))}
        return result

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")
        else:
            self.log.info(event["message"])
        result = {"Training": serialize(self.hook.describe_training_job(self.config["TrainingJobName"]))}
        return result


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

    def __init__(self, *, config: dict, aws_conn_id: str = DEFAULT_CONN_ID, **kwargs):
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
    :param verbose: Whether to print steps details when waiting for completion.
        Defaults to true, consider turning off for pipelines that have thousands of steps.

    :return str: Returns The ARN of the pipeline execution created in Amazon SageMaker.
    """

    template_fields: Sequence[str] = ("aws_conn_id", "pipeline_name", "display_name", "pipeline_params")

    def __init__(
        self,
        *,
        aws_conn_id: str = DEFAULT_CONN_ID,
        pipeline_name: str,
        display_name: str = "airflow-triggered-execution",
        pipeline_params: dict | None = None,
        wait_for_completion: bool = False,
        check_interval: int = CHECK_INTERVAL_SECOND,
        verbose: bool = True,
        **kwargs,
    ):
        super().__init__(config={}, aws_conn_id=aws_conn_id, **kwargs)
        self.pipeline_name = pipeline_name
        self.display_name = display_name
        self.pipeline_params = pipeline_params
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.verbose = verbose

    def execute(self, context: Context) -> str:
        arn = self.hook.start_pipeline(
            pipeline_name=self.pipeline_name,
            display_name=self.display_name,
            pipeline_params=self.pipeline_params,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            verbose=self.verbose,
        )
        self.log.info(
            "Starting a new execution for pipeline %s, running with ARN %s", self.pipeline_name, arn
        )
        return arn


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

    :return str: Returns the status of the pipeline execution after the operation has been done.
    """

    template_fields: Sequence[str] = (
        "aws_conn_id",
        "pipeline_exec_arn",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str = DEFAULT_CONN_ID,
        pipeline_exec_arn: str,
        wait_for_completion: bool = False,
        check_interval: int = CHECK_INTERVAL_SECOND,
        verbose: bool = True,
        fail_if_not_running: bool = False,
        **kwargs,
    ):
        super().__init__(config={}, aws_conn_id=aws_conn_id, **kwargs)
        self.pipeline_exec_arn = pipeline_exec_arn
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.verbose = verbose
        self.fail_if_not_running = fail_if_not_running

    def execute(self, context: Context) -> str:
        status = self.hook.stop_pipeline(
            pipeline_exec_arn=self.pipeline_exec_arn,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            verbose=self.verbose,
            fail_if_not_running=self.fail_if_not_running,
        )
        self.log.info(
            "Stop requested for pipeline execution with ARN %s. Status is now %s",
            self.pipeline_exec_arn,
            status,
        )
        return status


class SageMakerRegisterModelVersionOperator(SageMakerBaseOperator):
    """
    Registers an Amazon SageMaker model by creating a model version that specifies the model group to which it
    belongs. Will create the model group if it does not exist already.

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
        aws_conn_id: str = DEFAULT_CONN_ID,
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
        aws_conn_id: str = DEFAULT_CONN_ID,
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
        aws_conn_id: str = DEFAULT_CONN_ID,
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
