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
from typing import TYPE_CHECKING, Any, Sequence

from botocore.exceptions import ClientError

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
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
    :param max_ingestion_time: If wait is set to True, the operation fails if the processing job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
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
        max_ingestion_time: int | None = None,
        action_if_job_exists: str = "increment",
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        if action_if_job_exists not in ("increment", "fail"):
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )
        self.action_if_job_exists = action_if_job_exists
        self.wait_for_completion = wait_for_completion
        self.print_log = print_log
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

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
        processing_job_name = self.config["ProcessingJobName"]
        processing_job_dedupe_pattern = "-[0-9]+$"
        existing_jobs_found = self.hook.count_processing_jobs_by_name(
            processing_job_name, processing_job_dedupe_pattern
        )
        if existing_jobs_found:
            if self.action_if_job_exists == "fail":
                raise AirflowException(
                    f"A SageMaker processing job with name {processing_job_name} already exists."
                )
            elif self.action_if_job_exists == "increment":
                self.log.info("Found existing processing job with name '%s'.", processing_job_name)
                new_processing_job_name = f"{processing_job_name}-{existing_jobs_found + 1}"
                self.config["ProcessingJobName"] = new_processing_job_name
                self.log.info("Incremented processing job name to '%s'.", new_processing_job_name)

        response = self.hook.create_processing_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Processing Job creation failed: {response}")
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
    :param max_ingestion_time: If wait is set to True, the operation fails
        if the transform job doesn't finish within max_ingestion_time seconds. If you
        set this parameter to None, the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a transform job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
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
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "increment",
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.check_if_job_exists = check_if_job_exists
        if action_if_job_exists in ("increment", "fail"):
            self.action_if_job_exists = action_if_job_exists
        else:
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )

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
        model_config = self.config.get("Model")
        transform_config = self.config.get("Transform", self.config)
        if self.check_if_job_exists:
            self._check_if_transform_job_exists()
        if model_config:
            self.log.info("Creating SageMaker Model %s for transform job", model_config["ModelName"])
            self.hook.create_model(model_config)
        self.log.info("Creating SageMaker transform Job %s.", transform_config["TransformJobName"])
        response = self.hook.create_transform_job(
            transform_config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker transform Job creation failed: {response}")
        else:
            return {
                "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
                "Transform": serialize(
                    self.hook.describe_transform_job(transform_config["TransformJobName"])
                ),
            }

    def _check_if_transform_job_exists(self) -> None:
        transform_config = self.config.get("Transform", self.config)
        transform_job_name = transform_config["TransformJobName"]
        transform_jobs = self.hook.list_transform_jobs(name_contains=transform_job_name)
        if transform_job_name in [tj["TransformJobName"] for tj in transform_jobs]:
            if self.action_if_job_exists == "increment":
                self.log.info("Found existing transform job with name '%s'.", transform_job_name)
                new_transform_job_name = f"{transform_job_name}-{(len(transform_jobs) + 1)}"
                transform_config["TransformJobName"] = new_transform_job_name
                self.log.info("Incremented transform job name to '%s'.", new_transform_job_name)
            elif self.action_if_job_exists == "fail":
                raise AirflowException(
                    f"A SageMaker transform job with name {transform_job_name} already exists."
                )


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
    :param max_ingestion_time: If wait is set to True, the operation fails if the training job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a training job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
        This is only relevant if check_if_job_exists is True.
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
        max_ingestion_time: int | None = None,
        check_if_job_exists: bool = True,
        action_if_job_exists: str = "increment",
        **kwargs,
    ):
        super().__init__(config=config, aws_conn_id=aws_conn_id, **kwargs)
        self.wait_for_completion = wait_for_completion
        self.print_log = print_log
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.check_if_job_exists = check_if_job_exists
        if action_if_job_exists in ("increment", "fail"):
            self.action_if_job_exists = action_if_job_exists
        else:
            raise AirflowException(
                f"Argument action_if_job_exists accepts only 'increment' and 'fail'. \
                Provided value: '{action_if_job_exists}'."
            )

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
            self._check_if_job_exists()
        self.log.info("Creating SageMaker training job %s.", self.config["TrainingJobName"])
        response = self.hook.create_training_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            print_log=self.print_log,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Training Job creation failed: {response}")
        else:
            return {"Training": serialize(self.hook.describe_training_job(self.config["TrainingJobName"]))}

    def _check_if_job_exists(self) -> None:
        training_job_name = self.config["TrainingJobName"]
        training_jobs = self.hook.list_training_jobs(name_contains=training_job_name)
        if training_job_name in [tj["TrainingJobName"] for tj in training_jobs]:
            if self.action_if_job_exists == "increment":
                self.log.info("Found existing training job with name '%s'.", training_job_name)
                new_training_job_name = f"{training_job_name}-{(len(training_jobs) + 1)}"
                self.config["TrainingJobName"] = new_training_job_name
                self.log.info("Incremented training job name to '%s'.", new_training_job_name)
            elif self.action_if_job_exists == "fail":
                raise AirflowException(
                    f"A SageMaker training job with name {training_job_name} already exists."
                )


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
