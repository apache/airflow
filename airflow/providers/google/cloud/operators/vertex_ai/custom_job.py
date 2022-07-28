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
"""This module contains Google Vertex AI operators."""

from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.aiplatform.models import Model
from google.cloud.aiplatform_v1.types.dataset import Dataset
from google.cloud.aiplatform_v1.types.training_pipeline import TrainingPipeline

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.vertex_ai.custom_job import CustomJobHook
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIModelLink,
    VertexAITrainingLink,
    VertexAITrainingPipelinesLink,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CustomTrainingJobBaseOperator(BaseOperator):
    """The base class for operators that launch Custom jobs on VertexAI."""

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        display_name: str,
        container_uri: str,
        model_serving_container_image_uri: Optional[str] = None,
        model_serving_container_predict_route: Optional[str] = None,
        model_serving_container_health_route: Optional[str] = None,
        model_serving_container_command: Optional[Sequence[str]] = None,
        model_serving_container_args: Optional[Sequence[str]] = None,
        model_serving_container_environment_variables: Optional[Dict[str, str]] = None,
        model_serving_container_ports: Optional[Sequence[int]] = None,
        model_description: Optional[str] = None,
        model_instance_schema_uri: Optional[str] = None,
        model_parameters_schema_uri: Optional[str] = None,
        model_prediction_schema_uri: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        training_encryption_spec_key_name: Optional[str] = None,
        model_encryption_spec_key_name: Optional[str] = None,
        staging_bucket: Optional[str] = None,
        # RUN
        dataset_id: Optional[str] = None,
        annotation_schema_uri: Optional[str] = None,
        model_display_name: Optional[str] = None,
        model_labels: Optional[Dict[str, str]] = None,
        base_output_dir: Optional[str] = None,
        service_account: Optional[str] = None,
        network: Optional[str] = None,
        bigquery_destination: Optional[str] = None,
        args: Optional[List[Union[str, float, int]]] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
        training_fraction_split: Optional[float] = None,
        validation_fraction_split: Optional[float] = None,
        test_fraction_split: Optional[float] = None,
        training_filter_split: Optional[str] = None,
        validation_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        predefined_split_column_name: Optional[str] = None,
        timestamp_split_column_name: Optional[str] = None,
        tensorboard: Optional[str] = None,
        sync=True,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.display_name = display_name
        # START Custom
        self.container_uri = container_uri
        self.model_serving_container_image_uri = model_serving_container_image_uri
        self.model_serving_container_predict_route = model_serving_container_predict_route
        self.model_serving_container_health_route = model_serving_container_health_route
        self.model_serving_container_command = model_serving_container_command
        self.model_serving_container_args = model_serving_container_args
        self.model_serving_container_environment_variables = model_serving_container_environment_variables
        self.model_serving_container_ports = model_serving_container_ports
        self.model_description = model_description
        self.model_instance_schema_uri = model_instance_schema_uri
        self.model_parameters_schema_uri = model_parameters_schema_uri
        self.model_prediction_schema_uri = model_prediction_schema_uri
        self.labels = labels
        self.training_encryption_spec_key_name = training_encryption_spec_key_name
        self.model_encryption_spec_key_name = model_encryption_spec_key_name
        self.staging_bucket = staging_bucket
        # END Custom
        # START Run param
        self.dataset = Dataset(name=dataset_id) if dataset_id else None
        self.annotation_schema_uri = annotation_schema_uri
        self.model_display_name = model_display_name
        self.model_labels = model_labels
        self.base_output_dir = base_output_dir
        self.service_account = service_account
        self.network = network
        self.bigquery_destination = bigquery_destination
        self.args = args
        self.environment_variables = environment_variables
        self.replica_count = replica_count
        self.machine_type = machine_type
        self.accelerator_type = accelerator_type
        self.accelerator_count = accelerator_count
        self.boot_disk_type = boot_disk_type
        self.boot_disk_size_gb = boot_disk_size_gb
        self.training_fraction_split = training_fraction_split
        self.validation_fraction_split = validation_fraction_split
        self.test_fraction_split = test_fraction_split
        self.training_filter_split = training_filter_split
        self.validation_filter_split = validation_filter_split
        self.test_filter_split = test_filter_split
        self.predefined_split_column_name = predefined_split_column_name
        self.timestamp_split_column_name = timestamp_split_column_name
        self.tensorboard = tensorboard
        self.sync = sync
        # END Run param
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain


class CreateCustomContainerTrainingJobOperator(CustomTrainingJobBaseOperator):
    """Create Custom Container Training job

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param display_name: Required. The user-defined name of this TrainingPipeline.
    :param command: The command to be invoked when the container is started.
        It overrides the entrypoint instruction in Dockerfile when provided
    :param container_uri: Required: Uri of the training container image in the GCR.
    :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
        of the Model serving container suitable for serving the model produced by the
        training script.
    :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
        HTTP path to send prediction requests to the container, and which must be supported
        by it. If not specified a default HTTP path will be used by Vertex AI.
    :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
        HTTP path to send health check requests to the container, and which must be supported
        by it. If not specified a standard HTTP path will be used by AI Platform.
    :param model_serving_container_command: The command with which the container is run. Not executed
        within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
        Variable references $(VAR_NAME) are expanded using the container's
        environment. If a variable cannot be resolved, the reference in the
        input string will be unchanged. The $(VAR_NAME) syntax can be escaped
        with a double $$, ie: $$(VAR_NAME). Escaped references will never be
        expanded, regardless of whether the variable exists or not.
    :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
        this is not provided. Variable references $(VAR_NAME) are expanded using the
        container's environment. If a variable cannot be resolved, the reference
        in the input string will be unchanged. The $(VAR_NAME) syntax can be
        escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
        never be expanded, regardless of whether the variable exists or not.
    :param model_serving_container_environment_variables: The environment variables that are to be
        present in the container. Should be a dictionary where keys are environment variable names
        and values are environment variable values for those names.
    :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
        field is primarily informational, it gives Vertex AI information about the
        network connections the container uses. Listing or not a port here has
        no impact on whether the port is actually exposed, any port listening on
        the default "0.0.0.0" address inside a container will be accessible from
        the network.
    :param model_description: The description of the Model.
    :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single instance, which
            are used in
            ``PredictRequest.instances``,
            ``ExplainRequest.instances``
            and
            ``BatchPredictionJob.input_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the parameters of prediction and
            explanation via
            ``PredictRequest.parameters``,
            ``ExplainRequest.parameters``
            and
            ``BatchPredictionJob.model_parameters``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform, if no parameters are supported it is set to an
            empty string. Note: The URI given on output will be
            immutable and probably different, including the URI scheme,
            than the one given on input. The output URI will point to a
            location where the user only has a read access.
    :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single prediction
            produced by this Model, which are returned via
            ``PredictResponse.predictions``,
            ``ExplainResponse.explanations``,
            and
            ``BatchPredictionJob.output_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param project_id: Project to run training in.
    :param region: Location to run training in.
    :param labels: Optional. The labels with user-defined metadata to
            organize TrainingPipelines.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, this TrainingPipeline will be secured by this key.

            Note: Model trained by this TrainingPipeline is also secured
            by this key if ``model_to_upload`` is not set separately.
    :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, the trained Model will be secured by this key.
    :param staging_bucket: Bucket used to stage source and training artifacts.
    :param dataset: Vertex AI to fit this training against.
    :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
        annotation schema. The schema is defined as an OpenAPI 3.0.2
        [Schema Object]
        (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

        Only Annotations that both match this schema and belong to
        DataItems not ignored by the split method are used in
        respectively training, validation or test role, depending on
        the role of the DataItem they are on.

        When used in conjunction with
        ``annotations_filter``,
        the Annotations used for training are filtered by both
        ``annotations_filter``
        and
        ``annotation_schema_uri``.
    :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
            the Model. The name can be up to 128 characters long and can be consist
            of any UTF-8 characters.

            If not provided upon creation, the job's display_name is used.
    :param model_labels: Optional. The labels with user-defined metadata to
            organize your Models.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
        staging directory will be used.

        Vertex AI sets the following environment variables when it runs your training code:

        -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
            i.e. <base_output_dir>/model/
        -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
            i.e. <base_output_dir>/checkpoints/
        -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
            logs, i.e. <base_output_dir>/logs/
    :param service_account: Specifies the service account for workload run-as account.
            Users submitting jobs must have act-as permission on this run-as account.
    :param network: The full name of the Compute Engine network to which the job
            should be peered.
            Private services access must already be configured for the network.
            If left unspecified, the job is not peered with any network.
    :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
            The BigQuery project location where the training data is to
            be written to. In the given project a new dataset is created
            with name
            ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
            where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
            training input data will be written into that dataset. In
            the dataset three tables will be created, ``training``,
            ``validation`` and ``test``.

            -  AIP_DATA_FORMAT = "bigquery".
            -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
            -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
            -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
    :param args: Command line arguments to be passed to the Python script.
    :param environment_variables: Environment variables to be passed to the container.
            Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
            At most 10 environment variables can be specified.
            The Name of the environment variable must be unique.
    :param replica_count: The number of worker replicas. If replica count = 1 then one chief
            replica will be provisioned. If replica_count > 1 the remainder will be
            provisioned as a worker replica pool.
    :param machine_type: The type of machine to use for training.
    :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
            NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
            NVIDIA_TESLA_T4
    :param accelerator_count: The number of accelerators to attach to a worker replica.
    :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
            Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
            `pd-standard` (Persistent Disk Hard Disk Drive).
    :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
            boot disk size must be within the range of [100, 64000].
    :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
    :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
        validate the Model. This is ignored if Dataset is not provided.
    :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
    :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to validate the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to test the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key (either the label's value or
            value in the column) must be one of {``training``,
            ``validation``, ``test``}, and it defines to which set the
            given piece of data is assigned. If for a piece of data the
            key is not present or has an invalid value, that piece is
            ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key values of the key (the values in
            the column) must be in RFC 3339 `date-time` format, where
            `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
            piece of data the key is not present or has an invalid value,
            that piece is ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
            logs. Format:
            ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
            For more information on configuring your service account please visit:
            https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
    :param sync: Whether to execute the AI Platform job synchronously. If False, this method
            will be executed in concurrent Future and any downstream object will
            be immediately returned and synced when the Future has completed.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        'region',
        'command',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(), VertexAITrainingLink())

    def __init__(
        self,
        *,
        command: Sequence[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command

    def execute(self, context: "Context"):
        self.hook = CustomJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model, training_id = self.hook.create_custom_container_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            container_uri=self.container_uri,
            command=self.command,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )

        if model:
            result = Model.to_dict(model)
            model_id = self.hook.extract_model_id(result)
            VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        else:
            result = model  # type: ignore
        VertexAITrainingLink.persist(context=context, task_instance=self, training_id=training_id)
        return result

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_job()


class CreateCustomPythonPackageTrainingJobOperator(CustomTrainingJobBaseOperator):
    """Create Custom Python Package Training job

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param display_name: Required. The user-defined name of this TrainingPipeline.
    :param python_package_gcs_uri: Required: GCS location of the training python package.
    :param python_module_name: Required: The module name of the training python package.
    :param container_uri: Required: Uri of the training container image in the GCR.
    :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
        of the Model serving container suitable for serving the model produced by the
        training script.
    :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
        HTTP path to send prediction requests to the container, and which must be supported
        by it. If not specified a default HTTP path will be used by Vertex AI.
    :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
        HTTP path to send health check requests to the container, and which must be supported
        by it. If not specified a standard HTTP path will be used by AI Platform.
    :param model_serving_container_command: The command with which the container is run. Not executed
        within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
        Variable references $(VAR_NAME) are expanded using the container's
        environment. If a variable cannot be resolved, the reference in the
        input string will be unchanged. The $(VAR_NAME) syntax can be escaped
        with a double $$, ie: $$(VAR_NAME). Escaped references will never be
        expanded, regardless of whether the variable exists or not.
    :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
        this is not provided. Variable references $(VAR_NAME) are expanded using the
        container's environment. If a variable cannot be resolved, the reference
        in the input string will be unchanged. The $(VAR_NAME) syntax can be
        escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
        never be expanded, regardless of whether the variable exists or not.
    :param model_serving_container_environment_variables: The environment variables that are to be
        present in the container. Should be a dictionary where keys are environment variable names
        and values are environment variable values for those names.
    :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
        field is primarily informational, it gives Vertex AI information about the
        network connections the container uses. Listing or not a port here has
        no impact on whether the port is actually exposed, any port listening on
        the default "0.0.0.0" address inside a container will be accessible from
        the network.
    :param model_description: The description of the Model.
    :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single instance, which
            are used in
            ``PredictRequest.instances``,
            ``ExplainRequest.instances``
            and
            ``BatchPredictionJob.input_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the parameters of prediction and
            explanation via
            ``PredictRequest.parameters``,
            ``ExplainRequest.parameters``
            and
            ``BatchPredictionJob.model_parameters``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform, if no parameters are supported it is set to an
            empty string. Note: The URI given on output will be
            immutable and probably different, including the URI scheme,
            than the one given on input. The output URI will point to a
            location where the user only has a read access.
    :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single prediction
            produced by this Model, which are returned via
            ``PredictResponse.predictions``,
            ``ExplainResponse.explanations``,
            and
            ``BatchPredictionJob.output_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param project_id: Project to run training in.
    :param region: Location to run training in.
    :param labels: Optional. The labels with user-defined metadata to
            organize TrainingPipelines.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, this TrainingPipeline will be secured by this key.

            Note: Model trained by this TrainingPipeline is also secured
            by this key if ``model_to_upload`` is not set separately.
    :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, the trained Model will be secured by this key.
    :param staging_bucket: Bucket used to stage source and training artifacts.
    :param dataset: Vertex AI to fit this training against.
    :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
        annotation schema. The schema is defined as an OpenAPI 3.0.2
        [Schema Object]
        (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

        Only Annotations that both match this schema and belong to
        DataItems not ignored by the split method are used in
        respectively training, validation or test role, depending on
        the role of the DataItem they are on.

        When used in conjunction with
        ``annotations_filter``,
        the Annotations used for training are filtered by both
        ``annotations_filter``
        and
        ``annotation_schema_uri``.
    :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
            the Model. The name can be up to 128 characters long and can be consist
            of any UTF-8 characters.

            If not provided upon creation, the job's display_name is used.
    :param model_labels: Optional. The labels with user-defined metadata to
            organize your Models.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
        staging directory will be used.

        Vertex AI sets the following environment variables when it runs your training code:

        -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
            i.e. <base_output_dir>/model/
        -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
            i.e. <base_output_dir>/checkpoints/
        -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
            logs, i.e. <base_output_dir>/logs/
    :param service_account: Specifies the service account for workload run-as account.
            Users submitting jobs must have act-as permission on this run-as account.
    :param network: The full name of the Compute Engine network to which the job
            should be peered.
            Private services access must already be configured for the network.
            If left unspecified, the job is not peered with any network.
    :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
            The BigQuery project location where the training data is to
            be written to. In the given project a new dataset is created
            with name
            ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
            where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
            training input data will be written into that dataset. In
            the dataset three tables will be created, ``training``,
            ``validation`` and ``test``.

            -  AIP_DATA_FORMAT = "bigquery".
            -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
            -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
            -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
    :param args: Command line arguments to be passed to the Python script.
    :param environment_variables: Environment variables to be passed to the container.
            Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
            At most 10 environment variables can be specified.
            The Name of the environment variable must be unique.
    :param replica_count: The number of worker replicas. If replica count = 1 then one chief
            replica will be provisioned. If replica_count > 1 the remainder will be
            provisioned as a worker replica pool.
    :param machine_type: The type of machine to use for training.
    :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
            NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
            NVIDIA_TESLA_T4
    :param accelerator_count: The number of accelerators to attach to a worker replica.
    :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
            Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
            `pd-standard` (Persistent Disk Hard Disk Drive).
    :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
            boot disk size must be within the range of [100, 64000].
    :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
    :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
        validate the Model. This is ignored if Dataset is not provided.
    :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
    :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to validate the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to test the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key (either the label's value or
            value in the column) must be one of {``training``,
            ``validation``, ``test``}, and it defines to which set the
            given piece of data is assigned. If for a piece of data the
            key is not present or has an invalid value, that piece is
            ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key values of the key (the values in
            the column) must be in RFC 3339 `date-time` format, where
            `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
            piece of data the key is not present or has an invalid value,
            that piece is ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
            logs. Format:
            ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
            For more information on configuring your service account please visit:
            https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
    :param sync: Whether to execute the AI Platform job synchronously. If False, this method
            will be executed in concurrent Future and any downstream object will
            be immediately returned and synced when the Future has completed.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        'region',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(), VertexAITrainingLink())

    def __init__(
        self,
        *,
        python_package_gcs_uri: str,
        python_module_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.python_package_gcs_uri = python_package_gcs_uri
        self.python_module_name = python_module_name

    def execute(self, context: "Context"):
        self.hook = CustomJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model, training_id = self.hook.create_custom_python_package_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            python_package_gcs_uri=self.python_package_gcs_uri,
            python_module_name=self.python_module_name,
            container_uri=self.container_uri,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )

        if model:
            result = Model.to_dict(model)
            model_id = self.hook.extract_model_id(result)
            VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        else:
            result = model  # type: ignore
        VertexAITrainingLink.persist(context=context, task_instance=self, training_id=training_id)
        return result

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_job()


class CreateCustomTrainingJobOperator(CustomTrainingJobBaseOperator):
    """Create Custom Training job

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param display_name: Required. The user-defined name of this TrainingPipeline.
    :param script_path: Required. Local path to training script.
    :param container_uri: Required: Uri of the training container image in the GCR.
    :param requirements: List of python packages dependencies of script.
    :param model_serving_container_image_uri: If the training produces a managed Vertex AI Model, the URI
        of the Model serving container suitable for serving the model produced by the
        training script.
    :param model_serving_container_predict_route: If the training produces a managed Vertex AI Model, An
        HTTP path to send prediction requests to the container, and which must be supported
        by it. If not specified a default HTTP path will be used by Vertex AI.
    :param model_serving_container_health_route: If the training produces a managed Vertex AI Model, an
        HTTP path to send health check requests to the container, and which must be supported
        by it. If not specified a standard HTTP path will be used by AI Platform.
    :param model_serving_container_command: The command with which the container is run. Not executed
        within a shell. The Docker image's ENTRYPOINT is used if this is not provided.
        Variable references $(VAR_NAME) are expanded using the container's
        environment. If a variable cannot be resolved, the reference in the
        input string will be unchanged. The $(VAR_NAME) syntax can be escaped
        with a double $$, ie: $$(VAR_NAME). Escaped references will never be
        expanded, regardless of whether the variable exists or not.
    :param model_serving_container_args: The arguments to the command. The Docker image's CMD is used if
        this is not provided. Variable references $(VAR_NAME) are expanded using the
        container's environment. If a variable cannot be resolved, the reference
        in the input string will be unchanged. The $(VAR_NAME) syntax can be
        escaped with a double $$, ie: $$(VAR_NAME). Escaped references will
        never be expanded, regardless of whether the variable exists or not.
    :param model_serving_container_environment_variables: The environment variables that are to be
        present in the container. Should be a dictionary where keys are environment variable names
        and values are environment variable values for those names.
    :param model_serving_container_ports: Declaration of ports that are exposed by the container. This
        field is primarily informational, it gives Vertex AI information about the
        network connections the container uses. Listing or not a port here has
        no impact on whether the port is actually exposed, any port listening on
        the default "0.0.0.0" address inside a container will be accessible from
        the network.
    :param model_description: The description of the Model.
    :param model_instance_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single instance, which
            are used in
            ``PredictRequest.instances``,
            ``ExplainRequest.instances``
            and
            ``BatchPredictionJob.input_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param model_parameters_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the parameters of prediction and
            explanation via
            ``PredictRequest.parameters``,
            ``ExplainRequest.parameters``
            and
            ``BatchPredictionJob.model_parameters``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform, if no parameters are supported it is set to an
            empty string. Note: The URI given on output will be
            immutable and probably different, including the URI scheme,
            than the one given on input. The output URI will point to a
            location where the user only has a read access.
    :param model_prediction_schema_uri: Optional. Points to a YAML file stored on Google Cloud
            Storage describing the format of a single prediction
            produced by this Model, which are returned via
            ``PredictResponse.predictions``,
            ``ExplainResponse.explanations``,
            and
            ``BatchPredictionJob.output_config``.
            The schema is defined as an OpenAPI 3.0.2 `Schema
            Object <https://tinyurl.com/y538mdwt#schema-object>`__.
            AutoML Models always have this field populated by AI
            Platform. Note: The URI given on output will be immutable
            and probably different, including the URI scheme, than the
            one given on input. The output URI will point to a location
            where the user only has a read access.
    :param project_id: Project to run training in.
    :param region: Location to run training in.
    :param labels: Optional. The labels with user-defined metadata to
            organize TrainingPipelines.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param training_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the training pipeline. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, this TrainingPipeline will be secured by this key.

            Note: Model trained by this TrainingPipeline is also secured
            by this key if ``model_to_upload`` is not set separately.
    :param model_encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer
            managed encryption key used to protect the model. Has the
            form:
            ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
            The key needs to be in the same region as where the compute
            resource is created.

            If set, the trained Model will be secured by this key.
    :param staging_bucket: Bucket used to stage source and training artifacts.
    :param dataset: Vertex AI to fit this training against.
    :param annotation_schema_uri: Google Cloud Storage URI points to a YAML file describing
        annotation schema. The schema is defined as an OpenAPI 3.0.2
        [Schema Object]
        (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)

        Only Annotations that both match this schema and belong to
        DataItems not ignored by the split method are used in
        respectively training, validation or test role, depending on
        the role of the DataItem they are on.

        When used in conjunction with
        ``annotations_filter``,
        the Annotations used for training are filtered by both
        ``annotations_filter``
        and
        ``annotation_schema_uri``.
    :param model_display_name: If the script produces a managed Vertex AI Model. The display name of
            the Model. The name can be up to 128 characters long and can be consist
            of any UTF-8 characters.

            If not provided upon creation, the job's display_name is used.
    :param model_labels: Optional. The labels with user-defined metadata to
            organize your Models.
            Label keys and values can be no longer than 64
            characters, can only
            contain lowercase letters, numeric characters,
            underscores and dashes. International characters
            are allowed.
            See https://goo.gl/xmQnxf for more information
            and examples of labels.
    :param base_output_dir: GCS output directory of job. If not provided a timestamped directory in the
        staging directory will be used.

        Vertex AI sets the following environment variables when it runs your training code:

        -  AIP_MODEL_DIR: a Cloud Storage URI of a directory intended for saving model artifacts,
            i.e. <base_output_dir>/model/
        -  AIP_CHECKPOINT_DIR: a Cloud Storage URI of a directory intended for saving checkpoints,
            i.e. <base_output_dir>/checkpoints/
        -  AIP_TENSORBOARD_LOG_DIR: a Cloud Storage URI of a directory intended for saving TensorBoard
            logs, i.e. <base_output_dir>/logs/
    :param service_account: Specifies the service account for workload run-as account.
            Users submitting jobs must have act-as permission on this run-as account.
    :param network: The full name of the Compute Engine network to which the job
            should be peered.
            Private services access must already be configured for the network.
            If left unspecified, the job is not peered with any network.
    :param bigquery_destination: Provide this field if `dataset` is a BiqQuery dataset.
            The BigQuery project location where the training data is to
            be written to. In the given project a new dataset is created
            with name
            ``dataset_<dataset-id>_<annotation-type>_<timestamp-of-training-call>``
            where timestamp is in YYYY_MM_DDThh_mm_ss_sssZ format. All
            training input data will be written into that dataset. In
            the dataset three tables will be created, ``training``,
            ``validation`` and ``test``.

            -  AIP_DATA_FORMAT = "bigquery".
            -  AIP_TRAINING_DATA_URI ="bigquery_destination.dataset_*.training"
            -  AIP_VALIDATION_DATA_URI = "bigquery_destination.dataset_*.validation"
            -  AIP_TEST_DATA_URI = "bigquery_destination.dataset_*.test"
    :param args: Command line arguments to be passed to the Python script.
    :param environment_variables: Environment variables to be passed to the container.
            Should be a dictionary where keys are environment variable names
            and values are environment variable values for those names.
            At most 10 environment variables can be specified.
            The Name of the environment variable must be unique.
    :param replica_count: The number of worker replicas. If replica count = 1 then one chief
            replica will be provisioned. If replica_count > 1 the remainder will be
            provisioned as a worker replica pool.
    :param machine_type: The type of machine to use for training.
    :param accelerator_type: Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
            NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
            NVIDIA_TESLA_T4
    :param accelerator_count: The number of accelerators to attach to a worker replica.
    :param boot_disk_type: Type of the boot disk, default is `pd-ssd`.
            Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
            `pd-standard` (Persistent Disk Hard Disk Drive).
    :param boot_disk_size_gb: Size in GB of the boot disk, default is 100GB.
            boot disk size must be within the range of [100, 64000].
    :param training_fraction_split: Optional. The fraction of the input data that is to be used to train
            the Model. This is ignored if Dataset is not provided.
    :param validation_fraction_split: Optional. The fraction of the input data that is to be used to
        validate the Model. This is ignored if Dataset is not provided.
    :param test_fraction_split: Optional. The fraction of the input data that is to be used to evaluate
            the Model. This is ignored if Dataset is not provided.
    :param training_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to train the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param validation_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to validate the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param test_filter_split: Optional. A filter on DataItems of the Dataset. DataItems that match
            this filter are used to test the Model. A filter with same syntax
            as the one used in DatasetService.ListDataItems may be used. If a
            single DataItem is matched by more than one of the FilterSplit filters,
            then it is assigned to the first set that applies to it in the training,
            validation, test order. This is ignored if Dataset is not provided.
    :param predefined_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key (either the label's value or
            value in the column) must be one of {``training``,
            ``validation``, ``test``}, and it defines to which set the
            given piece of data is assigned. If for a piece of data the
            key is not present or has an invalid value, that piece is
            ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param timestamp_split_column_name: Optional. The key is a name of one of the Dataset's data
            columns. The value of the key values of the key (the values in
            the column) must be in RFC 3339 `date-time` format, where
            `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
            piece of data the key is not present or has an invalid value,
            that piece is ignored by the pipeline.

            Supported only for tabular and time series Datasets.
    :param tensorboard: Optional. The name of a Vertex AI resource to which this CustomJob will upload
            logs. Format:
            ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``
            For more information on configuring your service account please visit:
            https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
    :param sync: Whether to execute the AI Platform job synchronously. If False, this method
            will be executed in concurrent Future and any downstream object will
            be immediately returned and synced when the Future has completed.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        'region',
        'script_path',
        'requirements',
        'impersonation_chain',
    ]
    operator_extra_links = (VertexAIModelLink(), VertexAITrainingLink())

    def __init__(
        self,
        *,
        script_path: str,
        requirements: Optional[Sequence[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.requirements = requirements
        self.script_path = script_path

    def execute(self, context: "Context"):
        self.hook = CustomJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        model, training_id = self.hook.create_custom_training_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            script_path=self.script_path,
            container_uri=self.container_uri,
            requirements=self.requirements,
            model_serving_container_image_uri=self.model_serving_container_image_uri,
            model_serving_container_predict_route=self.model_serving_container_predict_route,
            model_serving_container_health_route=self.model_serving_container_health_route,
            model_serving_container_command=self.model_serving_container_command,
            model_serving_container_args=self.model_serving_container_args,
            model_serving_container_environment_variables=self.model_serving_container_environment_variables,
            model_serving_container_ports=self.model_serving_container_ports,
            model_description=self.model_description,
            model_instance_schema_uri=self.model_instance_schema_uri,
            model_parameters_schema_uri=self.model_parameters_schema_uri,
            model_prediction_schema_uri=self.model_prediction_schema_uri,
            labels=self.labels,
            training_encryption_spec_key_name=self.training_encryption_spec_key_name,
            model_encryption_spec_key_name=self.model_encryption_spec_key_name,
            staging_bucket=self.staging_bucket,
            # RUN
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            bigquery_destination=self.bigquery_destination,
            args=self.args,
            environment_variables=self.environment_variables,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_type=self.accelerator_type,
            accelerator_count=self.accelerator_count,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            tensorboard=self.tensorboard,
            sync=True,
        )

        if model:
            result = Model.to_dict(model)
            model_id = self.hook.extract_model_id(result)
            VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        else:
            result = model  # type: ignore
        VertexAITrainingLink.persist(context=context, task_instance=self, training_id=training_id)
        return result

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.hook:
            self.hook.cancel_job()


class DeleteCustomTrainingJobOperator(BaseOperator):
    """Deletes a CustomTrainingJob, CustomPythonTrainingJob, or CustomContainerTrainingJob.

    :param training_pipeline_id: Required. The name of the TrainingPipeline resource to be deleted.
    :param custom_job_id: Required. The name of the CustomJob to delete.
    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        training_pipeline_id: str,
        custom_job_id: str,
        region: str,
        project_id: str,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.training_pipeline = training_pipeline_id
        self.custom_job = custom_job_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context"):
        hook = CustomJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Deleting custom training pipeline: %s", self.training_pipeline)
            training_pipeline_operation = hook.delete_training_pipeline(
                training_pipeline=self.training_pipeline,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=training_pipeline_operation)
            self.log.info("Training pipeline was deleted.")
        except NotFound:
            self.log.info("The Training Pipeline ID %s does not exist.", self.training_pipeline)
        try:
            self.log.info("Deleting custom job: %s", self.custom_job)
            custom_job_operation = hook.delete_custom_job(
                custom_job=self.custom_job,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=custom_job_operation)
            self.log.info("Custom job was deleted.")
        except NotFound:
            self.log.info("The Custom Job ID %s does not exist.", self.custom_job)


class ListCustomTrainingJobOperator(BaseOperator):
    """Lists CustomTrainingJob, CustomPythonTrainingJob, or CustomContainerTrainingJob in a Location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param filter: Optional. The standard list filter. Supported fields:

        -  ``display_name`` supports = and !=.

        -  ``state`` supports = and !=.

        Some examples of using the filter are:

        -  ``state="PIPELINE_STATE_SUCCEEDED" AND display_name="my_pipeline"``

        -  ``state="PIPELINE_STATE_RUNNING" OR display_name="my_pipeline"``

        -  ``NOT display_name="my_pipeline"``

        -  ``state="PIPELINE_STATE_FAILED"``
    :param page_size: Optional. The standard list page size.
    :param page_token: Optional. The standard list page token. Typically obtained via
        [ListTrainingPipelinesResponse.next_page_token][google.cloud.aiplatform.v1.ListTrainingPipelinesResponse.next_page_token]
        of the previous
        [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelines]
        call.
    :param read_mask: Optional. Mask specifying which fields to read.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        "region",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = [
        VertexAITrainingPipelinesLink(),
    ]

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        read_mask: Optional[str] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.read_mask = read_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context"):
        hook = CustomJobHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_training_pipelines(
            region=self.region,
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            read_mask=self.read_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        VertexAITrainingPipelinesLink.persist(context=context, task_instance=self)
        return [TrainingPipeline.to_dict(result) for result in results]
