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

from typing import Dict, List, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.aiplatform import datasets, initializer, schema, utils
from google.cloud.aiplatform.utils import _timestamped_gcs_dir, worker_spec_utils
from google.cloud.aiplatform_v1.types import (
    BigQueryDestination,
    CancelPipelineJobRequest,
    CancelTrainingPipelineRequest,
    CreatePipelineJobRequest,
    CreateTrainingPipelineRequest,
    DeletePipelineJobRequest,
    DeleteTrainingPipelineRequest,
    EnvVar,
    FilterSplit,
    FractionSplit,
    GcsDestination,
    GetPipelineJobRequest,
    GetTrainingPipelineRequest,
    InputDataConfig,
    ListPipelineJobsRequest,
    ListTrainingPipelinesRequest,
    Model,
    ModelContainerSpec,
    PipelineJob,
    Port,
    PredefinedSplit,
    PredictSchemata,
    TimestampSplit,
    TrainingPipeline,
)

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.vertex_ai import VertexAIHook


class VertexAITrainingJobBaseOperator(BaseOperator):
    """The base class for operators that launch job on VertexAI."""

    def __init__(
        self,
        *,
        region: str = None,
        project_id: str,
        display_name: str,
        # START Run param
        dataset: Optional[
            Union[
                datasets.ImageDataset,
                datasets.TabularDataset,
                datasets.TextDataset,
                datasets.VideoDataset,
            ]
        ] = None,
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
        # END Run param
        # START Custom
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
        # END Custom
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.display_name = display_name
        # START Run param
        self.dataset = dataset
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
        # TODO: add optional and important parameters
        # END Run param
        # START Custom
        self._container_uri = container_uri

        model_predict_schemata = None
        if any(
            [
                model_instance_schema_uri,
                model_parameters_schema_uri,
                model_prediction_schema_uri,
            ]
        ):
            model_predict_schemata = PredictSchemata(
                instance_schema_uri=model_instance_schema_uri,
                parameters_schema_uri=model_parameters_schema_uri,
                prediction_schema_uri=model_prediction_schema_uri,
            )

        # Create the container spec
        env = None
        ports = None

        if model_serving_container_environment_variables:
            env = [
                EnvVar(name=str(key), value=str(value))
                for key, value in model_serving_container_environment_variables.items()
            ]

        if model_serving_container_ports:
            ports = [Port(container_port=port) for port in model_serving_container_ports]

        container_spec = ModelContainerSpec(
            image_uri=model_serving_container_image_uri,
            command=model_serving_container_command,
            args=model_serving_container_args,
            env=env,
            ports=ports,
            predict_route=model_serving_container_predict_route,
            health_route=model_serving_container_health_route,
        )

        self._model_encryption_spec = initializer.global_config.get_encryption_spec(
            encryption_spec_key_name=model_encryption_spec_key_name
        )

        self._managed_model = Model(
            description=model_description,
            predict_schemata=model_predict_schemata,
            container_spec=container_spec,
            encryption_spec=self._model_encryption_spec,
        )

        self.labels = labels
        self._training_encryption_spec = initializer.global_config.get_encryption_spec(
            encryption_spec_key_name=training_encryption_spec_key_name
        )

        self._staging_bucket = staging_bucket or initializer.global_config.staging_bucket
        # END Custom
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook = VertexAIHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        self.training_pipeline: Optional[TrainingPipeline] = None
        self.worker_pool_specs: worker_spec_utils._DistributedTrainingSpec = None
        self.managed_model: Optional[Model] = None

    def _prepare_and_validate_run(
        self,
        model_display_name: Optional[str] = None,
        model_labels: Optional[Dict[str, str]] = None,
        replica_count: int = 1,
        machine_type: str = "n1-standard-4",
        accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED",
        accelerator_count: int = 0,
        boot_disk_type: str = "pd-ssd",
        boot_disk_size_gb: int = 100,
    ) -> Tuple[worker_spec_utils._DistributedTrainingSpec, Optional[Model]]:
        """Create worker pool specs and managed model as well validating the
        run.

        Args:
            model_display_name (str):
                If the script produces a managed Vertex AI Model. The display name of
                the Model. The name can be up to 128 characters long and can be consist
                of any UTF-8 characters.

                If not provided upon creation, the job's display_name is used.
            model_labels (Dict[str, str]):
                Optional. The labels with user-defined metadata to
                organize your Models.
                Label keys and values can be no longer than 64
                characters (Unicode codepoints), can only
                contain lowercase letters, numeric characters,
                underscores and dashes. International characters
                are allowed.
                See https://goo.gl/xmQnxf for more information
                and examples of labels.
            replica_count (int):
                The number of worker replicas. If replica count = 1 then one chief
                replica will be provisioned. If replica_count > 1 the remainder will be
                provisioned as a worker replica pool.
            machine_type (str):
                The type of machine to use for training.
            accelerator_type (str):
                Hardware accelerator type. One of ACCELERATOR_TYPE_UNSPECIFIED,
                NVIDIA_TESLA_K80, NVIDIA_TESLA_P100, NVIDIA_TESLA_V100, NVIDIA_TESLA_P4,
                NVIDIA_TESLA_T4
            accelerator_count (int):
                The number of accelerators to attach to a worker replica.
            boot_disk_type (str):
                Type of the boot disk, default is `pd-ssd`.
                Valid values: `pd-ssd` (Persistent Disk Solid State Drive) or
                `pd-standard` (Persistent Disk Hard Disk Drive).
            boot_disk_size_gb (int):
                Size in GB of the boot disk, default is 100GB.
                boot disk size must be within the range of [100, 64000].
        Returns:
            Worker pools specs and managed model for run.

        Raises:
            RuntimeError: If Training job has already been run or model_display_name was
                provided but required arguments were not provided in constructor.
        """
        # TODO: Maybe not need
        # if self._is_waiting_to_run():
        #     raise RuntimeError("Custom Training is already scheduled to run.")

        # TODO: Maybe not need
        # if self._has_run:
        #     raise RuntimeError("Custom Training has already run.")

        # if args needed for model is incomplete
        if model_display_name and not self._managed_model.container_spec.image_uri:
            raise RuntimeError(
                """model_display_name was provided but
                model_serving_container_image_uri was not provided when this
                custom pipeline was constructed.
                """
            )

        if self._managed_model.container_spec.image_uri:
            model_display_name = model_display_name or self._display_name + "-model"

        # validates args and will raise
        worker_pool_specs = worker_spec_utils._DistributedTrainingSpec.chief_worker_pool(
            replica_count=replica_count,
            machine_type=machine_type,
            accelerator_count=accelerator_count,
            accelerator_type=accelerator_type,
            boot_disk_type=boot_disk_type,
            boot_disk_size_gb=boot_disk_size_gb,
        ).pool_specs

        managed_model = self._managed_model
        if model_display_name:
            utils.validate_display_name(model_display_name)
            managed_model.display_name = model_display_name
            if model_labels:
                utils.validate_labels(model_labels)
                managed_model.labels = model_labels
            else:
                managed_model.labels = self.labels
        else:
            managed_model = None

        return worker_pool_specs, managed_model

    def _prepare_training_task_inputs_and_output_dir(
        self,
        worker_pool_specs: worker_spec_utils._DistributedTrainingSpec,
        base_output_dir: Optional[str] = None,
        service_account: Optional[str] = None,
        network: Optional[str] = None,
        tensorboard: Optional[str] = None,
    ) -> Tuple[Dict, str]:
        """Prepares training task inputs and output directory for custom job.

        Args:
            worker_pools_spec (worker_spec_utils._DistributedTrainingSpec):
                Worker pools pecs required to run job.
            base_output_dir (str):
                GCS output directory of job. If not provided a
                timestamped directory in the staging directory will be used.
            service_account (str):
                Specifies the service account for workload run-as account.
                Users submitting jobs must have act-as permission on this run-as account.
            network (str):
                The full name of the Compute Engine network to which the job
                should be peered. For example, projects/12345/global/networks/myVPC.
                Private services access must already be configured for the network.
                If left unspecified, the job is not peered with any network.
            tensorboard (str):
                Optional. The name of a Vertex AI
                [Tensorboard][google.cloud.aiplatform.v1beta1.Tensorboard]
                resource to which this CustomJob will upload Tensorboard
                logs. Format:
                ``projects/{project}/locations/{location}/tensorboards/{tensorboard}``

                The training script should write Tensorboard to following Vertex AI environment
                variable:

                AIP_TENSORBOARD_LOG_DIR

                `service_account` is required with provided `tensorboard`.
                For more information on configuring your service account please visit:
                https://cloud.google.com/vertex-ai/docs/experiments/tensorboard-training
        Returns:
            Training task inputs and Output directory for custom job.
        """
        # default directory if not given
        base_output_dir = base_output_dir or _timestamped_gcs_dir(
            self._staging_bucket, "aiplatform-custom-training"
        )

        self.log.info(f"Training Output directory:\n{base_output_dir} ")

        training_task_inputs = {
            "worker_pool_specs": worker_pool_specs,
            "base_output_directory": {"output_uri_prefix": base_output_dir},
        }

        if service_account:
            training_task_inputs["service_account"] = service_account
        if network:
            training_task_inputs["network"] = network
        if tensorboard:
            training_task_inputs["tensorboard"] = tensorboard

        return training_task_inputs, base_output_dir

    def _create_input_data_config(
        self,
        dataset: Optional[datasets._Dataset] = None,
        annotation_schema_uri: Optional[str] = None,
        training_fraction_split: Optional[float] = None,
        validation_fraction_split: Optional[float] = None,
        test_fraction_split: Optional[float] = None,
        training_filter_split: Optional[str] = None,
        validation_filter_split: Optional[str] = None,
        test_filter_split: Optional[str] = None,
        predefined_split_column_name: Optional[str] = None,
        timestamp_split_column_name: Optional[str] = None,
        gcs_destination_uri_prefix: Optional[str] = None,
        bigquery_destination: Optional[str] = None,
    ) -> Optional[InputDataConfig]:
        """Constructs a input data config to pass to the training pipeline.

        Args:
            dataset (datasets._Dataset):
                The dataset within the same Project from which data will be used to train the Model. The
                Dataset must use schema compatible with Model being trained,
                and what is compatible should be described in the used
                TrainingPipeline's [training_task_definition]
                [google.cloud.aiplatform.v1beta1.TrainingPipeline.training_task_definition].
                For tabular Datasets, all their data is exported to
                training, to pick and choose from.
            annotation_schema_uri (str):
                Google Cloud Storage URI points to a YAML file describing
                annotation schema. The schema is defined as an OpenAPI 3.0.2
                [Schema Object]
                (https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#schema-object)
                The schema files that can be used here are found in
                gs://google-cloud-aiplatform/schema/dataset/annotation/,
                note that the chosen schema must be consistent with
                ``metadata``
                of the Dataset specified by
                ``dataset_id``.

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
            training_fraction_split (float):
                Optional. The fraction of the input data that is to be used to train
                the Model. This is ignored if Dataset is not provided.
            validation_fraction_split (float):
                Optional. The fraction of the input data that is to be used to validate
                the Model. This is ignored if Dataset is not provided.
            test_fraction_split (float):
                Optional. The fraction of the input data that is to be used to evaluate
                the Model. This is ignored if Dataset is not provided.
            training_filter_split (str):
                Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to train the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
            validation_filter_split (str):
                Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to validate the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
            test_filter_split (str):
                Optional. A filter on DataItems of the Dataset. DataItems that match
                this filter are used to test the Model. A filter with same syntax
                as the one used in DatasetService.ListDataItems may be used. If a
                single DataItem is matched by more than one of the FilterSplit filters,
                then it is assigned to the first set that applies to it in the training,
                validation, test order. This is ignored if Dataset is not provided.
            predefined_split_column_name (str):
                Optional. The key is a name of one of the Dataset's data
                columns. The value of the key (either the label's value or
                value in the column) must be one of {``training``,
                ``validation``, ``test``}, and it defines to which set the
                given piece of data is assigned. If for a piece of data the
                key is not present or has an invalid value, that piece is
                ignored by the pipeline.

                Supported only for tabular and time series Datasets.
            timestamp_split_column_name (str):
                Optional. The key is a name of one of the Dataset's data
                columns. The value of the key values of the key (the values in
                the column) must be in RFC 3339 `date-time` format, where
                `time-offset` = `"Z"` (e.g. 1985-04-12T23:20:50.52Z). If for a
                piece of data the key is not present or has an invalid value,
                that piece is ignored by the pipeline.

                Supported only for tabular and time series Datasets.
                This parameter must be used with training_fraction_split,
                validation_fraction_split and test_fraction_split.
            gcs_destination_uri_prefix (str):
                Optional. The Google Cloud Storage location.

                The Vertex AI environment variables representing Google
                Cloud Storage data URIs will always be represented in the
                Google Cloud Storage wildcard format to support sharded
                data.

                -  AIP_DATA_FORMAT = "jsonl".
                -  AIP_TRAINING_DATA_URI = "gcs_destination/training-*"
                -  AIP_VALIDATION_DATA_URI = "gcs_destination/validation-*"
                -  AIP_TEST_DATA_URI = "gcs_destination/test-*".
            bigquery_destination (str):
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
        Raises:
            ValueError: When more than 1 type of split configuration is passed or when
                the split configuration passed is incompatible with the dataset schema.
        """
        input_data_config = None
        if dataset:
            # Initialize all possible splits
            filter_split = None
            predefined_split = None
            timestamp_split = None
            fraction_split = None

            # Create filter split
            if any(
                [
                    training_filter_split is not None,
                    validation_filter_split is not None,
                    test_filter_split is not None,
                ]
            ):
                if all(
                    [
                        training_filter_split is not None,
                        validation_filter_split is not None,
                        test_filter_split is not None,
                    ]
                ):
                    filter_split = FilterSplit(
                        training_filter=training_filter_split,
                        validation_filter=validation_filter_split,
                        test_filter=test_filter_split,
                    )
                else:
                    raise ValueError("All filter splits must be passed together or not at all")

            # Create predefined split
            if predefined_split_column_name:
                predefined_split = PredefinedSplit(key=predefined_split_column_name)

            # Create timestamp split or fraction split
            if timestamp_split_column_name:
                timestamp_split = TimestampSplit(
                    training_fraction=training_fraction_split,
                    validation_fraction=validation_fraction_split,
                    test_fraction=test_fraction_split,
                    key=timestamp_split_column_name,
                )
            elif any(
                [
                    training_fraction_split is not None,
                    validation_fraction_split is not None,
                    test_fraction_split is not None,
                ]
            ):
                fraction_split = FractionSplit(
                    training_fraction=training_fraction_split,
                    validation_fraction=validation_fraction_split,
                    test_fraction=test_fraction_split,
                )

            splits = [
                split
                for split in [
                    filter_split,
                    predefined_split,
                    timestamp_split_column_name,
                    fraction_split,
                ]
                if split is not None
            ]

            # Fallback to fraction split if nothing else is specified
            if len(splits) == 0:
                self.log.info("No dataset split provided. The service will use a default split.")
            elif len(splits) > 1:
                raise ValueError(
                    """Can only specify one of:
                        1. training_filter_split, validation_filter_split, test_filter_split
                        2. predefined_split_column_name
                        3. timestamp_split_column_name, training_fraction_split, validation_fraction_split,
                        test_fraction_split
                        4. training_fraction_split, validation_fraction_split, test_fraction_split"""
                )

            # create GCS destination
            gcs_destination = None
            if gcs_destination_uri_prefix:
                gcs_destination = GcsDestination(output_uri_prefix=gcs_destination_uri_prefix)

            # TODO(b/177416223) validate managed BQ dataset is passed in
            bigquery_destination_proto = None
            if bigquery_destination:
                bigquery_destination_proto = BigQueryDestination(output_uri=bigquery_destination)

            # create input data config
            input_data_config = InputDataConfig(
                fraction_split=fraction_split,
                filter_split=filter_split,
                predefined_split=predefined_split,
                timestamp_split=timestamp_split,
                dataset_id=dataset.name,
                annotation_schema_uri=annotation_schema_uri,
                gcs_destination=gcs_destination,
                bigquery_destination=bigquery_destination_proto,
            )

        return input_data_config

    def _get_model(self, training_pipeline):
        # TODO: implement logic for extract model from training_pipeline object
        pass

    def execute(self, context):
        (training_task_inputs, base_output_dir,) = self._prepare_training_task_inputs_and_output_dir(
            worker_pool_specs=self.worker_pool_specs,
            base_output_dir=self.base_output_dir,
            service_account=self.service_account,
            network=self.network,
            tensorboard=self.tensorboard,
        )

        input_data_config = self._create_input_data_config(
            dataset=self.dataset,
            annotation_schema_uri=self.annotation_schema_uri,
            training_fraction_split=self.training_fraction_split,
            validation_fraction_split=self.validation_fraction_split,
            test_fraction_split=self.test_fraction_split,
            training_filter_split=self.training_filter_split,
            validation_filter_split=self.validation_filter_split,
            test_filter_split=self.test_filter_split,
            predefined_split_column_name=self.predefined_split_column_name,
            timestamp_split_column_name=self.timestamp_split_column_name,
            gcs_destination_uri_prefix=base_output_dir,
            bigquery_destination=self.bigquery_destination,
        )

        # create training pipeline configuration object
        training_pipeline = TrainingPipeline(
            display_name=self.display_name,
            training_task_definition=schema.training_job.definition.custom_task,  # TODO: different for automl
            training_task_inputs=training_task_inputs,  # Required
            model_to_upload=self.managed_model,  # Optional
            input_data_config=input_data_config,  # Optional
            labels=self.labels,  # Optional
            encryption_spec=self._training_encryption_spec,  # Optional
        )

        self.training_pipeline = self.hook.create_training_pipeline(
            project_id=self.project_id,
            region=self.region,
            training_pipeline=training_pipeline,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        model = self._get_model(self.training_pipeline)

        return model

    def on_kill(self) -> None:
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.training_pipeline:
            self.hook.cancel_training_pipeline(
                project_id=self.project_id,
                region=self.region,
                training_pipeline=self.training_pipeline.name,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )


class VertexAICreateCustomContainerTrainingJobOperator(VertexAITrainingJobBaseOperator):
    """Create Custom Container Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]

    def __init__(
        self,
        *,
        command: Sequence[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._command = command

    def execute(self, context):
        self.worker_pool_specs, self.managed_model = self._prepare_and_validate_run(
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_count=self.accelerator_count,
            accelerator_type=self.accelerator_type,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
        )

        for spec in self.worker_pool_specs:
            spec["containerSpec"] = {"imageUri": self._container_uri}

            if self._command:
                spec["containerSpec"]["command"] = self._command

            if self.args:
                spec["containerSpec"]["args"] = self.args

            if self.environment_variables:
                spec["containerSpec"]["env"] = [
                    {"name": key, "value": value} for key, value in self.environment_variables.items()
                ]

        super().execute(context)


class VertexAICreateCustomPythonPackageTrainingJobOperator(VertexAITrainingJobBaseOperator):
    """Create Custom Python Package Training job"""

    template_fields = [
        'region',
        'impersonation_chain',
    ]

    def __init__(
        self,
        python_package_gcs_uri: str,
        python_module_name: str,
    ) -> None:
        self._package_gcs_uri = python_package_gcs_uri
        self._python_module = python_module_name

    def execute(self, context):
        self.worker_pool_specs, self.managed_model = self._prepare_and_validate_run(
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_count=self.accelerator_count,
            accelerator_type=self.accelerator_type,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
        )

        for spec in self.worker_pool_specs:
            spec["python_package_spec"] = {
                "executor_image_uri": self._container_uri,
                "python_module": self._python_module,
                "package_uris": [self._package_gcs_uri],
            }

            if self.args:
                spec["python_package_spec"]["args"] = self.args

            if self.environment_variables:
                spec["python_package_spec"]["env"] = [
                    {"name": key, "value": value} for key, value in self.environment_variables.items()
                ]

        super().execute(context)


class VertexAICreateCustomTrainingJobOperator(VertexAITrainingJobBaseOperator):
    """Create Custom Training job"""

    def __init__(
        self,
        display_name: str,
        script_path: str,
    ) -> None:
        pass

    def execute(self, context):
        self.worker_pool_specs, self.managed_model = self._prepare_and_validate_run(
            model_display_name=self.model_display_name,
            model_labels=self.model_labels,
            replica_count=self.replica_count,
            machine_type=self.machine_type,
            accelerator_count=self.accelerator_count,
            accelerator_type=self.accelerator_type,
            boot_disk_type=self.boot_disk_type,
            boot_disk_size_gb=self.boot_disk_size_gb,
        )
        super().execute(context)


class VertexAICancelPipelineJobOperator(BaseOperator):
    """
    Cancels a PipelineJob. Starts asynchronous cancellation on the PipelineJob. The server makes a best effort
    to cancel the pipeline, but success is not guaranteed. Clients can use
    [PipelineService.GetPipelineJob][google.cloud.aiplatform.v1.PipelineService.GetPipelineJob] or other
    methods to check whether the cancellation succeeded or whether the pipeline completed despite
    cancellation. On successful cancellation, the PipelineJob is not deleted; instead it becomes a pipeline
    with a [PipelineJob.error][google.cloud.aiplatform.v1.PipelineJob.error] value with a
    [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
    [PipelineJob.state][google.cloud.aiplatform.v1.PipelineJob.state] is set to ``CANCELLED``.

    :param request:  The request object. Request message for
        [PipelineService.CancelPipelineJob][google.cloud.aiplatform.v1.PipelineService.CancelPipelineJob].
    :type request: Union[google.cloud.aiplatform_v1.types.CancelPipelineJobRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param pipeline_job: TODO: Fill description
    :type pipeline_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[CancelPipelineJobRequest, Dict],
        location: str,
        pipeline_job: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.pipeline_job = pipeline_job
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.cancel_pipeline_job(
            request=self.request,
            location=self.location,
            pipeline_job=self.pipeline_job,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAICancelTrainingPipelineOperator(BaseOperator):
    """
    Cancels a TrainingPipeline. Starts asynchronous cancellation on the TrainingPipeline. The server makes a
    best effort to cancel the pipeline, but success is not guaranteed. Clients can use
    [PipelineService.GetTrainingPipeline][google.cloud.aiplatform.v1.PipelineService.GetTrainingPipeline] or
    other methods to check whether the cancellation succeeded or whether the pipeline completed despite
    cancellation. On successful cancellation, the TrainingPipeline is not deleted; instead it becomes a
    pipeline with a [TrainingPipeline.error][google.cloud.aiplatform.v1.TrainingPipeline.error] value with a
    [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
    [TrainingPipeline.state][google.cloud.aiplatform.v1.TrainingPipeline.state] is set to ``CANCELLED``.

    :param request:  The request object. Request message for [PipelineService.CancelTrainingPipeline][google.c
        loud.aiplatform.v1.PipelineService.CancelTrainingPipeline].
    :type request: Union[google.cloud.aiplatform_v1.types.CancelTrainingPipelineRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param training_pipeline: TODO: Fill description
    :type training_pipeline: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[CancelTrainingPipelineRequest, Dict],
        location: str,
        training_pipeline: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.training_pipeline = training_pipeline
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.cancel_training_pipeline(
            request=self.request,
            location=self.location,
            training_pipeline=self.training_pipeline,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAICreatePipelineJobOperator(BaseOperator):
    """
    Creates a PipelineJob. A PipelineJob will run immediately when created.

    :param request:  The request object. Request message for
        [PipelineService.CreatePipelineJob][google.cloud.aiplatform.v1.PipelineService.CreatePipelineJob].
    :type request: Union[google.cloud.aiplatform_v1.types.CreatePipelineJobRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param pipeline_job:  Required. The PipelineJob to create. This corresponds to the ``pipeline_job`` field
        on the ``request`` instance; if ``request`` is provided, this should not be set.
    :type pipeline_job: google.cloud.aiplatform_v1.types.PipelineJob
    :param pipeline_job_id:  The ID to use for the PipelineJob, which will become the final component of the
        PipelineJob name. If not provided, an ID will be automatically generated.

        This value should be less than 128 characters, and valid characters are /[a-z][0-9]-/.

        This corresponds to the ``pipeline_job_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type pipeline_job_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[CreatePipelineJobRequest, Dict],
        location: str,
        pipeline_job: PipelineJob,
        pipeline_job_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.pipeline_job = pipeline_job
        self.pipeline_job_id = pipeline_job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.create_pipeline_job(
            request=self.request,
            location=self.location,
            pipeline_job=self.pipeline_job,
            pipeline_job_id=self.pipeline_job_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAICreateTrainingPipelineOperator(BaseOperator):
    """
    Creates a TrainingPipeline. A created TrainingPipeline right away will be attempted to be run.

    :param request:  The request object. Request message for [PipelineService.CreateTrainingPipeline][google.c
        loud.aiplatform.v1.PipelineService.CreateTrainingPipeline].
    :type request: Union[google.cloud.aiplatform_v1.types.CreateTrainingPipelineRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param training_pipeline:  Required. The TrainingPipeline to create.

        This corresponds to the ``training_pipeline`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type training_pipeline: google.cloud.aiplatform_v1.types.TrainingPipeline
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[CreateTrainingPipelineRequest, Dict],
        location: str,
        training_pipeline: TrainingPipeline,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.training_pipeline = training_pipeline
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.create_training_pipeline(
            request=self.request,
            location=self.location,
            training_pipeline=self.training_pipeline,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIDeletePipelineJobOperator(BaseOperator):
    """
    Deletes a PipelineJob.

    :param request:  The request object. Request message for
        [PipelineService.DeletePipelineJob][google.cloud.aiplatform.v1.PipelineService.DeletePipelineJob].
    :type request: Union[google.cloud.aiplatform_v1.types.DeletePipelineJobRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param pipeline_job: TODO: Fill description
    :type pipeline_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[DeletePipelineJobRequest, Dict],
        location: str,
        pipeline_job: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.pipeline_job = pipeline_job
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_pipeline_job(
            request=self.request,
            location=self.location,
            pipeline_job=self.pipeline_job,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIDeleteTrainingPipelineOperator(BaseOperator):
    """
    Deletes a TrainingPipeline.

    :param request:  The request object. Request message for [PipelineService.DeleteTrainingPipeline][google.c
        loud.aiplatform.v1.PipelineService.DeleteTrainingPipeline].
    :type request: Union[google.cloud.aiplatform_v1.types.DeleteTrainingPipelineRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param training_pipeline: TODO: Fill description
    :type training_pipeline: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[DeleteTrainingPipelineRequest, Dict],
        location: str,
        training_pipeline: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.training_pipeline = training_pipeline
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_training_pipeline(
            request=self.request,
            location=self.location,
            training_pipeline=self.training_pipeline,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIGetPipelineJobOperator(BaseOperator):
    """
    Gets a PipelineJob.

    :param request:  The request object. Request message for
        [PipelineService.GetPipelineJob][google.cloud.aiplatform.v1.PipelineService.GetPipelineJob].
    :type request: Union[google.cloud.aiplatform_v1.types.GetPipelineJobRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param pipeline_job: TODO: Fill description
    :type pipeline_job: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[GetPipelineJobRequest, Dict],
        location: str,
        pipeline_job: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.pipeline_job = pipeline_job
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.get_pipeline_job(
            request=self.request,
            location=self.location,
            pipeline_job=self.pipeline_job,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIGetTrainingPipelineOperator(BaseOperator):
    """
    Gets a TrainingPipeline.

    :param request:  The request object. Request message for
        [PipelineService.GetTrainingPipeline][google.cloud.aiplatform.v1.PipelineService.GetTrainingPipeline].
    :type request: Union[google.cloud.aiplatform_v1.types.GetTrainingPipelineRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param training_pipeline: TODO: Fill description
    :type training_pipeline: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[GetTrainingPipelineRequest, Dict],
        location: str,
        training_pipeline: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.training_pipeline = training_pipeline
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.get_training_pipeline(
            request=self.request,
            location=self.location,
            training_pipeline=self.training_pipeline,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIListPipelineJobsOperator(BaseOperator):
    """
    Lists PipelineJobs in a Location.

    :param request:  The request object. Request message for
        [PipelineService.ListPipelineJobs][google.cloud.aiplatform.v1.PipelineService.ListPipelineJobs].
    :type request: Union[google.cloud.aiplatform_v1.types.ListPipelineJobsRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[ListPipelineJobsRequest, Dict],
        location: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.list_pipeline_jobs(
            request=self.request,
            location=self.location,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )


class VertexAIListTrainingPipelinesOperator(BaseOperator):
    """
    Lists TrainingPipelines in a Location.

    :param request:  The request object. Request message for
        [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelin
        es].
    :type request: Union[google.cloud.aiplatform_v1.types.ListTrainingPipelinesRequest, Dict]
    :param location: TODO: Fill description
    :type location: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param project_id: TODO: Fill description
    :type project_id: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        request: Union[ListTrainingPipelinesRequest, Dict],
        location: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.location = location
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = VertexAIHook(gcp_conn_id=self.gcp_conn_id)
        hook.list_training_pipelines(
            request=self.request,
            location=self.location,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            project_id=self.project_id,
        )
