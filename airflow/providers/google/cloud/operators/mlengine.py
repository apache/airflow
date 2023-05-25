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
"""This module contains Google Cloud MLEngine operators."""

from __future__ import annotations

import logging
import re
import time
import warnings
from typing import TYPE_CHECKING, Any, Sequence

from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.mlengine import MLEngineHook
from airflow.providers.google.cloud.links.mlengine import (
    MLEngineJobDetailsLink,
    MLEngineJobSListLink,
    MLEngineModelLink,
    MLEngineModelsListLink,
    MLEngineModelVersionDetailsLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.mlengine import MLEngineStartTrainingJobTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


log = logging.getLogger(__name__)


def _normalize_mlengine_job_id(job_id: str) -> str:
    """
    Replaces invalid MLEngine job_id characters with '_'.

    This also adds a leading 'z' in case job_id starts with an invalid
    character.

    :param job_id: A job_id str that may have invalid characters.
    :return: A valid job_id representation.
    """
    # Add a prefix when a job_id starts with a digit or a template
    match = re.search(r"\d|\{{2}", job_id)
    if match and match.start() == 0:
        job = f"z_{job_id}"
    else:
        job = job_id

    # Clean up 'bad' characters except templates
    tracker = 0
    cleansed_job_id = ""
    for match in re.finditer(r"\{{2}.+?\}{2}", job):
        cleansed_job_id += re.sub(r"[^0-9a-zA-Z]+", "_", job[tracker : match.start()])
        cleansed_job_id += job[match.start() : match.end()]
        tracker = match.end()

    # Clean up last substring or the full string if no templates
    cleansed_job_id += re.sub(r"[^0-9a-zA-Z]+", "_", job[tracker:])

    return cleansed_job_id


class MLEngineStartBatchPredictionJobOperator(GoogleCloudBaseOperator):
    """
    Start a Google Cloud ML Engine prediction job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineStartBatchPredictionJobOperator`

    NOTE: For model origin, users should consider exactly one from the
    three options below:

    1. Populate ``uri`` field only, which should be a GCS location that
       points to a tensorflow savedModel directory.
    2. Populate ``model_name`` field only, which refers to an existing
       model, and the default version of the model will be used.
    3. Populate both ``model_name`` and ``version_name`` fields, which
       refers to a specific version of a specific model.

    In options 2 and 3, both model and version name should contain the
    minimal identifier. For instance, call::

        MLEngineBatchPredictionOperator(
            ...,
            model_name='my_model',
            version_name='my_version',
            ...)

    if the desired model version is
    ``projects/my_project/models/my_model/versions/my_version``.

    See https://cloud.google.com/ml-engine/reference/rest/v1/projects.jobs
    for further documentation on the parameters.

    :param job_id: A unique id for the prediction job on Google Cloud
        ML Engine. (templated)
    :param data_format: The format of the input data.
        It will default to 'DATA_FORMAT_UNSPECIFIED' if is not provided
        or is not one of ["TEXT", "TF_RECORD", "TF_RECORD_GZIP"].
    :param input_paths: A list of GCS paths of input data for batch
        prediction. Accepting wildcard operator ``*``, but only at the end. (templated)
    :param output_path: The GCS path where the prediction results are
        written to. (templated)
    :param region: The Google Compute Engine region to run the
        prediction job in. (templated)
    :param model_name: The Google Cloud ML Engine model to use for prediction.
        If version_name is not provided, the default version of this
        model will be used.
        Should not be None if version_name is provided.
        Should be None if uri is provided. (templated)
    :param version_name: The Google Cloud ML Engine model version to use for
        prediction.
        Should be None if uri is provided. (templated)
    :param uri: The GCS path of the saved model to use for prediction.
        Should be None if model_name is provided.
        It should be a GCS path pointing to a tensorflow SavedModel. (templated)
    :param max_worker_count: The maximum number of workers to be used
        for parallel processing. Defaults to 10 if not specified. Should be a
        string representing the worker count ("10" instead of 10, "50" instead
        of 50, etc.)
    :param runtime_version: The Google Cloud ML Engine runtime version to use
        for batch prediction.
    :param signature_name: The name of the signature defined in the SavedModel
        to use for this job.
    :param project_id: The Google Cloud project name where the prediction job is submitted.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID used for connection to Google
        Cloud Platform.
    :param labels: a dictionary containing labels for the job; passed to BigQuery
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :raises: ``ValueError``: if a unique model/version origin cannot be
        determined.
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_job_id",
        "_region",
        "_input_paths",
        "_output_path",
        "_model_name",
        "_version_name",
        "_uri",
        "_impersonation_chain",
    )

    def __init__(
        self,
        *,
        job_id: str,
        region: str,
        data_format: str,
        input_paths: list[str],
        output_path: str,
        model_name: str | None = None,
        version_name: str | None = None,
        uri: str | None = None,
        max_worker_count: int | None = None,
        runtime_version: str | None = None,
        signature_name: str | None = None,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        labels: dict[str, str] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self._project_id = project_id
        self._job_id = job_id
        self._region = region
        self._data_format = data_format
        self._input_paths = input_paths
        self._output_path = output_path
        self._model_name = model_name
        self._version_name = version_name
        self._uri = uri
        self._max_worker_count = max_worker_count
        self._runtime_version = runtime_version
        self._signature_name = signature_name
        self._gcp_conn_id = gcp_conn_id
        self._labels = labels
        self._impersonation_chain = impersonation_chain

        if not self._project_id:
            raise AirflowException("Google Cloud project id is required.")
        if not self._job_id:
            raise AirflowException("An unique job id is required for Google MLEngine prediction job.")

        if self._uri:
            if self._model_name or self._version_name:
                raise AirflowException(
                    "Ambiguous model origin: Both uri and model/version name are provided."
                )

        if self._version_name and not self._model_name:
            raise AirflowException(
                "Missing model: Batch prediction expects a model name when a version name is provided."
            )

        if not (self._uri or self._model_name):
            raise AirflowException(
                "Missing model origin: Batch prediction expects a model, "
                "a model & version combination, or a URI to a savedModel."
            )

    def execute(self, context: Context):
        job_id = _normalize_mlengine_job_id(self._job_id)
        prediction_request: dict[str, Any] = {
            "jobId": job_id,
            "predictionInput": {
                "dataFormat": self._data_format,
                "inputPaths": self._input_paths,
                "outputPath": self._output_path,
                "region": self._region,
            },
        }
        if self._labels:
            prediction_request["labels"] = self._labels

        if self._uri:
            prediction_request["predictionInput"]["uri"] = self._uri
        elif self._model_name:
            origin_name = f"projects/{self._project_id}/models/{self._model_name}"
            if not self._version_name:
                prediction_request["predictionInput"]["modelName"] = origin_name
            else:
                prediction_request["predictionInput"]["versionName"] = (
                    origin_name + f"/versions/{self._version_name}"
                )

        if self._max_worker_count:
            prediction_request["predictionInput"]["maxWorkerCount"] = self._max_worker_count

        if self._runtime_version:
            prediction_request["predictionInput"]["runtimeVersion"] = self._runtime_version

        if self._signature_name:
            prediction_request["predictionInput"]["signatureName"] = self._signature_name

        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, impersonation_chain=self._impersonation_chain)

        # Helper method to check if the existing job's prediction input is the
        # same as the request we get here.
        def check_existing_job(existing_job):
            return existing_job.get("predictionInput") == prediction_request["predictionInput"]

        finished_prediction_job = hook.create_job(
            project_id=self._project_id, job=prediction_request, use_existing_job_fn=check_existing_job
        )

        if finished_prediction_job["state"] != "SUCCEEDED":
            self.log.error("MLEngine batch prediction job failed: %s", str(finished_prediction_job))
            raise RuntimeError(finished_prediction_job["errorMessage"])

        return finished_prediction_job["predictionOutput"]


class MLEngineManageModelOperator(GoogleCloudBaseOperator):
    """
    Operator for managing a Google Cloud ML Engine model.

    .. warning::
       This operator is deprecated. Consider using operators for specific operations:
       MLEngineCreateModelOperator, MLEngineGetModelOperator.

    :param model: A dictionary containing the information about the model.
        If the `operation` is `create`, then the `model` parameter should
        contain all the information about this model such as `name`.

        If the `operation` is `get`, the `model` parameter
        should contain the `name` of the model.
    :param operation: The operation to perform. Available operations are:

        * ``create``: Creates a new model as provided by the `model` parameter.
        * ``get``: Gets a particular model where the name is specified in `model`.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model",
        "_impersonation_chain",
    )

    def __init__(
        self,
        *,
        model: dict,
        operation: str = "create",
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        warnings.warn(
            "This operator is deprecated. Consider using operators for specific operations: "
            "MLEngineCreateModelOperator, MLEngineGetModelOperator.",
            AirflowProviderDeprecationWarning,
            stacklevel=3,
        )

        self._project_id = project_id
        self._model = model
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )
        if self._operation == "create":
            return hook.create_model(project_id=self._project_id, model=self._model)
        elif self._operation == "get":
            return hook.get_model(project_id=self._project_id, model_name=self._model["name"])
        else:
            raise ValueError(f"Unknown operation: {self._operation}")


class MLEngineCreateModelOperator(GoogleCloudBaseOperator):
    """
    Creates a new model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineCreateModelOperator`

    The model should be provided by the `model` parameter.

    :param model: A dictionary containing the information about the model.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelLink(),)

    def __init__(
        self,
        *,
        model: dict,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._model = model
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model["name"],
            )

        return hook.create_model(project_id=self._project_id, model=self._model)


class MLEngineGetModelOperator(GoogleCloudBaseOperator):
    """
    Gets a particular model

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineGetModelOperator`

    The name of model should be specified in `model_name`.

    :param model_name: The name of the model.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )
        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model_name,
            )

        return hook.get_model(project_id=self._project_id, model_name=self._model_name)


class MLEngineDeleteModelOperator(GoogleCloudBaseOperator):
    """
    Deletes a model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineDeleteModelOperator`

    The model should be provided by the `model_name` parameter.

    :param model_name: The name of the model.
    :param delete_contents: (Optional) Whether to force the deletion even if the models is not empty.
        Will delete all version (if any) in the dataset if set to True.
        The default value is False.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelsListLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        delete_contents: bool = False,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._delete_contents = delete_contents
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelsListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )

        return hook.delete_model(
            project_id=self._project_id, model_name=self._model_name, delete_contents=self._delete_contents
        )


class MLEngineManageVersionOperator(GoogleCloudBaseOperator):
    """
    Operator for managing a Google Cloud ML Engine version.

    .. warning::
       This operator is deprecated. Consider using operators for specific operations:
       MLEngineCreateVersionOperator, MLEngineSetDefaultVersionOperator,
       MLEngineListVersionsOperator, MLEngineDeleteVersionOperator.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :param version_name: A name to use for the version being operated upon.
        If not None and the `version` argument is None or does not have a value for
        the `name` key, then this will be populated in the payload for the
        `name` key. (templated)
    :param version: A dictionary containing the information about the version.
        If the `operation` is `create`, `version` should contain all the
        information about this version such as name, and deploymentUrl.
        If the `operation` is `get` or `delete`, the `version` parameter
        should contain the `name` of the version.
        If it is None, the only `operation` possible would be `list`. (templated)
    :param operation: The operation to perform. Available operations are:

        *   ``create``: Creates a new version in the model specified by `model_name`,
            in which case the `version` parameter should contain all the
            information to create that version
            (e.g. `name`, `deploymentUrl`).

        *   ``set_defaults``: Sets a version in the model specified by `model_name` to be the default.
            The name of the version should be specified in the `version`
            parameter.

        *   ``list``: Lists all available versions of the model specified
            by `model_name`.

        *   ``delete``: Deletes the version specified in `version` parameter from the
            model specified by `model_name`).
            The name of the version should be specified in the `version`
            parameter.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_version_name",
        "_version",
        "_impersonation_chain",
    )

    def __init__(
        self,
        *,
        model_name: str,
        version_name: str | None = None,
        version: dict | None = None,
        operation: str = "create",
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._version = version or {}
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

        warnings.warn(
            "This operator is deprecated. Consider using operators for specific operations: "
            "MLEngineCreateVersion, MLEngineSetDefaultVersion, MLEngineListVersions, MLEngineDeleteVersion.",
            AirflowProviderDeprecationWarning,
            stacklevel=3,
        )

    def execute(self, context: Context):
        if "name" not in self._version:
            self._version["name"] = self._version_name

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        if self._operation == "create":
            if not self._version:
                raise ValueError(f"version attribute of {self.__class__.__name__} could not be empty")
            return hook.create_version(
                project_id=self._project_id, model_name=self._model_name, version_spec=self._version
            )
        elif self._operation == "set_default":
            return hook.set_default_version(
                project_id=self._project_id, model_name=self._model_name, version_name=self._version["name"]
            )
        elif self._operation == "list":
            return hook.list_versions(project_id=self._project_id, model_name=self._model_name)
        elif self._operation == "delete":
            return hook.delete_version(
                project_id=self._project_id, model_name=self._model_name, version_name=self._version["name"]
            )
        else:
            raise ValueError(f"Unknown operation: {self._operation}")


class MLEngineCreateVersionOperator(GoogleCloudBaseOperator):
    """
    Creates a new version in the model

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineCreateVersionOperator`

    Model should be specified by `model_name`, in which case the `version` parameter should contain all the
    information to create that version

    :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
    :param version: A dictionary containing the information about the version. (templated)
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_version",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelVersionDetailsLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        version: dict,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version = version
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version:
            raise AirflowException("The version parameter could not be empty.")

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelVersionDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model_name,
                version_id=self._version["name"],
            )

        return hook.create_version(
            project_id=self._project_id, model_name=self._model_name, version_spec=self._version
        )


class MLEngineSetDefaultVersionOperator(GoogleCloudBaseOperator):
    """
    Sets a version in the model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineSetDefaultVersionOperator`

    The model should be specified by `model_name` to be the default. The name of the version should be
    specified in the `version_name` parameter.

    :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
    :param version_name: A name to use for the version being operated upon. (templated)
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_version_name",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelVersionDetailsLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        version_name: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version_name:
            raise AirflowException("The version_name parameter could not be empty.")

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelVersionDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model_name,
                version_id=self._version_name,
            )

        return hook.set_default_version(
            project_id=self._project_id, model_name=self._model_name, version_name=self._version_name
        )


class MLEngineListVersionsOperator(GoogleCloudBaseOperator):
    """
    Lists all available versions of the model

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineListVersionsOperator`

    The model should be specified by `model_name`.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model_name,
            )

        return hook.list_versions(
            project_id=self._project_id,
            model_name=self._model_name,
        )


class MLEngineDeleteVersionOperator(GoogleCloudBaseOperator):
    """
    Deletes the version from the model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineDeleteVersionOperator`

    The name of the version should be specified in `version_name` parameter from the model specified
    by `model_name`.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :param version_name: A name to use for the version being operated upon. (templated)
    :param project_id: The Google Cloud project name to which MLEngine
        model belongs.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_model_name",
        "_version_name",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineModelLink(),)

    def __init__(
        self,
        *,
        model_name: str,
        version_name: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version_name:
            raise AirflowException("The version_name parameter could not be empty.")

    def execute(self, context: Context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineModelLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                model_id=self._model_name,
            )

        return hook.delete_version(
            project_id=self._project_id, model_name=self._model_name, version_name=self._version_name
        )


class MLEngineStartTrainingJobOperator(GoogleCloudBaseOperator):
    """
    Operator for launching a MLEngine training job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineStartTrainingJobOperator`

    :param job_id: A unique templated id for the submitted Google MLEngine
        training job. (templated)
    :param region: The Google Compute Engine region to run the MLEngine training
        job in (templated).
    :param package_uris: A list of Python package locations for the training
        job, which should include the main training program and any additional
        dependencies. This is mutually exclusive with a custom image specified
        via master_config. (templated)
    :param training_python_module: The name of the Python module to run within
        the training job after installing the packages. This is mutually
        exclusive with a custom image specified via master_config. (templated)
    :param training_args: A list of command-line arguments to pass to the
        training program. (templated)
    :param scale_tier: Resource tier for MLEngine training job. (templated)
    :param master_type: The type of virtual machine to use for the master
        worker. It must be set whenever scale_tier is CUSTOM. (templated)
    :param master_config: The configuration for the master worker. If this is
        provided, master_type must be set as well. If a custom image is
        specified, this is mutually exclusive with package_uris and
        training_python_module. (templated)
    :param runtime_version: The Google Cloud ML runtime version to use for
        training. (templated)
    :param python_version: The version of Python used in training. (templated)
    :param job_dir: A Google Cloud Storage path in which to store training
        outputs and other data needed for training. (templated)
    :param service_account: Optional service account to use when running the training application.
        (templated)
        The specified service account must have the `iam.serviceAccounts.actAs` role. The
        Google-managed Cloud ML Engine service account must have the `iam.serviceAccountAdmin` role
        for the specified service account.
        If set to None or missing, the Google-managed Cloud ML Engine service account will be used.
    :param project_id: The Google Cloud project name within which MLEngine training job should run.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param mode: Can be one of 'DRY_RUN'/'CLOUD'. In 'DRY_RUN' mode, no real
        training job will be launched, but the MLEngine training job request
        will be printed out. In 'CLOUD' mode, a real MLEngine training job
        creation request will be issued.
    :param labels: a dictionary containing labels for the job; passed to BigQuery
    :param hyperparameters: Optional HyperparameterSpec dictionary for hyperparameter tuning.
        For further reference, check:
        https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs#HyperparameterSpec
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_job_id",
        "_region",
        "_package_uris",
        "_training_python_module",
        "_training_args",
        "_scale_tier",
        "_master_type",
        "_master_config",
        "_runtime_version",
        "_python_version",
        "_job_dir",
        "_service_account",
        "_hyperparameters",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineJobDetailsLink(),)

    def __init__(
        self,
        *,
        job_id: str,
        region: str,
        project_id: str,
        package_uris: list[str] | None = None,
        training_python_module: str | None = None,
        training_args: list[str] | None = None,
        scale_tier: str | None = None,
        master_type: str | None = None,
        master_config: dict | None = None,
        runtime_version: str | None = None,
        python_version: str | None = None,
        job_dir: str | None = None,
        service_account: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        mode: str = "PRODUCTION",
        labels: dict[str, str] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        hyperparameters: dict | None = None,
        deferrable: bool = False,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._job_id = job_id
        self._region = region
        self._package_uris = package_uris
        self._training_python_module = training_python_module
        self._training_args = training_args
        self._scale_tier = scale_tier
        self._master_type = master_type
        self._master_config = master_config
        self._runtime_version = runtime_version
        self._python_version = python_version
        self._job_dir = job_dir
        self._service_account = service_account
        self._gcp_conn_id = gcp_conn_id
        self._mode = mode
        self._labels = labels
        self._hyperparameters = hyperparameters
        self._impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.cancel_on_kill = cancel_on_kill

        custom = self._scale_tier is not None and self._scale_tier.upper() == "CUSTOM"
        custom_image = (
            custom
            and self._master_config is not None
            and self._master_config.get("imageUri", None) is not None
        )

        if not self._project_id:
            raise AirflowException("Google Cloud project id is required.")
        if not self._job_id:
            raise AirflowException("An unique job id is required for Google MLEngine training job.")
        if not self._region:
            raise AirflowException("Google Compute Engine region is required.")
        if custom and not self._master_type:
            raise AirflowException("master_type must be set when scale_tier is CUSTOM")
        if self._master_config and not self._master_type:
            raise AirflowException("master_type must be set when master_config is provided")
        if not (package_uris and training_python_module) and not custom_image:
            raise AirflowException(
                "Either a Python package with a Python module or a custom Docker image should be provided."
            )
        if (package_uris or training_python_module) and custom_image:
            raise AirflowException(
                "Either a Python package with a Python module or "
                "a custom Docker image should be provided but not both."
            )

    def _handle_job_error(self, finished_training_job) -> None:
        if finished_training_job["state"] != "SUCCEEDED":
            self.log.error("MLEngine training job failed: %s", str(finished_training_job))
            raise RuntimeError(finished_training_job["errorMessage"])

    def execute(self, context: Context):
        job_id = _normalize_mlengine_job_id(self._job_id)
        self.job_id = job_id
        training_request: dict[str, Any] = {
            "jobId": self.job_id,
            "trainingInput": {
                "scaleTier": self._scale_tier,
                "region": self._region,
            },
        }
        if self._package_uris:
            training_request["trainingInput"]["packageUris"] = self._package_uris

        if self._training_python_module:
            training_request["trainingInput"]["pythonModule"] = self._training_python_module

        if self._training_args:
            training_request["trainingInput"]["args"] = self._training_args

        if self._master_type:
            training_request["trainingInput"]["masterType"] = self._master_type

        if self._master_config:
            training_request["trainingInput"]["masterConfig"] = self._master_config

        if self._runtime_version:
            training_request["trainingInput"]["runtimeVersion"] = self._runtime_version

        if self._python_version:
            training_request["trainingInput"]["pythonVersion"] = self._python_version

        if self._job_dir:
            training_request["trainingInput"]["jobDir"] = self._job_dir

        if self._service_account:
            training_request["trainingInput"]["serviceAccount"] = self._service_account

        if self._hyperparameters:
            training_request["trainingInput"]["hyperparameters"] = self._hyperparameters

        if self._labels:
            training_request["labels"] = self._labels

        if self._mode == "DRY_RUN":
            self.log.info("In dry_run mode.")
            self.log.info("MLEngine Training job request is: %s", training_request)
            return

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )
        self.hook = hook

        try:
            self.log.info("Executing: %s'", training_request)
            self.job_id = self.hook.create_job_without_waiting_result(
                project_id=self._project_id,
                body=training_request,
            )
        except HttpError as e:
            if e.resp.status == 409:
                # If the job already exists retrieve it
                self.hook.get_job(project_id=self._project_id, job_id=self.job_id)
                if self._project_id:
                    MLEngineJobDetailsLink.persist(
                        context=context,
                        task_instance=self,
                        project_id=self._project_id,
                        job_id=self.job_id,
                    )
                self.log.error(
                    "Failed to create new job with given name since it already exists. "
                    "The existing one will be used."
                )
            else:
                raise e

        context["ti"].xcom_push(key="job_id", value=self.job_id)
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=MLEngineStartTrainingJobTrigger(
                    conn_id=self._gcp_conn_id,
                    job_id=self.job_id,
                    project_id=self._project_id,
                    region=self._region,
                    runtime_version=self._runtime_version,
                    python_version=self._python_version,
                    job_dir=self._job_dir,
                    package_uris=self._package_uris,
                    training_python_module=self._training_python_module,
                    training_args=self._training_args,
                    labels=self._labels,
                    gcp_conn_id=self._gcp_conn_id,
                    impersonation_chain=self._impersonation_chain,
                ),
                method_name="execute_complete",
            )
        else:
            finished_training_job = self._wait_for_job_done(self._project_id, self.job_id)
            self._handle_job_error(finished_training_job)
            gcp_metadata = {
                "job_id": self.job_id,
                "project_id": self._project_id,
            }
            context["task_instance"].xcom_push("gcp_metadata", gcp_metadata)

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineJobDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                job_id=job_id,
            )

    def _wait_for_job_done(self, project_id: str, job_id: str, interval: int = 30):
        """
        Waits for the Job to reach a terminal state.

        This method will periodically check the job state until the job reach
        a terminal state.

        :param project_id: The project in which the Job is located. If set to None or missing, the default
            project_id from the Google Cloud connection is used. (templated)
        :param job_id: A unique id for the Google MLEngine job. (templated)
        :param interval: Time expressed in seconds after which the job status is checked again. (templated)
        :raises: googleapiclient.errors.HttpError
        """
        self.log.info("Waiting for job. job_id=%s", job_id)

        if interval <= 0:
            raise ValueError("Interval must be > 0")
        while True:
            job = self.hook.get_job(project_id, job_id)
            if job["state"] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                return job
            time.sleep(interval)

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        if self._project_id:
            MLEngineJobDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=self._project_id,
                job_id=self._job_id,
            )

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(job_id=self.job_id, project_id=self._project_id)  # type: ignore[union-attr]
        else:
            self.log.info("Skipping to cancel job: %s:%s.%s", self._project_id, self.job_id)


class MLEngineTrainingCancelJobOperator(GoogleCloudBaseOperator):
    """
    Operator for cleaning up failed MLEngine training job.

    :param job_id: A unique templated id for the submitted Google MLEngine
        training job. (templated)
    :param project_id: The Google Cloud project name within which MLEngine training job should run.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "_project_id",
        "_job_id",
        "_impersonation_chain",
    )
    operator_extra_links = (MLEngineJobSListLink(),)

    def __init__(
        self,
        *,
        job_id: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._job_id = job_id
        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain

        if not self._project_id:
            raise AirflowException("Google Cloud project id is required.")

    def execute(self, context: Context):

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            impersonation_chain=self._impersonation_chain,
        )

        project_id = self._project_id or hook.project_id
        if project_id:
            MLEngineJobSListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )

        hook.cancel_job(project_id=self._project_id, job_id=_normalize_mlengine_job_id(self._job_id))
