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
"""
This module contains GCP MLEngine operators.
"""
import re
import warnings
from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.gcp.hooks.mlengine import MLEngineHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _normalize_mlengine_job_id(job_id: str) -> str:
    """
    Replaces invalid MLEngine job_id characters with '_'.

    This also adds a leading 'z' in case job_id starts with an invalid
    character.

    :param job_id: A job_id str that may have invalid characters.
    :type job_id: str:
    :return: A valid job_id representation.
    :rtype: str
    """

    # Add a prefix when a job_id starts with a digit or a template
    match = re.search(r'\d|\{{2}', job_id)
    if match and match.start() == 0:
        job = 'z_{}'.format(job_id)
    else:
        job = job_id

    # Clean up 'bad' characters except templates
    tracker = 0
    cleansed_job_id = ''
    for match in re.finditer(r'\{{2}.+?\}{2}', job):
        cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_',
                                  job[tracker:match.start()])
        cleansed_job_id += job[match.start():match.end()]
        tracker = match.end()

    # Clean up last substring or the full string if no templates
    cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_', job[tracker:])

    return cleansed_job_id


class MLEngineStartBatchPredictionJobOperator(BaseOperator):
    """
    Start a Google Cloud ML Engine prediction job.

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
    :type job_id: str
    :param data_format: The format of the input data.
        It will default to 'DATA_FORMAT_UNSPECIFIED' if is not provided
        or is not one of ["TEXT", "TF_RECORD", "TF_RECORD_GZIP"].
    :type data_format: str
    :param input_paths: A list of GCS paths of input data for batch
        prediction. Accepting wildcard operator ``*``, but only at the end. (templated)
    :type input_paths: list[str]
    :param output_path: The GCS path where the prediction results are
        written to. (templated)
    :type output_path: str
    :param region: The Google Compute Engine region to run the
        prediction job in. (templated)
    :type region: str
    :param model_name: The Google Cloud ML Engine model to use for prediction.
        If version_name is not provided, the default version of this
        model will be used.
        Should not be None if version_name is provided.
        Should be None if uri is provided. (templated)
    :type model_name: str
    :param version_name: The Google Cloud ML Engine model version to use for
        prediction.
        Should be None if uri is provided. (templated)
    :type version_name: str
    :param uri: The GCS path of the saved model to use for prediction.
        Should be None if model_name is provided.
        It should be a GCS path pointing to a tensorflow SavedModel. (templated)
    :type uri: str
    :param max_worker_count: The maximum number of workers to be used
        for parallel processing. Defaults to 10 if not specified. Should be a
        string representing the worker count ("10" instead of 10, "50" instead
        of 50, etc.)
    :type max_worker_count: str
    :param runtime_version: The Google Cloud ML Engine runtime version to use
        for batch prediction.
    :type runtime_version: str
    :param signature_name: The name of the signature defined in the SavedModel
        to use for this job.
    :type signature_name: str
    :param project_id: The Google Cloud project name where the prediction job is submitted.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID used for connection to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :type delegate_to: str
    :raises: ``ValueError``: if a unique model/version origin cannot be
        determined.
    """

    template_fields = [
        '_project_id',
        '_job_id',
        '_region',
        '_input_paths',
        '_output_path',
        '_model_name',
        '_version_name',
        '_uri',
    ]

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 job_id: str,
                 region: str,
                 data_format: str,
                 input_paths: List[str],
                 output_path: str,
                 model_name: Optional[str] = None,
                 version_name: Optional[str] = None,
                 uri: Optional[str] = None,
                 max_worker_count: Optional[int] = None,
                 runtime_version: Optional[str] = None,
                 signature_name: Optional[str] = None,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

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
        self._delegate_to = delegate_to

        if not self._project_id:
            raise AirflowException('Google Cloud project id is required.')
        if not self._job_id:
            raise AirflowException(
                'An unique job id is required for Google MLEngine prediction '
                'job.')

        if self._uri:
            if self._model_name or self._version_name:
                raise AirflowException('Ambiguous model origin: Both uri and '
                                       'model/version name are provided.')

        if self._version_name and not self._model_name:
            raise AirflowException(
                'Missing model: Batch prediction expects '
                'a model name when a version name is provided.')

        if not (self._uri or self._model_name):
            raise AirflowException(
                'Missing model origin: Batch prediction expects a model, '
                'a model & version combination, or a URI to a savedModel.')

    def execute(self, context):
        job_id = _normalize_mlengine_job_id(self._job_id)
        prediction_request = {
            'jobId': job_id,
            'predictionInput': {
                'dataFormat': self._data_format,
                'inputPaths': self._input_paths,
                'outputPath': self._output_path,
                'region': self._region
            }
        }

        if self._uri:
            prediction_request['predictionInput']['uri'] = self._uri
        elif self._model_name:
            origin_name = 'projects/{}/models/{}'.format(
                self._project_id, self._model_name)
            if not self._version_name:
                prediction_request['predictionInput'][
                    'modelName'] = origin_name
            else:
                prediction_request['predictionInput']['versionName'] = \
                    origin_name + '/versions/{}'.format(self._version_name)

        if self._max_worker_count:
            prediction_request['predictionInput'][
                'maxWorkerCount'] = self._max_worker_count

        if self._runtime_version:
            prediction_request['predictionInput'][
                'runtimeVersion'] = self._runtime_version

        if self._signature_name:
            prediction_request['predictionInput'][
                'signatureName'] = self._signature_name

        hook = MLEngineHook(self._gcp_conn_id, self._delegate_to)

        # Helper method to check if the existing job's prediction input is the
        # same as the request we get here.
        def check_existing_job(existing_job):
            return existing_job.get('predictionInput', None) == \
                prediction_request['predictionInput']

        finished_prediction_job = hook.create_job(
            project_id=self._project_id, job=prediction_request, use_existing_job_fn=check_existing_job
        )

        if finished_prediction_job['state'] != 'SUCCEEDED':
            self.log.error(
                'MLEngine batch prediction job failed: %s', str(finished_prediction_job)
            )
            raise RuntimeError(finished_prediction_job['errorMessage'])

        return finished_prediction_job['predictionOutput']


class MLEngineManageModelOperator(BaseOperator):
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
    :type model: dict
    :param operation: The operation to perform. Available operations are:

        * ``create``: Creates a new model as provided by the `model` parameter.
        * ``get``: Gets a particular model where the name is specified in `model`.
    :type operation: str
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model',
    ]

    @apply_defaults
    def __init__(self,
                 model: dict,
                 operation: str = 'create',
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        warnings.warn(
            "This operator is deprecated. Consider using operators for specific operations: "
            "MLEngineCreateModelOperator, MLEngineGetModelOperator.",
            DeprecationWarning,
            stacklevel=3
        )

        self._project_id = project_id
        self._model = model
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        if self._operation == 'create':
            return hook.create_model(project_id=self._project_id, model=self._model)
        elif self._operation == 'get':
            return hook.get_model(project_id=self._project_id, model_name=self._model['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class MLEngineCreateModelOperator(BaseOperator):
    """
    Creates a new model.

    The model should be provided by the `model` parameter.

    :param model: A dictionary containing the information about the model.
    :type model: dict
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model',
    ]

    @apply_defaults
    def __init__(self,
                 model: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model = model
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        return hook.create_model(project_id=self._project_id, model=self._model)


class MLEngineGetModelOperator(BaseOperator):
    """
    Gets a particular model

    The name of model shold be specified in `model_name`.

    :param model_name: The name of the model.
    :type model_name: str
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        return hook.get_model(project_id=self._project_id, model_name=self._model_name)


class MLEngineDeleteModelOperator(BaseOperator):
    """
    Deletes a model.

    The model should be provided by the `model_name` parameter.

    :param model_name: The name of the model.
    :type model_name: str
    :param delete_contents: (Optional) Whether to force the deletion even if the models is not empty.
        Will delete all version (if any) in the dataset if set to True.
        The default value is False.
    :type delete_contents: bool
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 delete_contents: bool = False,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._delete_contents = delete_contents
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        return hook.delete_model(
            project_id=self._project_id, model_name=self._model_name, delete_contents=self._delete_contents
        )


class MLEngineManageVersionOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML Engine version.

    .. warning::
       This operator is deprecated. Consider using operators for specific operations:
       MLEngineCreateVersionOperator, MLEngineSetDefaultVersionOperator,
       MLEngineListVersionsOperator, MLEngineDeleteVersionOperator.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :type model_name: str
    :param version_name: A name to use for the version being operated upon.
        If not None and the `version` argument is None or does not have a value for
        the `name` key, then this will be populated in the payload for the
        `name` key. (templated)
    :type version_name: str
    :param version: A dictionary containing the information about the version.
        If the `operation` is `create`, `version` should contain all the
        information about this version such as name, and deploymentUrl.
        If the `operation` is `get` or `delete`, the `version` parameter
        should contain the `name` of the version.
        If it is None, the only `operation` possible would be `list`. (templated)
    :type version: dict
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
    :type operation: str
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model_name',
        '_version_name',
        '_version',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 version_name: Optional[str] = None,
                 version: Optional[dict] = None,
                 operation: str = 'create',
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._version = version or {}
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

        warnings.warn(
            "This operator is deprecated. Consider using operators for specific operations: "
            "MLEngineCreateVersion, MLEngineSetDefaultVersion, MLEngineListVersions, MLEngineDeleteVersion.",
            DeprecationWarning,
            stacklevel=3
        )

    def execute(self, context):
        if 'name' not in self._version:
            self._version['name'] = self._version_name

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        if self._operation == 'create':
            if not self._version:
                raise ValueError("version attribute of {} could not "
                                 "be empty".format(self.__class__.__name__))
            return hook.create_version(
                project_id=self._project_id,
                model_name=self._model_name,
                version_spec=self._version
            )
        elif self._operation == 'set_default':
            return hook.set_default_version(
                project_id=self._project_id,
                model_name=self._model_name,
                version_name=self._version['name']
            )
        elif self._operation == 'list':
            return hook.list_versions(
                project_id=self._project_id,
                model_name=self._model_name
            )
        elif self._operation == 'delete':
            return hook.delete_version(
                project_id=self._project_id,
                model_name=self._model_name,
                version_name=self._version['name']
            )
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class MLEngineCreateVersionOperator(BaseOperator):
    """
    Creates a new version in the model

    Model should be specified by `model_name`, in which case the `version` parameter should contain all the
    information to create that version

    :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
    :type model_name: str
    :param version: A dictionary containing the information about the version. (templated)
    :type version: dict
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model_name',
        '_version',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 version: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version = version
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version:
            raise AirflowException("The version parameter could not be empty.")

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        return hook.create_version(
            project_id=self._project_id,
            model_name=self._model_name,
            version_spec=self._version
        )


class MLEngineSetDefaultVersionOperator(BaseOperator):
    """
    Sets a version in the model.

    The model should be specified by `model_name` to be the default. The name of the version should be
    specified in the `version_name` parameter.

    :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
    :type model_name: str
    :param version_name: A name to use for the version being operated upon. (templated)
    :type version_name: str
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_project_id',
        '_model_name',
        '_version_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 version_name: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version_name:
            raise AirflowException("The version_name parameter could not be empty.")

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        return hook.set_default_version(
            project_id=self._project_id,
            model_name=self._model_name,
            version_name=self._version_name
        )


class MLEngineListVersionsOperator(BaseOperator):
    """
    Lists all available versions of the model

    The model should be specified by `model_name`.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :type model_name: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param project_id: The Google Cloud project name to which MLEngine model belongs.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = [
        '_project_id',
        '_model_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        return hook.list_versions(
            project_id=self._project_id,
            model_name=self._model_name,
        )


class MLEngineDeleteVersionOperator(BaseOperator):
    """
    Deletes the version from the model.

    The name of the version should be specified in `version_name` parameter from the model specified
    by `model_name`.

    :param model_name: The name of the Google Cloud ML Engine model that the version
        belongs to. (templated)
    :type model_name: str
    :param version_name: A name to use for the version being operated upon. (templated)
    :type version_name: str
    :param project_id: The Google Cloud project name to which MLEngine
        model belongs.
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = [
        '_project_id',
        '_model_name',
        '_version_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name: str,
                 version_name: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._validate_inputs()

    def _validate_inputs(self):
        if not self._model_name:
            raise AirflowException("The model_name parameter could not be empty.")

        if not self._version_name:
            raise AirflowException("The version_name parameter could not be empty.")

    def execute(self, context):
        hook = MLEngineHook(gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        return hook.delete_version(
            project_id=self._project_id,
            model_name=self._model_name,
            version_name=self._version_name
        )


class MLEngineStartTrainingJobOperator(BaseOperator):
    """
    Operator for launching a MLEngine training job.

    :param job_id: A unique templated id for the submitted Google MLEngine
        training job. (templated)
    :type job_id: str
    :param package_uris: A list of package locations for MLEngine training job,
        which should include the main training program + any additional
        dependencies. (templated)
    :type package_uris: List[str]
    :param training_python_module: The Python module name to run within MLEngine
        training job after installing 'package_uris' packages. (templated)
    :type training_python_module: str
    :param training_args: A list of templated command line arguments to pass to
        the MLEngine training program. (templated)
    :type training_args: List[str]
    :param region: The Google Compute Engine region to run the MLEngine training
        job in (templated).
    :type region: str
    :param scale_tier: Resource tier for MLEngine training job. (templated)
    :type scale_tier: str
    :param master_type: Cloud ML Engine machine name.
        Must be set when scale_tier is CUSTOM. (templated)
    :type master_type: str
    :param runtime_version: The Google Cloud ML runtime version to use for
        training. (templated)
    :type runtime_version: str
    :param python_version: The version of Python used in training. (templated)
    :type python_version: str
    :param job_dir: A Google Cloud Storage path in which to store training
        outputs and other data needed for training. (templated)
    :type job_dir: str
    :param project_id: The Google Cloud project name within which MLEngine training job should run.
        If set to None or missing, the default project_id from the GCP connection is used. (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param mode: Can be one of 'DRY_RUN'/'CLOUD'. In 'DRY_RUN' mode, no real
        training job will be launched, but the MLEngine training job request
        will be printed out. In 'CLOUD' mode, a real MLEngine training job
        creation request will be issued.
    :type mode: str
    """

    template_fields = [
        '_project_id',
        '_job_id',
        '_package_uris',
        '_training_python_module',
        '_training_args',
        '_region',
        '_scale_tier',
        '_master_type',
        '_runtime_version',
        '_python_version',
        '_job_dir'
    ]

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 job_id: str,
                 package_uris: List[str],
                 training_python_module: str,
                 training_args: List[str],
                 region: str,
                 scale_tier: Optional[str] = None,
                 master_type: Optional[str] = None,
                 runtime_version: Optional[str] = None,
                 python_version: Optional[str] = None,
                 job_dir: Optional[str] = None,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 mode: str = 'PRODUCTION',
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._job_id = job_id
        self._package_uris = package_uris
        self._training_python_module = training_python_module
        self._training_args = training_args
        self._region = region
        self._scale_tier = scale_tier
        self._master_type = master_type
        self._runtime_version = runtime_version
        self._python_version = python_version
        self._job_dir = job_dir
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._mode = mode

        if not self._project_id:
            raise AirflowException('Google Cloud project id is required.')
        if not self._job_id:
            raise AirflowException(
                'An unique job id is required for Google MLEngine training '
                'job.')
        if not package_uris:
            raise AirflowException(
                'At least one python package is required for MLEngine '
                'Training job.')
        if not training_python_module:
            raise AirflowException(
                'Python module name to run after installing required '
                'packages is required.')
        if not self._region:
            raise AirflowException('Google Compute Engine region is required.')
        if self._scale_tier is not None and self._scale_tier.upper() == "CUSTOM" and not self._master_type:
            raise AirflowException(
                'master_type must be set when scale_tier is CUSTOM')

    def execute(self, context):
        job_id = _normalize_mlengine_job_id(self._job_id)
        training_request = {
            'jobId': job_id,
            'trainingInput': {
                'scaleTier': self._scale_tier,
                'packageUris': self._package_uris,
                'pythonModule': self._training_python_module,
                'region': self._region,
                'args': self._training_args,
            }
        }

        if self._runtime_version:
            training_request['trainingInput']['runtimeVersion'] = self._runtime_version

        if self._python_version:
            training_request['trainingInput']['pythonVersion'] = self._python_version

        if self._job_dir:
            training_request['trainingInput']['jobDir'] = self._job_dir

        if self._scale_tier is not None and self._scale_tier.upper() == "CUSTOM":
            training_request['trainingInput']['masterType'] = self._master_type

        if self._mode == 'DRY_RUN':
            self.log.info('In dry_run mode.')
            self.log.info('MLEngine Training job request is: %s', training_request)
            return

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        # Helper method to check if the existing job's training input is the
        # same as the request we get here.
        def check_existing_job(existing_job):
            return existing_job.get('trainingInput', None) == \
                training_request['trainingInput']

        finished_training_job = hook.create_job(
            project_id=self._project_id, job=training_request, use_existing_job_fn=check_existing_job
        )

        if finished_training_job['state'] != 'SUCCEEDED':
            self.log.error('MLEngine training job failed: %s', str(finished_training_job))
            raise RuntimeError(finished_training_job['errorMessage'])
