#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re

from googleapiclient.errors import HttpError

from airflow.contrib.hooks.gcp_mlengine_hook import MLEngineHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _normalize_mlengine_job_id(job_id):
    """
    Replaces invalid MLEngine job_id characters with '_'.

    This also adds a leading 'z' in case job_id starts with an invalid
    character.

    Args:
        job_id: A job_id str that may have invalid characters.

    Returns:
        A valid job_id representation.
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
    for m in re.finditer(r'\{{2}.+?\}{2}', job):
        cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_',
                                  job[tracker:m.start()])
        cleansed_job_id += job[m.start():m.end()]
        tracker = m.end()

    # Clean up last substring or the full string if no templates
    cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_', job[tracker:])

    return cleansed_job_id


class MLEngineBatchPredictionOperator(BaseOperator):
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

    :param project_id: The Google Cloud project name where the
        prediction job is submitted. (templated)
    :type project_id: str

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
        for parallel processing. Defaults to 10 if not specified.
    :type max_worker_count: int

    :param runtime_version: The Google Cloud ML Engine runtime version to use
        for batch prediction.
    :type runtime_version: str

    :param signature_name: The name of the signature defined in the SavedModel
        to use for this job.
    :type signature_name: str

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
    def __init__(self,
                 project_id,
                 job_id,
                 region,
                 data_format,
                 input_paths,
                 output_path,
                 model_name=None,
                 version_name=None,
                 uri=None,
                 max_worker_count=None,
                 runtime_version=None,
                 signature_name=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
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

        try:
            finished_prediction_job = hook.create_job(
                self._project_id, prediction_request, check_existing_job)
        except HttpError:
            raise

        if finished_prediction_job['state'] != 'SUCCEEDED':
            self.log.error(
                'MLEngine batch prediction job failed: %s', str(finished_prediction_job)
            )
            raise RuntimeError(finished_prediction_job['errorMessage'])

        return finished_prediction_job['predictionOutput']


class MLEngineModelOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML Engine model.

    :param project_id: The Google Cloud project name to which MLEngine
        model belongs. (templated)
    :type project_id: str
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
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_model',
    ]

    @apply_defaults
    def __init__(self,
                 project_id,
                 model,
                 operation='create',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model = model
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        if self._operation == 'create':
            return hook.create_model(self._project_id, self._model)
        elif self._operation == 'get':
            return hook.get_model(self._project_id, self._model['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class MLEngineVersionOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML Engine version.

    :param project_id: The Google Cloud project name to which MLEngine
        model belongs.
    :type project_id: str

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

        *   ``get``: Gets full information of a particular version in the model
            specified by `model_name`.
            The name of the version should be specified in the `version`
            parameter.

        *   ``list``: Lists all available versions of the model specified
            by `model_name`.

        *   ``delete``: Deletes the version specified in `version` parameter from the
            model specified by `model_name`).
            The name of the version should be specified in the `version`
            parameter.
    :type operation: str

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = [
        '_model_name',
        '_version_name',
        '_version',
    ]

    @apply_defaults
    def __init__(self,
                 project_id,
                 model_name,
                 version_name=None,
                 version=None,
                 operation='create',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self._project_id = project_id
        self._model_name = model_name
        self._version_name = version_name
        self._version = version or {}
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to

    def execute(self, context):
        if 'name' not in self._version:
            self._version['name'] = self._version_name

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        if self._operation == 'create':
            if not self._version:
                raise ValueError("version attribute of {} could not "
                                 "be empty".format(self.__class__.__name__))
            return hook.create_version(self._project_id, self._model_name,
                                       self._version)
        elif self._operation == 'set_default':
            return hook.set_default_version(self._project_id, self._model_name,
                                            self._version['name'])
        elif self._operation == 'list':
            return hook.list_versions(self._project_id, self._model_name)
        elif self._operation == 'delete':
            return hook.delete_version(self._project_id, self._model_name,
                                       self._version['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class MLEngineTrainingOperator(BaseOperator):
    """
    Operator for launching a MLEngine training job.

    :param project_id: The Google Cloud project name within which MLEngine
        training job should run (templated).
    :type project_id: str

    :param job_id: A unique templated id for the submitted Google MLEngine
        training job. (templated)
    :type job_id: str

    :param package_uris: A list of package locations for MLEngine training job,
        which should include the main training program + any additional
        dependencies. (templated)
    :type package_uris: str

    :param training_python_module: The Python module name to run within MLEngine
        training job after installing 'package_uris' packages. (templated)
    :type training_python_module: str

    :param training_args: A list of templated command line arguments to pass to
        the MLEngine training program. (templated)
    :type training_args: str

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
    def __init__(self,
                 project_id,
                 job_id,
                 package_uris,
                 training_python_module,
                 training_args,
                 region,
                 scale_tier=None,
                 master_type=None,
                 runtime_version=None,
                 python_version=None,
                 job_dir=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 mode='PRODUCTION',
                 *args,
                 **kwargs):
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

        try:
            finished_training_job = hook.create_job(
                self._project_id, training_request, check_existing_job)
        except HttpError:
            raise

        if finished_training_job['state'] != 'SUCCEEDED':
            self.log.error('MLEngine training job failed: %s', str(finished_training_job))
            raise RuntimeError(finished_training_job['errorMessage'])
