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

import logging
import re

from airflow import settings
from airflow.contrib.hooks.gcp_cloudml_hook import CloudMLHook
from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from apiclient import errors


logging.getLogger('GoogleCloudML').setLevel(settings.LOGGING_LEVEL)


def _create_prediction_input(project_id,
                             region,
                             data_format,
                             input_paths,
                             output_path,
                             model_name=None,
                             version_name=None,
                             uri=None,
                             max_worker_count=None,
                             runtime_version=None):
    """
    Create the batch prediction input from the given parameters.

    Args:
        A subset of arguments documented in __init__ method of class
        CloudMLBatchPredictionOperator

    Returns:
        A dictionary representing the predictionInput object as documented
        in https://cloud.google.com/ml-engine/reference/rest/v1/projects.jobs.

    Raises:
        ValueError: if a unique model/version origin cannot be determined.
    """

    prediction_input = {
        'dataFormat': data_format,
        'inputPaths': input_paths,
        'outputPath': output_path,
        'region': region
    }

    if uri:
        if model_name or version_name:
            logging.error(
                'Ambiguous model origin: Both uri and model/version name are '
                'provided.')
            raise ValueError('Ambiguous model origin.')
        prediction_input['uri'] = uri
    elif model_name:
        origin_name = 'projects/{}/models/{}'.format(project_id, model_name)
        if not version_name:
            prediction_input['modelName'] = origin_name
        else:
            prediction_input['versionName'] = \
                origin_name + '/versions/{}'.format(version_name)
    else:
        logging.error(
            'Missing model origin: Batch prediction expects a model, '
            'a model & version combination, or a URI to savedModel.')
        raise ValueError('Missing model origin.')

    if max_worker_count:
        prediction_input['maxWorkerCount'] = max_worker_count
    if runtime_version:
        prediction_input['runtimeVersion'] = runtime_version

    return prediction_input


def _normalize_cloudml_job_id(job_id):
    """
    Replaces invalid CloudML job_id characters with '_'.

    This also adds a leading 'z' in case job_id starts with an invalid
    character.

    Args:
        job_id: A job_id str that may have invalid characters.

    Returns:
        A valid job_id representation.
    """
    match = re.search(r'\d', job_id)
    if match and match.start() is 0:
        job_id = 'z_{}'.format(job_id)
    return re.sub('[^0-9a-zA-Z]+', '_', job_id)


class CloudMLBatchPredictionOperator(BaseOperator):
    """
    Start a Cloud ML prediction job.

    NOTE: For model origin, users should consider exactly one from the
    three options below:
    1. Populate 'uri' field only, which should be a GCS location that
    points to a tensorflow savedModel directory.
    2. Populate 'model_name' field only, which refers to an existing
    model, and the default version of the model will be used.
    3. Populate both 'model_name' and 'version_name' fields, which
    refers to a specific version of a specific model.

    In options 2 and 3, both model and version name should contain the
    minimal identifier. For instance, call
        CloudMLBatchPredictionOperator(
            ...,
            model_name='my_model',
            version_name='my_version',
            ...)
    if the desired model version is
    "projects/my_project/models/my_model/versions/my_version".


    :param project_id: The Google Cloud project name where the
        prediction job is submitted.
    :type project_id: string

    :param job_id: A unique id for the prediction job on Google Cloud
        ML Engine.
    :type job_id: string

    :param data_format: The format of the input data.
        It will default to 'DATA_FORMAT_UNSPECIFIED' if is not provided
        or is not one of ["TEXT", "TF_RECORD", "TF_RECORD_GZIP"].
    :type data_format: string

    :param input_paths: A list of GCS paths of input data for batch
        prediction. Accepting wildcard operator *, but only at the end.
    :type input_paths: list of string

    :param output_path: The GCS path where the prediction results are
        written to.
    :type output_path: string

    :param region: The Google Compute Engine region to run the
        prediction job in.:
    :type region: string

    :param model_name: The Google Cloud ML model to use for prediction.
        If version_name is not provided, the default version of this
        model will be used.
        Should not be None if version_name is provided.
        Should be None if uri is provided.
    :type model_name: string

    :param version_name: The Google Cloud ML model version to use for
        prediction.
        Should be None if uri is provided.
    :type version_name: string

    :param uri: The GCS path of the saved model to use for prediction.
        Should be None if model_name is provided.
        It should be a GCS path pointing to a tensorflow SavedModel.
    :type uri: string

    :param max_worker_count: The maximum number of workers to be used
        for parallel processing. Defaults to 10 if not specified.
    :type max_worker_count: int

    :param runtime_version: The Google Cloud ML runtime version to use
        for batch prediction.
    :type runtime_version: string

    :param gcp_conn_id: The connection ID used for connection to Google
        Cloud Platform.
    :type gcp_conn_id: string

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have doamin-wide delegation enabled.
    :type delegate_to: string

    Raises:
        ValueError: if a unique model/version origin cannot be determined.
    """

    template_fields = [
        "prediction_job_request",
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
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(CloudMLBatchPredictionOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

        try:
            prediction_input = _create_prediction_input(
                project_id, region, data_format, input_paths, output_path,
                model_name, version_name, uri, max_worker_count,
                runtime_version)
        except ValueError as e:
            logging.error(
                'Cannot create batch prediction job request due to: {}'
                .format(str(e)))
            raise

        self.prediction_job_request = {
            'jobId': _normalize_cloudml_job_id(job_id),
            'predictionInput': prediction_input
        }

    def execute(self, context):
        hook = CloudMLHook(self.gcp_conn_id, self.delegate_to)

        def check_existing_job(existing_job):
            return existing_job.get('predictionInput', None) == \
                self.prediction_job_request['predictionInput']
        try:
            finished_prediction_job = hook.create_job(
                self.project_id,
                self.prediction_job_request,
                check_existing_job)
        except errors.HttpError:
            raise

        if finished_prediction_job['state'] != 'SUCCEEDED':
            logging.error(
                'Batch prediction job failed: %s',
                str(finished_prediction_job))
            raise RuntimeError(finished_prediction_job['errorMessage'])

        return finished_prediction_job['predictionOutput']


class CloudMLModelOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML model.

    :param model: A dictionary containing the information about the model.
        If the `operation` is `create`, then the `model` parameter should
        contain all the information about this model such as `name`.

        If the `operation` is `get`, the `model` parameter
        should contain the `name` of the model.
    :type model: dict

    :param project_id: The Google Cloud project name to which CloudML
        model belongs.
    :type project_id: string

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string

    :param operation: The operation to perform. Available operations are:
        'create': Creates a new model as provided by the `model` parameter.
        'get': Gets a particular model where the name is specified in `model`.

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    """

    template_fields = [
        '_model',
        '_model_name',
    ]

    @apply_defaults
    def __init__(self,
                 project_id,
                 model,
                 gcp_conn_id='google_cloud_default',
                 operation='create',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(CloudMLModelOperator, self).__init__(*args, **kwargs)
        self._model = model
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._project_id = project_id

    def execute(self, context):
        hook = CloudMLHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        if self._operation == 'create':
            hook.create_model(self._project_id, self._model)
        elif self._operation == 'get':
            hook.get_model(self._project_id, self._model['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class CloudMLVersionOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML version.

    :param model_name: The name of the Google Cloud ML model that the version
        belongs to.
    :type model_name: string

    :param project_id: The Google Cloud project name to which CloudML
        model belongs.
    :type project_id: string

    :param version: A dictionary containing the information about the version.
        If the `operation` is `create`, `version` should contain all the
        information about this version such as name, and deploymentUrl.
        If the `operation` is `get` or `delete`, the `version` parameter
        should contain the `name` of the version.
        If it is None, the only `operation` possible would be `list`.
    :type version: dict

    :param version_name: A name to use for the version being operated upon. If
        not None and the `version` argument is None or does not have a value for
        the `name` key, then this will be populated in the payload for the
        `name` key.
    :type version_name: string

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string

    :param operation: The operation to perform. Available operations are:
        'create': Creates a new version in the model specified by `model_name`,
            in which case the `version` parameter should contain all the
            information to create that version
            (e.g. `name`, `deploymentUrl`).
        'get': Gets full information of a particular version in the model
            specified by `model_name`.
            The name of the version should be specified in the `version`
            parameter.

        'list': Lists all available versions of the model specified
            by `model_name`.

        'delete': Deletes the version specified in `version` parameter from the
            model specified by `model_name`).
            The name of the version should be specified in the `version`
            parameter.
     :type operation: string

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    """

    template_fields = [
        '_model_name',
        '_version',
        '_version_name',
    ]

    @apply_defaults
    def __init__(self,
                 model_name,
                 project_id,
                 version=None,
                 version_name=None,
                 gcp_conn_id='google_cloud_default',
                 operation='create',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super(CloudMLVersionOperator, self).__init__(*args, **kwargs)
        self._model_name = model_name
        self._version = version or {}
        self._version_name = version_name
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._project_id = project_id
        self._operation = operation

    def execute(self, context):
        if 'name' not in self._version:
            self._version['name'] = self._version_name

        hook = CloudMLHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        if self._operation == 'create':
            assert self._version is not None
            return hook.create_version(self._project_id, self._model_name,
                                       self._version)
        elif self._operation == 'set_default':
            return hook.set_default_version(
                self._project_id, self._model_name,
                self._version['name'])
        elif self._operation == 'list':
            return hook.list_versions(self._project_id, self._model_name)
        elif self._operation == 'delete':
            return hook.delete_version(self._project_id, self._model_name,
                                       self._version['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class CloudMLTrainingOperator(BaseOperator):
    """
    Operator for launching a CloudML training job.

    :param project_id: The Google Cloud project name within which CloudML
        training job should run. This field could be templated.
    :type project_id: string

    :param job_id: A unique templated id for the submitted Google CloudML
        training job.
    :type job_id: string

    :param package_uris: A list of package locations for CloudML training job,
        which should include the main training program + any additional
        dependencies.
    :type package_uris: string

    :param training_python_module: The Python module name to run within CloudML
        training job after installing 'package_uris' packages.
    :type training_python_module: string

    :param training_args: A list of templated command line arguments to pass to
        the CloudML training program.
    :type training_args: string

    :param region: The Google Compute Engine region to run the CloudML training
        job in. This field could be templated.
    :type region: string

    :param scale_tier: Resource tier for CloudML training job.
    :type scale_tier: string

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string

    :param mode: Can be one of 'DRY_RUN'/'CLOUD'. In 'DRY_RUN' mode, no real
        training job will be launched, but the CloudML training job request
        will be printed out. In 'CLOUD' mode, a real CloudML training job
        creation request will be issued.
    :type mode: string
    """

    template_fields = [
        '_project_id',
        '_job_id',
        '_package_uris',
        '_training_python_module',
        '_training_args',
        '_region',
        '_scale_tier',
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
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 mode='PRODUCTION',
                 *args,
                 **kwargs):
        super(CloudMLTrainingOperator, self).__init__(*args, **kwargs)
        self._project_id = project_id
        self._job_id = job_id
        self._package_uris = package_uris
        self._training_python_module = training_python_module
        self._training_args = training_args
        self._region = region
        self._scale_tier = scale_tier
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._mode = mode

        if not self._project_id:
            raise AirflowException('Google Cloud project id is required.')
        if not self._job_id:
            raise AirflowException(
                'An unique job id is required for Google CloudML training '
                'job.')
        if not package_uris:
            raise AirflowException(
                'At least one python package is required for CloudML '
                'Training job.')
        if not training_python_module:
            raise AirflowException(
                'Python module name to run after installing required '
                'packages is required.')
        if not self._region:
            raise AirflowException('Google Compute Engine region is required.')

    def execute(self, context):
        job_id = _normalize_cloudml_job_id(self._job_id)
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

        if self._mode == 'DRY_RUN':
            logging.info('In dry_run mode.')
            logging.info(
                'CloudML Training job request is: {}'.format(training_request))
            return

        hook = CloudMLHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        # Helper method to check if the existing job's training input is the
        # same as the request we get here.
        def check_existing_job(existing_job):
            return existing_job.get('trainingInput', None) == \
                training_request['trainingInput']
        try:
            finished_training_job = hook.create_job(
                self._project_id, training_request, check_existing_job)
        except errors.HttpError:
            raise

        if finished_training_job['state'] != 'SUCCEEDED':
            logging.error('CloudML training job failed: {}'.format(
                str(finished_training_job)))
            raise RuntimeError(finished_training_job['errorMessage'])
