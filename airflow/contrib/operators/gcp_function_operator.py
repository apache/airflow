# -*- coding: utf-8 -*-
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
import re

from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.utils.gcp_field_validator import GcpBodyFieldValidator, \
    GcpFieldValidationException
from airflow.version import version
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcp_function_hook import GcfHook
from airflow.utils.decorators import apply_defaults


def _validate_available_memory_in_mb(value):
    if int(value) <= 0:
        raise GcpFieldValidationException("The available memory has to be greater than 0")


def _validate_max_instances(value):
    if int(value) <= 0:
        raise GcpFieldValidationException(
            "The max instances parameter has to be greater than 0")


CLOUD_FUNCTION_VALIDATION = [
    dict(name="name", regexp="^.+$"),
    dict(name="description", regexp="^.+$", optional=True),
    dict(name="entryPoint", regexp=r'^.+$', optional=True),
    dict(name="runtime", regexp=r'^.+$', optional=True),
    dict(name="timeout", regexp=r'^.+$', optional=True),
    dict(name="availableMemoryMb", custom_validation=_validate_available_memory_in_mb,
         optional=True),
    dict(name="labels", optional=True),
    dict(name="environmentVariables", optional=True),
    dict(name="network", regexp=r'^.+$', optional=True),
    dict(name="maxInstances", optional=True, custom_validation=_validate_max_instances),

    dict(name="source_code", type="union", fields=[
        dict(name="sourceArchiveUrl", regexp=r'^.+$'),
        dict(name="sourceRepositoryUrl", regexp=r'^.+$', api_version='v1beta2'),
        dict(name="sourceRepository", type="dict", fields=[
            dict(name="url", regexp=r'^.+$')
        ]),
        dict(name="sourceUploadUrl")
    ]),

    dict(name="trigger", type="union", fields=[
        dict(name="httpsTrigger", type="dict", fields=[
            # This dict should be empty at input (url is added at output)
        ]),
        dict(name="eventTrigger", type="dict", fields=[
            dict(name="eventType", regexp=r'^.+$'),
            dict(name="resource", regexp=r'^.+$'),
            dict(name="service", regexp=r'^.+$', optional=True),
            dict(name="failurePolicy", type="dict", optional=True, fields=[
                dict(name="retry", type="dict", optional=True)
            ])
        ])
    ]),
]


class GcfFunctionDeployOperator(BaseOperator):
    """
    Create a function in Google Cloud Functions.

    :param project_id: Project ID that the operator works on
    :type project_id: str
    :param location: Region where the operator operates on
    :type location: str
    :param body: Body of the cloud function definition. The body must be a CloudFunction
        dictionary as described in:
        https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions
        (note that different API versions require different
        variants of the CloudFunction dictionary)
    :type body: dict or google.cloud.functions.v1.CloudFunction
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: Version of the API used (for example v1).
    :type api_version: str
    :param zip_path: Path to zip file containing source code of the function. If it is
        set, then sourceUploadUrl should not be specified in the body (or it should
        be empty), then the zip file will be uploaded using upload URL generated
        via generateUploadUrl from cloud functions API
    :type zip_path: str
    :param validate_body: If set to False, no body validation is performed.
    :type validate_body: bool
    """

    @apply_defaults
    def __init__(self,
                 project_id,
                 location,
                 body,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 zip_path=None,
                 validate_body=True,
                 *args, **kwargs):
        self.project_id = project_id
        self.location = location
        self.full_location = 'projects/{}/locations/{}'.format(self.project_id,
                                                               self.location)
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.zip_path = zip_path
        self.zip_path_preprocessor = ZipPathPreprocessor(body, zip_path)
        self._field_validator = None
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(CLOUD_FUNCTION_VALIDATION,
                                                          api_version=api_version)
        self._hook = GcfHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        self._validate_inputs()
        super(GcfFunctionDeployOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.location:
            raise AirflowException("The required parameter 'location' is missing")
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        self.zip_path_preprocessor.preprocess_body()

    def _validate_all_body_fields(self):
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _create_new_function(self):
        self._hook.create_new_function(self.full_location, self.body)

    def _update_function(self):
        self._hook.update_function(self.body['name'], self.body, self.body.keys())

    def _check_if_function_exists(self):
        name = self.body.get('name')
        if not name:
            raise GcpFieldValidationException("The 'name' field should be present in "
                                              "body: '{}'.".format(self.body))
        try:
            self._hook.get_function(name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e
        return True

    def _upload_source_code(self):
        return self._hook.upload_function_zip(parent=self.full_location,
                                              zip_path=self.zip_path)

    def _set_airflow_version_label(self):
        if 'labels' not in self.body.keys():
            self.body['labels'] = {}
        self.body['labels'].update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})

    def execute(self, context):
        if self.zip_path_preprocessor.should_upload_function():
            self.body[SOURCE_UPLOAD_URL] = self._upload_source_code()
        self._validate_all_body_fields()
        self._set_airflow_version_label()
        if not self._check_if_function_exists():
            self._create_new_function()
        else:
            self._update_function()


SOURCE_ARCHIVE_URL = 'sourceArchiveUrl'
SOURCE_UPLOAD_URL = 'sourceUploadUrl'
SOURCE_REPOSITORY = 'sourceRepository'
ZIP_PATH = 'zip_path'


class ZipPathPreprocessor:
    """
    Pre-processes zip path parameter.

    Responsible for checking if the zip path parameter is correctly specified in
    relation with source_code body fields. Non empty zip path parameter is special because
    it is mutually exclusive with sourceArchiveUrl and sourceRepository body fields.
    It is also mutually exclusive with non-empty sourceUploadUrl.
    The pre-process modifies sourceUploadUrl body field in special way when zip_path
    is not empty. An extra step is run when execute method is called and sourceUploadUrl
    field value is set in the body with the value returned by generateUploadUrl Cloud
    Function API method.

    :param body: Body passed to the create/update method calls.
    :type body: dict
    :param zip_path: path to the zip file containing source code.
    :type body: dict

    """
    upload_function = None

    def __init__(self, body, zip_path):
        self.body = body
        self.zip_path = zip_path

    @staticmethod
    def _is_present_and_empty(dictionary, field):
        return field in dictionary and not dictionary[field]

    def _verify_upload_url_and_no_zip_path(self):
        if self._is_present_and_empty(self.body, SOURCE_UPLOAD_URL):
            if not self.zip_path:
                raise AirflowException(
                    "Parameter '{}' is empty in the body and argument '{}' "
                    "is missing or empty. You need to have non empty '{}' "
                    "when '{}' is present and empty.".
                    format(SOURCE_UPLOAD_URL, ZIP_PATH, ZIP_PATH, SOURCE_UPLOAD_URL))

    def _verify_upload_url_and_zip_path(self):
        if SOURCE_UPLOAD_URL in self.body and self.zip_path:
            if not self.body[SOURCE_UPLOAD_URL]:
                self.upload_function = True
            else:
                raise AirflowException("Only one of '{}' in body or '{}' argument "
                                       "allowed. Found both."
                                       .format(SOURCE_UPLOAD_URL, ZIP_PATH))

    def _verify_archive_url_and_zip_path(self):
        if SOURCE_ARCHIVE_URL in self.body and self.zip_path:
            raise AirflowException("Only one of '{}' in body or '{}' argument "
                                   "allowed. Found both."
                                   .format(SOURCE_ARCHIVE_URL, ZIP_PATH))

    def should_upload_function(self):
        if self.upload_function is None:
            raise AirflowException('validate() method has to be invoked before '
                                   'should_upload_function')
        return self.upload_function

    def preprocess_body(self):
        self._verify_archive_url_and_zip_path()
        self._verify_upload_url_and_zip_path()
        self._verify_upload_url_and_no_zip_path()
        if self.upload_function is None:
            self.upload_function = False


FUNCTION_NAME_PATTERN = '^projects/[^/]+/locations/[^/]+/functions/[^/]+$'
FUNCTION_NAME_COMPILED_PATTERN = re.compile(FUNCTION_NAME_PATTERN)


class GcfFunctionDeleteOperator(BaseOperator):
    """
    Delete a function with specified name from Google Cloud Functions.

    :param name: A fully-qualified function name, matching
        the pattern: `^projects/[^/]+/locations/[^/]+/functions/[^/]+$`
    :type name: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: Version of the API used (for example v1).
    :type api_version: str
    """

    @apply_defaults
    def __init__(self,
                 name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        self.name = name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self.hook = GcfHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        super(GcfFunctionDeleteOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.name:
            raise AttributeError('Empty parameter: name')
        else:
            pattern = FUNCTION_NAME_COMPILED_PATTERN
            if not pattern.match(self.name):
                raise AttributeError(
                    'Parameter name must match pattern: {}'.format(FUNCTION_NAME_PATTERN))

    def execute(self, context):
        try:
            return self.hook.delete_function(self.name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                self.log.info('The function does not exist in this project')
            else:
                self.log.error('An error occurred. Exiting.')
                raise e
