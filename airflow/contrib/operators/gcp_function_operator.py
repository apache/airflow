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

from airflow import AirflowException, LoggingMixin
from airflow.version import version
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcp_function_hook import GcfHook
from airflow.utils.decorators import apply_defaults

# TODO: This whole section should be extracted later to contrib/tools/field_validator.py

COMPOSITE_FIELD_TYPES = ['union', 'dict']


class FieldValidationException(AirflowException):
    """
    Thrown when validation finds dictionary field not valid according to specification.
    """

    def __init__(self, message):
        super(FieldValidationException, self).__init__(message)


class ValidationSpecificationException(AirflowException):
    """
    Thrown when validation specification is wrong
    (rather than dictionary being validated).
    This should only happen during development as ideally
     specification itself should not be invalid ;) .
    """

    def __init__(self, message):
        super(ValidationSpecificationException, self).__init__(message)


# TODO: make better description, add some examples
# TODO: move to contrib/utils folder when we reuse it.
class BodyFieldValidator(LoggingMixin):
    """
    Validates correctness of request body according to specification.
    The specification can describe various type of
    fields including custom validation, and union of fields. This validator is meant
    to be reusable by various operators
    in the near future, but for now it is left as part of the Google Cloud Function,
    so documentation about the
    validator is not yet complete. To see what kind of specification can be used,
    please take a look at
    gcp_function_operator.CLOUD_FUNCTION_VALIDATION which specifies validation
    for GCF deploy operator.

    :param validation_specs: dictionary describing validation specification
    :type validation_specs: [dict]
    :param api_version: Version of the api used (for example v1)
    :type api_version: str

    """
    def __init__(self, validation_specs, api_version):
        # type: ([dict], str) -> None
        super(BodyFieldValidator, self).__init__()
        self._validation_specs = validation_specs
        self._api_version = api_version

    @staticmethod
    def _get_field_name_with_parent(field_name, parent):
        if parent:
            return parent + '.' + field_name
        return field_name

    @staticmethod
    def _sanity_checks(children_validation_specs, field_type, full_field_path,
                       regexp, custom_validation, value):
        # type: (dict, str, str, str, function, object) -> None
        if value is None and field_type != 'union':
            raise FieldValidationException(
                "The required body field '{}' is missing. Please add it.".
                format(full_field_path))
        if regexp and field_type:
            raise ValidationSpecificationException(
                "The validation specification entry '{}' has both type and regexp. "
                "The regexp is only allowed without type (i.e. assume type is 'str' "
                "that can be validated with regexp)".format(full_field_path))
        if children_validation_specs and field_type not in COMPOSITE_FIELD_TYPES:
            raise ValidationSpecificationException(
                "Nested fields are specified in field '{}' of type '{}'. "
                "Nested fields are only allowed for fields of those types: ('{}').".
                format(full_field_path, field_type, COMPOSITE_FIELD_TYPES))
        if custom_validation and field_type:
            raise ValidationSpecificationException(
                "The validation specification field '{}' has both type and "
                "custom_validation. Custom validation is only allowed without type.".
                format(full_field_path))

    @staticmethod
    def _validate_regexp(full_field_path, regexp, value):
        # type: (str, str, str) -> None
        if not re.match(regexp, value):
            # Note matching of only the beginning as we assume the regexps all-or-nothing
            raise FieldValidationException(
                "The body field '{}' of value '{}' does not match the field "
                "specification regexp: '{}'.".
                format(full_field_path, value, regexp))

    def _validate_dict(self, children_validation_specs, full_field_path, value):
        # type: (dict, str, dict) -> None
        for child_validation_spec in children_validation_specs:
            self._validate_field(validation_spec=child_validation_spec,
                                 dictionary_to_validate=value,
                                 parent=full_field_path)
        for field_name in value.keys():
            if field_name not in [spec['name'] for spec in children_validation_specs]:
                self.log.warning(
                    "The field '{}' is in the body, but is not specified in the "
                    "validation specification '{}'. "
                    "This might be because you are using newer API version and "
                    "new field names defined for that version. Then the warning "
                    "can be safely ignored, or you might want to upgrade the operator"
                    "to the version that supports the new API version.".format(
                        self._get_field_name_with_parent(field_name, full_field_path),
                        children_validation_specs))

    def _validate_union(self, children_validation_specs, full_field_path,
                        dictionary_to_validate):
        # type: (dict, str, dict) -> None
        field_found = False
        found_field_name = None
        for child_validation_spec in children_validation_specs:
            # Forcing optional so that we do not have to type optional = True
            # in specification for all union fields
            new_field_found = self._validate_field(
                validation_spec=child_validation_spec,
                dictionary_to_validate=dictionary_to_validate,
                parent=full_field_path,
                force_optional=True)
            field_name = child_validation_spec['name']
            if new_field_found and field_found:
                raise FieldValidationException(
                    "The mutually exclusive fields '{}' and '{}' belonging to the "
                    "union '{}' are both present. Please remove one".
                    format(field_name, found_field_name, full_field_path))
            if new_field_found:
                field_found = True
                found_field_name = field_name
        if not field_found:
            self.log.warning(
                "There is no '{}' union defined in the body {}. "
                "Validation expected one of '{}' but could not find any. It's possible "
                "that you are using newer API version and there is another union variant "
                "defined for that version. Then the warning can be safely ignored, "
                "or you might want to upgrade the operator to the version that "
                "supports the new API version.".format(
                    full_field_path,
                    dictionary_to_validate,
                    [field['name'] for field in children_validation_specs]))

    def _validate_field(self, validation_spec, dictionary_to_validate, parent=None,
                        force_optional=False):
        """
        Validates if field is OK.
        :param validation_spec: specification of the field
        :type validation_spec: dict
        :param dictionary_to_validate: dictionary where the field should be present
        :type dictionary_to_validate: dict
        :param parent: full path of parent field
        :type parent: str
        :param force_optional: forces the field to be optional
          (all union fields have force_optional set to True)
        :type force_optional: bool
        :return: True if the field is present
        """
        field_name = validation_spec['name']
        field_type = validation_spec.get('type')
        optional = validation_spec.get('optional')
        regexp = validation_spec.get('regexp')
        children_validation_specs = validation_spec.get('fields')
        required_api_version = validation_spec.get('api_version')
        custom_validation = validation_spec.get('custom_validation')

        full_field_path = self._get_field_name_with_parent(field_name=field_name,
                                                           parent=parent)
        if required_api_version and required_api_version != self._api_version:
            self.log.debug(
                "Skipping validation of the field '{}' for API version '{}' "
                "as it is only valid for API version '{}'".
                format(field_name, self._api_version, required_api_version))
            return False
        value = dictionary_to_validate.get(field_name)

        if (optional or force_optional) and value is None:
            self.log.debug("The optional field '{}' is missing. That's perfectly OK.".
                           format(full_field_path))
            return False

        # Certainly down from here the field is present (value is not None)
        # so we should only return True from now on

        self._sanity_checks(children_validation_specs=children_validation_specs,
                            field_type=field_type,
                            full_field_path=full_field_path,
                            regexp=regexp,
                            custom_validation=custom_validation,
                            value=value)

        if regexp:
            self._validate_regexp(full_field_path, regexp, value)
        elif field_type == 'dict':
            if not isinstance(value, dict):
                raise FieldValidationException(
                    "The field '{}' should be dictionary type according to "
                    "specification '{}' but it is '{}'".
                    format(full_field_path, validation_spec, value))
            if children_validation_specs is None:
                self.log.debug(
                    "The dict field '{}' has no nested fields defined in the "
                    "specification '{}'. That's perfectly ok - it's content will "
                    "not be validated."
                        .format(full_field_path, validation_spec))
            else:
                self._validate_dict(children_validation_specs, full_field_path, value)
        elif field_type == 'union':
            if not children_validation_specs:
                raise ValidationSpecificationException(
                    "The union field '{}' has no nested fields "
                    "defined in specification '{}'. Unions should have at least one "
                    "nested field defined.".format(full_field_path, validation_spec))
            self._validate_union(children_validation_specs, full_field_path,
                                 dictionary_to_validate)
        elif custom_validation:
            try:
                custom_validation(value)
            except Exception as e:
                raise FieldValidationException(
                    "Error while validating custom field '{}' specified by '{}': '{}'".
                    format(full_field_path, validation_spec, e))
        elif field_type is None:
            self.log.debug("The type of field '{}' is not specified in '{}'. "
                           "Not validating its content.".
                           format(full_field_path, validation_spec))
        else:
            raise ValidationSpecificationException(
                "The field '{}' is of type '{}' in specification '{}'."
                "This type is unknown to validation!".format(
                    full_field_path, field_type, validation_spec))
        return True

    def validate(self, body_to_validate):
        """
        Validates if the body (dictionary) follows specification that the validator was
        instantiated with. Raises ValidationSpecificationException or
        ValidationFieldException in case of problems with specification or the
        body not conforming to the specification respectively.
        :param body_to_validate: body that must follow the specification
        :type body_to_validate: dict
        :return: None
        """
        try:
            for validation_spec in self._validation_specs:
                self._validate_field(validation_spec=validation_spec,
                                     dictionary_to_validate=body_to_validate)
        except FieldValidationException as e:
            raise FieldValidationException(
                "There was an error when validating: field '{}': '{}'".
                format(body_to_validate, e))

# TODO End of field validator to be extracted


def _validate_available_memory_in_mb(value):
    if int(value) <= 0:
        raise FieldValidationException("The available memory has to be greater than 0")


def _validate_max_instances(value):
    if int(value) <= 0:
        raise FieldValidationException(
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
        self.validate_body = validate_body
        self._field_validator = BodyFieldValidator(CLOUD_FUNCTION_VALIDATION,
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
        self._field_validator.validate(self.body)

    def _create_new_function(self):
        self._hook.create_new_function(self.full_location, self.body)

    def _update_function(self):
        self._hook.update_function(self.body['name'], self.body, self.body.keys())

    def _check_if_function_exists(self):
        name = self.body.get('name')
        if not name:
            raise FieldValidationException("The 'name' field should be present in "
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
        if self.validate_body:
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
