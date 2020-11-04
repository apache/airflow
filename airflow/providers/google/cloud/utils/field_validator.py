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
"""Validator for body fields sent via Google Cloud API.

The validator performs validation of the body (being dictionary of fields) that
is sent in the API request to Google Cloud (via googleclient API usually).

Context
-------
The specification mostly focuses on helping Airflow DAG developers in the development
phase. You can build your own Google Cloud operator (such as GcfDeployOperator for example) which
can have built-in validation specification for the particular API. It's super helpful
when developer plays with different fields and their values at the initial phase of
DAG development. Most of the Google Cloud APIs perform their own validation on the
server side, but most of the requests are asynchronous and you need to wait for result
of the operation. This takes precious times and slows
down iteration over the API. BodyFieldValidator is meant to be used on the client side
and it should therefore provide an instant feedback to the developer on misspelled or
wrong type of parameters.

The validation should be performed in "execute()" method call in order to allow
template parameters to be expanded before validation is performed.

Types of fields
---------------

Specification is an array of dictionaries - each dictionary describes field, its type,
validation, optionality, api_version supported and nested fields (for unions and dicts).

Typically (for clarity and in order to aid syntax highlighting) the array of
dicts should be defined as series of dict() executions. Fragment of example
specification might look as follows::

    SPECIFICATION =[
       dict(name="an_union", type="union", optional=True, fields=[
           dict(name="variant_1", type="dict"),
           dict(name="variant_2", regexp=r'^.+$', api_version='v1beta2'),
       ),
       dict(name="an_union", type="dict", fields=[
           dict(name="field_1", type="dict"),
           dict(name="field_2", regexp=r'^.+$'),
       ),
       ...
    ]


Each field should have key = "name" indicating field name. The field can be of one of the
following types:

* Dict fields: (key = "type", value="dict"):
  Field of this type should contain nested fields in form of an array of dicts.
  Each of the fields in the array is then expected (unless marked as optional)
  and validated recursively. If an extra field is present in the dictionary, warning is
  printed in log file (but the validation succeeds - see the Forward-compatibility notes)
* List fields: (key = "type", value="list"):
  Field of this type should be a list. Only the type correctness is validated.
  The contents of a list are not subject to validation.
* Union fields (key = "type", value="union"): field of this type should contain nested
  fields in form of an array of dicts. One of the fields (and only one) should be
  present (unless the union is marked as optional). If more than one union field is
  present, FieldValidationException is raised. If none of the union fields is
  present - warning is printed in the log (see below Forward-compatibility notes).
* Fields validated for non-emptiness: (key = "allow_empty") - this applies only to
  fields the value of which is a string, and it allows to check for non-emptiness of
  the field (allow_empty=False).
* Regexp-validated fields: (key = "regexp") - fields of this type are assumed to be
  strings and they are validated with the regexp specified. Remember that the regexps
  should ideally contain ^ at the beginning and $ at the end to make sure that
  the whole field content is validated. Typically such regexp
  validations should be used carefully and sparingly (see Forward-compatibility
  notes below).
* Custom-validated fields: (key = "custom_validation") - fields of this type are validated
  using method specified via custom_validation field. Any exception thrown in the custom
  validation will be turned into FieldValidationException and will cause validation to
  fail. Such custom validations might be used to check numeric fields (including
  ranges of values), booleans or any other types of fields.
* API version: (key="api_version") if API version is specified, then the field will only
  be validated when api_version used at field validator initialization matches exactly the
  version specified. If you want to declare fields that are available in several
  versions of the APIs, you should specify the field as many times as many API versions
  should be supported (each time with different API version).
* if none of the keys ("type", "regexp", "custom_validation" - the field is not validated

You can see some of the field examples in EXAMPLE_VALIDATION_SPECIFICATION.


Forward-compatibility notes
---------------------------
Certain decisions are crucial to allow the client APIs to work also with future API
versions. Since body attached is passed to the API’s call, this is entirely
possible to pass-through any new fields in the body (for future API versions) -
albeit without validation on the client side - they can and will still be validated
on the server side usually.

Here are the guidelines that you should follow to make validation forward-compatible:

* most of the fields are not validated for their content. It's possible to use regexp
  in some specific cases that are guaranteed not to change in the future, but for most
  fields regexp validation should be r'^.+$' indicating check for non-emptiness
* api_version is not validated - user can pass any future version of the api here. The API
  version is only used to filter parameters that are marked as present in this api version
  any new (not present in the specification) fields in the body are allowed (not verified)
  For dictionaries, new fields can be added to dictionaries by future calls. However if an
  unknown field in dictionary is added, a warning is logged by the client (but validation
  remains successful). This is very nice feature to protect against typos in names.
* For unions, newly added union variants can be added by future calls and they will
  pass validation, however the content or presence of those fields will not be validated.
  This means that it’s possible to send a new non-validated union field together with an
  old validated field and this problem will not be detected by the client. In such case
  warning will be printed.
* When you add validator to an operator, you should also add ``validate_body`` parameter
  (default = True) to __init__ of such operators - when it is set to False,
  no validation should be performed. This is a safeguard for totally unpredicted and
  backwards-incompatible changes that might sometimes occur in the APIs.

"""

import re
from typing import Callable, Dict, Sequence

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

COMPOSITE_FIELD_TYPES = ['union', 'dict', 'list']


class GcpFieldValidationException(AirflowException):
    """Thrown when validation finds dictionary field not valid according to specification."""


class GcpValidationSpecificationException(AirflowException):
    """Thrown when validation specification is wrong.

    This should only happen during development as ideally
     specification itself should not be invalid ;) .
    """


def _int_greater_than_zero(value):
    if int(value) <= 0:
        raise GcpFieldValidationException("The available memory has to be greater than 0")


EXAMPLE_VALIDATION_SPECIFICATION = [
    dict(name="name", allow_empty=False),
    dict(name="description", allow_empty=False, optional=True),
    dict(name="availableMemoryMb", custom_validation=_int_greater_than_zero, optional=True),
    dict(name="labels", optional=True, type="dict"),
    dict(
        name="an_union",
        type="union",
        fields=[
            dict(name="variant_1", regexp=r'^.+$'),
            dict(name="variant_2", regexp=r'^.+$', api_version='v1beta2'),
            dict(name="variant_3", type="dict", fields=[dict(name="url", regexp=r'^.+$')]),
            dict(name="variant_4"),
        ],
    ),
]


class GcpBodyFieldValidator(LoggingMixin):
    """Validates correctness of request body according to specification.

    The specification can describe various type of
    fields including custom validation, and union of fields. This validator is
    to be reusable by various operators. See the EXAMPLE_VALIDATION_SPECIFICATION
    for some examples and explanations of how to create specification.

    :param validation_specs: dictionary describing validation specification
    :type validation_specs: list[dict]
    :param api_version: Version of the api used (for example v1)
    :type api_version: str

    """

    def __init__(self, validation_specs: Sequence[Dict], api_version: str) -> None:
        super().__init__()
        self._validation_specs = validation_specs
        self._api_version = api_version

    @staticmethod
    def _get_field_name_with_parent(field_name, parent):
        if parent:
            return parent + '.' + field_name
        return field_name

    @staticmethod
    def _sanity_checks(
        children_validation_specs: Dict,
        field_type: str,
        full_field_path: str,
        regexp: str,
        allow_empty: bool,
        custom_validation: Callable,
        value,
    ) -> None:
        if value is None and field_type != 'union':
            raise GcpFieldValidationException(
                f"The required body field '{full_field_path}' is missing. Please add it."
            )
        if regexp and field_type:
            raise GcpValidationSpecificationException(
                "The validation specification entry '{}' has both type and regexp. "
                "The regexp is only allowed without type (i.e. assume type is 'str' "
                "that can be validated with regexp)".format(full_field_path)
            )
        if allow_empty is not None and field_type:
            raise GcpValidationSpecificationException(
                "The validation specification entry '{}' has both type and allow_empty. "
                "The allow_empty is only allowed without type (i.e. assume type is 'str' "
                "that can be validated with allow_empty)".format(full_field_path)
            )
        if children_validation_specs and field_type not in COMPOSITE_FIELD_TYPES:
            raise GcpValidationSpecificationException(
                "Nested fields are specified in field '{}' of type '{}'. "
                "Nested fields are only allowed for fields of those types: ('{}').".format(
                    full_field_path, field_type, COMPOSITE_FIELD_TYPES
                )
            )
        if custom_validation and field_type:
            raise GcpValidationSpecificationException(
                "The validation specification field '{}' has both type and "
                "custom_validation. Custom validation is only allowed without type.".format(full_field_path)
            )

    @staticmethod
    def _validate_regexp(full_field_path: str, regexp: str, value: str) -> None:
        if not re.match(regexp, value):
            # Note matching of only the beginning as we assume the regexps all-or-nothing
            raise GcpFieldValidationException(
                "The body field '{}' of value '{}' does not match the field "
                "specification regexp: '{}'.".format(full_field_path, value, regexp)
            )

    @staticmethod
    def _validate_is_empty(full_field_path: str, value: str) -> None:
        if not value:
            raise GcpFieldValidationException(
                f"The body field '{full_field_path}' can't be empty. Please provide a value."
            )

    def _validate_dict(self, children_validation_specs: Dict, full_field_path: str, value: Dict) -> None:
        for child_validation_spec in children_validation_specs:
            self._validate_field(
                validation_spec=child_validation_spec, dictionary_to_validate=value, parent=full_field_path
            )
        all_dict_keys = [spec['name'] for spec in children_validation_specs]
        for field_name in value.keys():
            if field_name not in all_dict_keys:
                self.log.warning(
                    "The field '%s' is in the body, but is not specified in the "
                    "validation specification '%s'. "
                    "This might be because you are using newer API version and "
                    "new field names defined for that version. Then the warning "
                    "can be safely ignored, or you might want to upgrade the operator"
                    "to the version that supports the new API version.",
                    self._get_field_name_with_parent(field_name, full_field_path),
                    children_validation_specs,
                )

    def _validate_union(
        self, children_validation_specs: Dict, full_field_path: str, dictionary_to_validate: Dict
    ) -> None:
        field_found = False
        found_field_name = None
        for child_validation_spec in children_validation_specs:
            # Forcing optional so that we do not have to type optional = True
            # in specification for all union fields
            new_field_found = self._validate_field(
                validation_spec=child_validation_spec,
                dictionary_to_validate=dictionary_to_validate,
                parent=full_field_path,
                force_optional=True,
            )
            field_name = child_validation_spec['name']
            if new_field_found and field_found:
                raise GcpFieldValidationException(
                    "The mutually exclusive fields '{}' and '{}' belonging to the "
                    "union '{}' are both present. Please remove one".format(
                        field_name, found_field_name, full_field_path
                    )
                )
            if new_field_found:
                field_found = True
                found_field_name = field_name
        if not field_found:
            self.log.warning(
                "There is no '%s' union defined in the body %s. "
                "Validation expected one of '%s' but could not find any. It's possible "
                "that you are using newer API version and there is another union variant "
                "defined for that version. Then the warning can be safely ignored, "
                "or you might want to upgrade the operator to the version that "
                "supports the new API version.",
                full_field_path,
                dictionary_to_validate,
                [field['name'] for field in children_validation_specs],
            )

    def _validate_field(self, validation_spec, dictionary_to_validate, parent=None, force_optional=False):
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
        allow_empty = validation_spec.get('allow_empty')
        children_validation_specs = validation_spec.get('fields')
        required_api_version = validation_spec.get('api_version')
        custom_validation = validation_spec.get('custom_validation')

        full_field_path = self._get_field_name_with_parent(field_name=field_name, parent=parent)
        if required_api_version and required_api_version != self._api_version:
            self.log.debug(
                "Skipping validation of the field '%s' for API version '%s' "
                "as it is only valid for API version '%s'",
                field_name,
                self._api_version,
                required_api_version,
            )
            return False
        value = dictionary_to_validate.get(field_name)

        if (optional or force_optional) and value is None:
            self.log.debug("The optional field '%s' is missing. That's perfectly OK.", full_field_path)
            return False

        # Certainly down from here the field is present (value is not None)
        # so we should only return True from now on

        self._sanity_checks(
            children_validation_specs=children_validation_specs,
            field_type=field_type,
            full_field_path=full_field_path,
            regexp=regexp,
            allow_empty=allow_empty,
            custom_validation=custom_validation,
            value=value,
        )

        if allow_empty is False:
            self._validate_is_empty(full_field_path, value)
        if regexp:
            self._validate_regexp(full_field_path, regexp, value)
        elif field_type == 'dict':
            if not isinstance(value, dict):
                raise GcpFieldValidationException(
                    "The field '{}' should be of dictionary type according to the "
                    "specification '{}' but it is '{}'".format(full_field_path, validation_spec, value)
                )
            if children_validation_specs is None:
                self.log.debug(
                    "The dict field '%s' has no nested fields defined in the "
                    "specification '%s'. That's perfectly ok - it's content will "
                    "not be validated.",
                    full_field_path,
                    validation_spec,
                )
            else:
                self._validate_dict(children_validation_specs, full_field_path, value)
        elif field_type == 'union':
            if not children_validation_specs:
                raise GcpValidationSpecificationException(
                    "The union field '{}' has no nested fields "
                    "defined in specification '{}'. Unions should have at least one "
                    "nested field defined.".format(full_field_path, validation_spec)
                )
            self._validate_union(children_validation_specs, full_field_path, dictionary_to_validate)
        elif field_type == 'list':
            if not isinstance(value, list):
                raise GcpFieldValidationException(
                    "The field '{}' should be of list type according to the "
                    "specification '{}' but it is '{}'".format(full_field_path, validation_spec, value)
                )
        elif custom_validation:
            try:
                custom_validation(value)
            except Exception as e:
                raise GcpFieldValidationException(
                    "Error while validating custom field '{}' specified by '{}': '{}'".format(
                        full_field_path, validation_spec, e
                    )
                )
        elif field_type is None:
            self.log.debug(
                "The type of field '%s' is not specified in '%s'. " "Not validating its content.",
                full_field_path,
                validation_spec,
            )
        else:
            raise GcpValidationSpecificationException(
                "The field '{}' is of type '{}' in specification '{}'."
                "This type is unknown to validation!".format(full_field_path, field_type, validation_spec)
            )
        return True

    def validate(self, body_to_validate: dict) -> None:
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
                self._validate_field(validation_spec=validation_spec, dictionary_to_validate=body_to_validate)
        except GcpFieldValidationException as e:
            raise GcpFieldValidationException(
                f"There was an error when validating: body '{body_to_validate}': '{e}'"
            )
        all_field_names = [
            spec['name']
            for spec in self._validation_specs
            if spec.get('type') != 'union' and spec.get('api_version') != self._api_version
        ]
        all_union_fields = [spec for spec in self._validation_specs if spec.get('type') == 'union']
        for union_field in all_union_fields:
            all_field_names.extend(
                [
                    nested_union_spec['name']
                    for nested_union_spec in union_field['fields']
                    if nested_union_spec.get('type') != 'union'
                    and nested_union_spec.get('api_version') != self._api_version
                ]
            )
        for field_name in body_to_validate.keys():
            if field_name not in all_field_names:
                self.log.warning(
                    "The field '%s' is in the body, but is not specified in the "
                    "validation specification '%s'. "
                    "This might be because you are using newer API version and "
                    "new field names defined for that version. Then the warning "
                    "can be safely ignored, or you might want to upgrade the operator"
                    "to the version that supports the new API version.",
                    field_name,
                    self._validation_specs,
                )
