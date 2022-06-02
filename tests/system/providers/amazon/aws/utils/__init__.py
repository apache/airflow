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

import inspect
import json
import os
from os.path import basename, splitext
from typing import Optional
from uuid import uuid4

import boto3
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError

ENV_ID_ENVIRON_KEY: str = 'SYSTEM_TESTS_ENV_ID'
DEFAULT_ENV_ID_PREFIX: str = 'env'
DEFAULT_ENV_ID_LEN: int = 8
DEFAULT_ENV_ID: str = f'{DEFAULT_ENV_ID_PREFIX}{str(uuid4())[:DEFAULT_ENV_ID_LEN]}'

# All test file names will contain this string.
TEST_FILE_IDENTIFIER: str = 'example'

INVALID_ENV_ID_MSG: str = (
    'In order to maximize compatibility, the SYSTEM_TESTS_ENV_ID must be an alphanumeric string '
    'which starts with a letter. Please see `tests/system/providers/amazon/aws/README.md`.'
)
LOWERCASE_ENV_ID_MSG: str = (
    'The provided Environment ID contains uppercase letters and '
    'will be converted to lowercase for the AWS System Tests.'
)
NO_VALUE_MSG: str = 'No Value Found: Variable {key} could not be found and no default value was provided.'


def _get_test_name() -> str:
    """
    Extracts the module name from the test module.

    :return: The name of the test module that called the helper method.
    """
    # The exact layer of the stack will depend on if this is called directly
    # or from another helper, but the test will always contain the identifier.
    test_filename: str = [
        frame.filename for frame in inspect.stack() if TEST_FILE_IDENTIFIER in frame.filename
    ][0]
    return splitext(basename(test_filename))[0]


def _validate_env_id(env_id: str) -> str:
    """
    Verifies that a prospective Environment ID value fits requirements.
    An Environment ID for an AWS System test must be a lowercase alphanumeric
    string which starts with a letter.

    :param env_id: An Environment ID to validate.
    :return: A validated string cast to lowercase.
    """
    if any(char.isupper() for char in str(env_id)):
        print(LOWERCASE_ENV_ID_MSG)
    if not env_id.isalnum() or not env_id[0].isalpha():
        raise ValueError(INVALID_ENV_ID_MSG)

    return env_id.lower()


def _fetch_from_ssm(key: str) -> str:
    """
    Test values are stored in the SSM Value as a JSON-encoded dict of key/value pairs.

    :param key: The key to search for within the returned Parameter Value.
    :return: The value of the provided key from SSM
    """
    test_name: str = _get_test_name()
    ssm_client: BaseClient = boto3.client('ssm')
    value: str = ''

    try:
        value = json.loads(ssm_client.get_parameter(Name=test_name)['Parameter']['Value'])[key]
    # Since a default value after the SSM check is allowed, these exceptions should not stop execution.
    except NoCredentialsError:
        # No boto credentials found.
        pass
    except ssm_client.exceptions.ParameterNotFound:
        # SSM does not contain any values for this test.
        pass
    except KeyError:
        # SSM contains values for this test, but not the requested value.
        pass
    return value


def fetch_variable(key: str, default_value: Optional[str] = None) -> str:
    """
    Given a Parameter name: first check for an existing Environment Variable,
    then check SSM for a value. If neither are available, fall back on the
    optional default value.

    :param key: The name of the Parameter to fetch a value for.
    :param default_value: The default value to use if no value can be found.
    :return: The value of the parameter.
    """

    value: Optional[str] = os.getenv(key, _fetch_from_ssm(key)) or default_value
    if not value:
        raise ValueError(NO_VALUE_MSG.format(key=key))
    return value


def set_env_id() -> str:
    """
    Retrieves or generates an Environment ID, validate that it is suitable,
    export it as an Environment Variable, and return it.

    If an Environment ID has already been generated, use that.
    Otherwise, try to fetch it and export it as an Environment Variable.
    If there is not one available to fetch then generate one and export it as an Environment Variable.

    :return: A valid System Test Environment ID.
    """
    env_id: str = fetch_variable(ENV_ID_ENVIRON_KEY, DEFAULT_ENV_ID)
    env_id = _validate_env_id(env_id)

    os.environ[ENV_ID_ENVIRON_KEY] = env_id
    return env_id
