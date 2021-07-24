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

import configparser
import json
import tempfile
from contextlib import ExitStack, contextmanager
from typing import Dict, Generator, Optional

from airflow.models.connection import Connection
from airflow.utils.process_utils import patch_environ

AWS_CREDENTIALS_FILE_ENV = 'AWS_SHARED_CREDENTIALS_FILE'
AWS_DEFAUL_CONN = 'AIRFLOW_CONN_AWS_DEFAULT'
AWS_CREDENTIALS_DEFAULT_PROFILE = 'default'

AWS_CREDENTIALS_CONN = 'AIRFLOW_CONN_AWS_DEFAULT'


def build_aws_conn(
    key_file_path: Optional[str] = None,
    # TODO: Maybe allow a key_file_dict?
) -> str:
    """
    Builds a uri that can be used as :envvar:`AIRFLOW_CONN_{CONN_ID}` with provided service key,
    scopes and project id.

    :param key_file_path: Path to service key.
    :type key_file_path: Optional[str]
    :return: String representing Airflow connection.
    """
    # Read keyfile into a confing parser:
    aws_credentials_config = configparser.ConfigParser()
    with open(key_file_path) as key_file:
        aws_credentials_config.read_file(key_file)
    # If there is a 'default' section use that, else pick the first section
    if 'default' in aws_credentials_config:
        aws_credentials = aws_credentials_config['default']
    else:
        aws_credentials = aws_credentials_config.sections()[0]
    aws_connection = Connection(
        conn_id='aws-default',
        conn_type='aws',
        login=aws_credentials['aws_access_key_id'],
        password=aws_credentials['aws_secret_access_key'],
        # TODO: We should probably at least set region
        # here? But the credentials file does not contain region
        # that's in the config file. BUT the Airflow
        # mechanism for key_files does not allow more
        # than one key_file (since the google stuff is
        # all in one). So this is a bit of an issue for
        # aws that has two files (e.g. ~/.aws/config
        # and ~/.aws/credentials)
        extra=json.dumps(
            {
                # # AWS session token used for the initial
                # # connection if you use external
                # # credentials. You are responsible for
                # # renewing these.
                # aws_session_token: '',
                # # If specified, then an assume_role will be done to this role.
                # role_arn: '',
                # # Used to construct role_arn if it was not specified.
                # aws_account_id: '',
                # # Used to construct role_arn if it was not specified.
                # aws_iam_role: '',
                # # Additional kwargs passed to assume_role.
                # assume_role_kwargs: '',
                # # Endpoint URL for the connection.
                # host: '',
                # # AWS region for the connection.
                # region_name: '',
                # # AWS external ID for the
                # # connection (deprecated, rather use
                # #             assume_role_kwargs).
                # external_id:'',
                # # Additional kwargs used to
                # # construct a botocore.config.Config passed
                # # to boto3.client and boto3.resource.
                # config_kwargs: '',
                # # Additional kwargs passed to boto3.session.Session.
                # session_kwargs: '',
                # # If you are getting your
                # # credentials from the credentials file, you
                # # can specify the profile with this.
                # profile: '',
            }
        ),
    )

    return aws_connection.get_uri()


@contextmanager
def provide_aws_credentials(key_file_path: Optional[str] = None, key_file_dict: Optional[Dict] = None):
    """
    Context manager that provides AWS credentials.

    It can be used to provide credentials for external programs (e.g. aws cli) that expect authorization
    file in ``AWS_SHARED_CREDENTIALS_FILE`` environment variable.

    :param key_file_path: Path to AWS credentials ini file.
    :type key_file_path: str
    :param key_file_dict: Dictionary with credentials.
    :type key_file_dict: Dict
    """
    if not key_file_path and not key_file_dict:
        raise ValueError("Please provide `key_file_path` or `key_file_dict`.")

    with tempfile.NamedTemporaryFile(mode="w+t") as aws_credentials_file:
        if not key_file_path and key_file_dict:
            # Create a credentials file with a "default" profile with the given
            # keys/values from the key_file_dict. AWS credential files use INI
            # format.
            aws_credentials = configparser.ConfigParser()
            aws_credentials[AWS_CREDENTIALS_DEFAULT_PROFILE] = key_file_dict
            aws_credentials.write(aws_credentials_file)
            aws_credentials_file.flush()
            key_file_path = aws_credentials_file.name
        if key_file_path:
            with patch_environ({AWS_CREDENTIALS_FILE_ENV: key_file_path}):
                yield
        else:
            # We will use the default service account credentials.
            yield


@contextmanager
def provide_aws_connection(
    key_file_path: Optional[str] = None,
    # TODO: Maybe allow a key_file_dict?
) -> Generator:
    """
    Context manager that provides a temporary value of :envvar:`AIRFLOW_CONN_AWS_DEFAULT`
    connection. It build a new connection that includes access key id and
    secret access key id.

    :param key_file_path: Path to ini AWS credentials file.
    :type key_file_path: str
    :param key_file_dict: Dictionary with and optional extras (region, role_arn, etc).
    :type key_file_dict: Dict
    """
    conn = build_aws_conn(key_file_path=key_file_path)

    with patch_environ({AWS_DEFAUL_CONN: conn}):
        yield


@contextmanager
def provide_aws_conn_and_credentials(
    key_file_path: Optional[str] = None,
) -> Generator:
    """
    Context manager that provides both:

    - Google Cloud credentials for application supporting `Application Default Credentials (ADC)
      strategy <https://cloud.google.com/docs/authentication/production>`__.
    - temporary value of :envvar:`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` connection

    :param key_file_path: Path to file with Google Cloud Service Account .json file.
    :type key_file_path: str
    :param scopes: OAuth scopes for the connection
    :type scopes: Sequence
    :param project_id: The id of Google Cloud project for the connection.
    :type project_id: str
    """
    with ExitStack() as stack:
        stack.enter_context(  # type; ignore  # pylint: disable=no-member
            provide_aws_credentials(key_file_path)
        )
        stack.enter_context(  # type; ignore  # pylint: disable=no-member
            provide_aws_connection(key_file_path)
        )
        yield
