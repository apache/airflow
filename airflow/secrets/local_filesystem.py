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
Objects relating to retrieving connections and variables from local file
"""
import json
import logging
import os
from collections import defaultdict

import funcsigs
import six

from airflow.exceptions import AirflowException, AirflowFileParseException, file_syntax_error
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.file import COMMENT_PATTERN
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)


def get_connection_parameter_names():
    """Returns :class:`airflow.models.connection.Connection` constructor parameters."""
    from airflow.models.connection import Connection

    return {k for k in funcsigs.signature(Connection.__init__).parameters.keys() if k != "self"}


def _parse_env_file(file_path):
    """
    Parse a file in the ``.env '' format.

   .. code-block:: text

        MY_CONN_ID=my-conn-type://my-login:my-pa%2Fssword@my-host:5432/my-schema?param1=val1&param2=val2

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Tuple with mapping of key and list of values and list of syntax errors
    """
    with open(file_path) as f:
        content = f.read()

    secrets = defaultdict(list)
    errors = []
    for line_no, line in enumerate(content.splitlines(), 1):
        if not line:
            # Ignore empty line
            continue

        if COMMENT_PATTERN.match(line):
            # Ignore comments
            continue

        var_parts = line.split("=", 2)
        if len(var_parts) != 2:
            errors.append(
                file_syntax_error(
                    line_no=line_no,
                    message='Invalid line format. The line should contain at least one equal sign ("=").',
                )
            )
            continue

        key, value = var_parts
        if not key:
            errors.append(file_syntax_error(line_no=line_no, message="Invalid line format. Key is empty.",))
        secrets[key].append(value)
    return secrets, errors


def _parse_json_file(file_path):
    """
    Parse a file in the JSON format.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Tuple with mapping of key and list of values and list of syntax errors
    """
    with open(file_path) as f:
        content = f.read()

    if not content:
        return {}, [file_syntax_error(line_no=1, message="The file is empty.")]
    try:
        secrets = json.loads(content)
    except ValueError as e:
        return {}, [file_syntax_error(line_no=1, message=str(e))]
    if not isinstance(secrets, dict):
        return {}, [file_syntax_error(line_no=1, message="The file should contain the object.")]

    return secrets, []


FILE_PARSERS = {
    "env": _parse_env_file,
    "json": _parse_json_file,
}


def _parse_secret_file(file_path):
    """
    Based on the file extension format, selects a parser, and parses the file.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Map of secret key (e.g. connection ID) and value.
    """
    if not os.path.exists(file_path):
        raise AirflowException(
            "File {} was not found. Check the configuration of your Secrets backend.".format(file_path)
        )

    log.debug("Parsing file: %s", file_path)

    ext = file_path.rsplit(".", 2)[-1].lower()

    if ext not in FILE_PARSERS:
        raise AirflowException("Unsupported file format. The file must have the extension .env or .json")

    secrets, parse_errors = FILE_PARSERS[ext](file_path)

    log.debug("Parsed file: len(parse_errors)=%d, len(secrets)=%d", len(parse_errors), len(secrets))

    if parse_errors:
        raise AirflowFileParseException(
            "Failed to load the secret file.", file_path=file_path, parse_errors=parse_errors
        )

    return secrets


def _create_connection(conn_id, value):
    """
    Creates a connection based on a URL or JSON object.
    """
    from airflow.models.connection import Connection

    if isinstance(value, six.string_types):
        return Connection(conn_id=conn_id, uri=value)
    if isinstance(value, dict):
        connection_parameter_names = get_connection_parameter_names()
        current_keys = set(value.keys())
        if not current_keys.issubset(connection_parameter_names):
            illegal_keys = current_keys - connection_parameter_names
            illegal_keys_list = ", ".join(illegal_keys)
            raise AirflowException(
                "The object have illegal keys: {}."
                "The dictionary can only contain the following keys: {}".format(
                    illegal_keys_list, connection_parameter_names
                )
            )

        if "conn_id" in current_keys and conn_id != value["conn_id"]:
            raise AirflowException(
                "Mismatch conn_id. "
                "The dictionary key has the value: " + value['conn_id'] + ". "
                "The item has the value: " + conn_id + " ."
            )
        value["conn_id"] = conn_id
        return Connection(**value)
    raise AirflowException(
        "Unexpected value type: {}"
        ". The connection can only be defined using a string or object.".format(type(value))
    )


def load_variables(file_path):
    """
    Load variables from a text file.
    Both ``JSON`` and ``.env`` files are supported.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :rtype: dict[str, list[str]]
    """
    log.debug("Loading variables from a text file")

    secrets = _parse_secret_file(file_path)
    invalid_keys = [key for key, values in secrets.items() if isinstance(values, list) and len(values) != 1]
    if invalid_keys:
        raise AirflowException(
            'The "{}" file contains multiple values for keys: {}'.format(file_path, invalid_keys)
        )
    variables = {key: values[0] if isinstance(values, list) else values for key, values in secrets.items()}
    log.debug("Loaded %d variables: ", len(variables))
    return variables


def load_connections(file_path):
    """
    Load connection from text file.
    Both ``JSON`` and ``.env`` files are supported.

    :return: A dictionary where the key contains a connection ID and the value contains a list of connections.
    :rtype: list[str, list[airflow.models.connection.Connection]]
    """
    log.debug("Loading connection")

    secrets = _parse_secret_file(file_path)
    connections_by_conn_id = defaultdict(list)
    for key, secret_values in list(secrets.items()):
        if isinstance(secret_values, list):
            for secret_value in secret_values:
                connections_by_conn_id[key].append(_create_connection(key, secret_value))
        else:
            connections_by_conn_id[key].append(_create_connection(key, secret_values))
    num_conn = sum(map(len, connections_by_conn_id.values()))
    log.debug("Loaded %d connections", num_conn)

    return connections_by_conn_id


class LocalFilesystemBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection objects and Variables from local files
    Both ``JSON`` and ``.env`` files are supported.

    :param variables_file_path: File location with variables data.
    :type variables_file_path: str
    :param connections_file_path: File location with connection data.
    :type connections_file_path: str
    """

    def __init__(
        self, variables_file_path=None, connections_file_path=None
    ):
        super(LocalFilesystemBackend, self).__init__()
        self.variables_file = variables_file_path
        self.connections_file = connections_file_path

    @property
    def _local_variables(self):
        if not self.variables_file:
            self.log.debug("The file for variables is not specified. Skipping")
            # The user may not specify any file.
            return {}
        secrets = load_variables(self.variables_file)
        return secrets

    @property
    def _local_connections(self):
        if not self.connections_file:
            self.log.debug("The file for connection is not specified. Skipping")
            # The user may not specify any file.
            return {}
        return load_connections(self.connections_file)

    def get_connections(self, conn_id):
        return self._local_connections.get(conn_id) or []

    def get_variable(self, key):
        return self._local_variables.get(key)
