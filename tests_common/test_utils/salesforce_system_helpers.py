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
from __future__ import annotations

import json
import os
from contextlib import contextmanager

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.process_utils import patch_environ

CONFIG_REQUIRED_FIELDS = ["host", "login", "password", "security_token"]
SALESFORCE_CONNECTION_ID = os.environ.get(
    "SALESFORCE_CONNECTION_ID", "salesforce_default"
)
CONNECTION_TYPE = os.environ.get("CONNECTION_TYPE", "http")


@contextmanager
def provide_salesforce_connection(key_file_path: str):
    """
    Context manager that provides a temporary value of SALESFORCE_DEFAULT connection.

    :param key_file_path: Path to file with SALESFORCE credentials .json file.
    """
    if not key_file_path.endswith(".json"):
        raise AirflowException("Use a JSON key file.")
    with open(key_file_path) as credentials:
        creds = json.load(credentials)
    missing_keys = CONFIG_REQUIRED_FIELDS - creds.keys()
    if missing_keys:
        message = f"{missing_keys} fields are missing"
        raise AirflowException(message)
    conn = Connection(
        conn_id=SALESFORCE_CONNECTION_ID,
        conn_type=CONNECTION_TYPE,
        host=creds["host"],
        login=creds["login"],
        password=creds["password"],
        extra=json.dumps({"security_token": creds["security_token"]}),
    )
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield
