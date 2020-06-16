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

import logging

from flask import current_app
from itsdangerous import BadSignature, URLSafeSerializer

from airflow.api_connexion.exceptions import NotFound
from airflow.models.dagcode import DagCode

log = logging.getLogger(__name__)


def get_dag_source(file_token: str):
    """
    Get source code using file token
    """
    secret_key = current_app.config["SECRET_KEY"]
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
        dag_source = DagCode.code(path)
    except (BadSignature, FileNotFoundError):
        NotFound("Dag Source not found")
    else:
        return dag_source
