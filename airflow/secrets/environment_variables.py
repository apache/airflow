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
Objects relating to sourcing connections from environment variables
"""

import os
from typing import List

from airflow.models import Connection
from airflow.secrets import CONN_ENV_PREFIX, BaseSecretsBackend


class EnvironmentVariablesSecretsBackend(BaseSecretsBackend):
    """
    Retrieves Connection object from environment variable.
    """

    # pylint: disable=missing-docstring
    def get_connections(self, conn_id) -> List[Connection]:
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
            return [conn]
        else:
            return []
