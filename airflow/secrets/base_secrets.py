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

from abc import ABCMeta
from typing import List, Optional

from airflow.models.connection import Connection


class BaseSecretsBackend:
    """
    Abstract base class to retrieve secrets given a conn_id and construct a Connection object
    """
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        pass

    @staticmethod
    def build_path(path_prefix, secret_id, sep="/"):
        # type: (str, str, str) -> str
        """
        Given conn_id, build path for Secrets Backend

        :param path_prefix: Prefix of the path to get secret
        :type path_prefix: str
        :param secret_id: Secret id
        :type secret_id: str
        :param sep: separator used to concatenate connections_prefix and conn_id. Default: "/"
        :type sep: str
        """
        return "{}{}{}".format(path_prefix, sep, secret_id)

    def get_conn_uri(self, conn_id):
        # type: (str) -> Optional[str]
        """
        Get conn_uri from Secrets Backend

        :param conn_id: connection id
        :type conn_id: str
        """
        raise NotImplementedError()

    def get_connections(self, conn_id):
        # type: (str) -> List[Connection]
        """
        Get connections with a specific ID

        :param conn_id: connection id
        :type conn_id: str
        """
        conn_uri = self.get_conn_uri(conn_id=conn_id)
        if not conn_uri:
            return []
        conn = Connection(conn_id=conn_id, uri=conn_uri)
        return [conn]

    def get_variable(self, key):
        # type: (str) -> Optional[str]
        """
        Return value for Airflow Connection

        :param key: Variable Key
        :return: Variable Value
        """
        raise NotImplementedError()
