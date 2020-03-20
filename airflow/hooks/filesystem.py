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
#

from airflow.hooks.base_hook import BaseHook


class FSHook(BaseHook):
    """
    Allows for interaction with an file server.

    Connection should have a name and a path specified under extra:

    example:
    Conn Id: fs_test
    Conn Type: File (path)
    Host, Schema, Login, Password, Port: empty
    Extra: {"path": "/tmp"}
    """

    def __init__(self, conn_id='fs_default'):
        conn = self.get_connection(conn_id)
        self.basepath = conn.extra_dejson.get('path', '')
        self.conn = conn

    def get_conn(self):
        pass

    def get_path(self) -> str:
        """
        Get the path to the filesystem location.

        :return: the path.
        """
        return self.basepath
