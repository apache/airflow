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

import os

from smbclient import SambaClient

from airflow.hooks.base_hook import BaseHook


class SambaHook(BaseHook):
    """
    Allows for interaction with an samba server.
    """

    def __init__(self, samba_conn_id):
        super().__init__()
        self.conn = self.get_connection(samba_conn_id)

    def get_conn(self):
        samba = SambaClient(
            server=self.conn.host,
            share=self.conn.schema,
            username=self.conn.login,
            ip=self.conn.host,
            password=self.conn.password)
        return samba

    def push_from_local(self, destination_filepath, local_filepath):
        """
        Push local file to samba server
        """
        samba = self.get_conn()
        if samba.exists(destination_filepath):
            if samba.isfile(destination_filepath):
                samba.remove(destination_filepath)
        else:
            folder = os.path.dirname(destination_filepath)
            if not samba.exists(folder):
                samba.mkdir(folder)
        samba.upload(local_filepath, destination_filepath)
