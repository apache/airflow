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

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.samba.hooks.samba import SambaHook


class SambaFileSensor(BaseSensorOperator):
    """
    Waits for file in samba share

    :param file_name: The full name of the file, including extension.
    :param samba_conn_id: The connection id reference.
    :param share: The name of the file share.
    """

    default_conn_name = "samba_default"

    def __init__(
        self, file_name: str, samba_conn_id: str = default_conn_name, share: str | None = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.samba_conn_id = samba_conn_id
        self.share = share
        self.file_name = file_name

    def poke(self, context):
        hook = SambaHook(self.samba_conn_id, share=self.share)
        files = hook.listdir(path="/")
        if not files:
            return False
        else:
            if self.file_name in files:
                return True
            else:
                return False
