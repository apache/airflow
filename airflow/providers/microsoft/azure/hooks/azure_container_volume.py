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

from azure.mgmt.containerinstance.models import AzureFileVolume, Volume

from airflow.hooks.base_hook import BaseHook


class AzureContainerVolumeHook(BaseHook):
    """
    A hook which wraps an Azure Volume.

    :param wasb_conn_id: connection id of a Azure storage account of
        which file shares should be mounted
    :type wasb_conn_id: str
    """

    def __init__(self, wasb_conn_id='wasb_default'):
        super().__init__()
        self.conn_id = wasb_conn_id

    def get_storagekey(self):
        """
        Get Azure File Volume storage key
        """
        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson

        if 'connection_string' in service_options:
            for keyvalue in service_options['connection_string'].split(";"):
                key, value = keyvalue.split("=", 1)
                if key == "AccountKey":
                    return value
        return conn.password

    def get_file_volume(self, mount_name, share_name, storage_account_name, read_only=False):
        """
        Get Azure File Volume
        """
        return Volume(
            name=mount_name,
            azure_file=AzureFileVolume(
                share_name=share_name,
                storage_account_name=storage_account_name,
                read_only=read_only,
                storage_account_key=self.get_storagekey(),
            ),
        )
