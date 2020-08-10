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
"""
This module contains integration with Azure Blob Storage.

It communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).
"""
import requests
import datetime
import hashlib
import hmac
import base64

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class LawsHook(BaseHook):
    """
    Interacts with Azure LAWS through api.

    :param remote_conn_id: Not Used
    :type remote_conn_id: str
    :param account_id: Laws Account Id
    :type account_id: str
    :param access_key: Access Key
    :type access_key: str
    :param table_name: <Table Name>_CL
    :type table_name: str
    """

    def __init__(self,remote_conn_id,account_id,access_key,table_name):
        super().__init__()
        self.conn_id = remote_conn_id
        self.account_id = account_id
        self.access_key = access_key
        self.table_name = table_name

    def build_signature(self, date, content_length, method, content_type, resource):
        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
        decoded_key = base64.b64decode(self.access_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        authorization = "SharedKey {}:{}".format(self.account_id, encoded_hash)
        return authorization

    def post_log(self, body):
        """
        Post data to Azure Log Analytics
        """
        custom_table_name = self.table_name
        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self.build_signature(rfc1123date, content_length, method, content_type, resource)
        uri = 'https://' + self.account_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': custom_table_name,
            'x-ms-date': rfc1123date
        }

        return requests.post(uri,data=body, headers=headers)
