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
This module contains integration with Azure LAWS.

"""
import requests
import datetime
import hashlib
import hmac
import base64
from datetime import datetime

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
        self.pushed = False

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

    @staticmethod
    def _clean_execution_date(execution_date: datetime) -> str:
        """
        Clean up an execution date so that it is safe to query in log analytics
        by formatting it correctly
        # 

        :param execution_date: execution date of the dag run.
        """
        return execution_date.strftime("%Y-%m-%d %H:%M:%S")

    def post_log(self, log, ti, ssl_verify = True):
        """
        Post data to Azure Log Analytics
        """
        ts = self._clean_execution_date(ti.execution_date)
        body = {"dag_id":ti.dag_id, "task_id":ti.task_id, "execution_date": ts, "try_number": ti.try_number, "raw_data": log} 

        custom_table_name = self.table_name
        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self.build_signature(rfc1123date, content_length, method, content_type, resource)
        uri = 'https://' + self.account_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': custom_table_name,
            'x-ms-date': rfc1123date
        }
        print("="*40)
        print(uri)
        try:
            response = requests.post(uri,data=body, headers=headers, verify=ssl_verify)
            print(response.status_code)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print("Logs Submit Error")
            raise Exception("AZ-LAWS:Log not submitted")
        return True
