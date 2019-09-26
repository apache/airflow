# -*- coding: utf-8 -*-
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

from airflow.hooks.base_hook import BaseHook
import requests
from airflow import AirflowException

OK_STATUS_CODE = 202


class OpenFaasHook(BaseHook):
    """
    Interact with OpenFaaS to query, deploy, invoke and update function

    :param function_name: Name of the function, Defaults to None
    :type query: str
    :param conn_id: openfass connection to use, Defaults to open_faas_default
        for example host : http://openfaas.faas.com, Conn Type : Http
    :type conn_id: str
    """

    GET_FUNCTION = "/system/function/"
    INVOKE_ASYNC_FUNCTION = "/async-function/"
    DEPLOY_FUNCTION = "/system/functions"
    UPDATE_FUNCTION = "/system/functions"

    def __init__(self,
                 function_name=None,
                 conn_id='open_faas_default',
                 *args, **kwargs):
        self.function_name = function_name
        self.conn_id = conn_id

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        return conn

    def deploy_function(self, overwrite_function_if_exist, body):
        if overwrite_function_if_exist:
            self.log.info("Function already exist " + self.function_name + " going to update")
            self.update_function(body)
        else:
            url = self.get_conn().host + self.DEPLOY_FUNCTION
            self.log.info("Deploying function " + url)
            response = requests.post(url, body)
            if response.status_code != OK_STATUS_CODE:
                self.log.error("Response status " + str(response.status_code))
                self.log.error("Failed to deploy")
                raise AirflowException('failed to deploy')
            else:
                self.log.info("Function deployed " + self.function_name)

    def invoke_async_function(self, body):
        url = self.get_conn().host + self.INVOKE_ASYNC_FUNCTION + self.function_name
        self.log.info("Invoking  function " + url)
        response = requests.post(url, body)
        if response.ok:
            self.log.info("Invoked " + self.function_name)
        else:
            self.log.error("Response status " + str(response.status_code))
            raise AirflowException('failed to invoke function')

    def update_function(self, body):
        url = self.get_conn().host + self.UPDATE_FUNCTION
        self.log.info("Updating function " + url)
        response = requests.put(url, body)
        if response.status_code != OK_STATUS_CODE:
            self.log.error("Response status " + str(response.status_code))
            self.log.error("Failed to update response " + response.content.decode("utf-8"))
            raise AirflowException('failed to update ' + self.function_name)
        else:
            self.log.info("Function was updated")

    def does_function_exist(self):
        url = self.get_conn().host + self.GET_FUNCTION + self.function_name

        response = requests.get(url)
        if response.ok:
            return True
        else:
            self.log.error("Failed to find function " + self.function_name)
            return False
