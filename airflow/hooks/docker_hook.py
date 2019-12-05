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

from docker import APIClient
from docker.errors import APIError

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DockerHook(BaseHook, LoggingMixin):
    """
    Interact with a private Docker registry.

    :param docker_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :type docker_conn_id: str
    """
    def __init__(self,
                 docker_conn_id='docker_default',
                 base_url=None,
                 version=None,
                 tls=None
                 ):
        if not base_url:
            raise AirflowException('No Docker base URL provided')
        if not version:
            raise AirflowException('No Docker API version provided')

        conn = self.get_connection(docker_conn_id)
        if not conn.host:
            raise AirflowException('No Docker registry URL provided')
        if not conn.login:
            raise AirflowException('No username provided')
        extra_options = conn.extra_dejson

        self.__base_url = base_url
        self.__version = version
        self.__tls = tls
        if conn.port:
            self.__registry = "{}:{}".format(conn.host, conn.port)
        else:
            self.__registry = conn.host
        self.__username = conn.login
        self.__password = conn.password
        self.__email = extra_options.get('email')
        self.__reauth = False if extra_options.get('reauth') == 'no' else True

    def get_conn(self):
        client = APIClient(
            base_url=self.__base_url,
            version=self.__version,
            tls=self.__tls
        )
        self.__login(client)
        return client

    def __login(self, client):
        self.log.debug('Logging into Docker registry')
        try:
            client.login(
                username=self.__username,
                password=self.__password,
                registry=self.__registry,
                email=self.__email,
                reauth=self.__reauth
            )
            self.log.debug('Login successful')
        except APIError as docker_error:
            self.log.error('Docker registry login failed: %s', str(docker_error))
            raise AirflowException('Docker registry login failed: %s', str(docker_error))
