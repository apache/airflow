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
#

from airflow.hooks.base_hook import BaseHook

import jenkins
from distutils.util import strtobool


class JenkinsHook(BaseHook):
    """
    Hook to manage connection to jenkins server
    """

    def __init__(self, conn_id='jenkins_default'):
        connection = self.get_connection(conn_id)
        self.connection = connection
        connectionPrefix = 'http'
        # connection.extra contains info about using https (true) or http (false)
        if connection.extra is None or connection.extra == '':
            connection.extra = 'false'
            # set a default value to connection.extra
            # to avoid rising ValueError in strtobool
        if strtobool(connection.extra):
            connectionPrefix = 'https'
        url = '%s://%s:%d' % (connectionPrefix, connection.host, connection.port)
        self.log.info('Trying to connect to %s', url)
        self.jenkins_server = jenkins.Jenkins(url, connection.login, connection.password)

    def get_jenkins_server(self):
        return self.jenkins_server
