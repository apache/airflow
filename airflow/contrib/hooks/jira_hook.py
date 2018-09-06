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
from jira import JIRA
from jira.exceptions import JIRAError

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class JiraHook(BaseHook, LoggingMixin):
    """
    Jira interaction hook, a Wrapper around JIRA Python SDK.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    """
    def __init__(self,
                 jira_conn_id='jira_default',
                 proxies=None):
        super(JiraHook, self).__init__(jira_conn_id)
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
        self.client = None
        self.get_conn()

    def get_conn(self):
        if not self.client:
            self.log.debug('Creating Jira client for conn_id: %s', self.jira_conn_id)

            get_server_info = True
            validate = True
            extra_options = {}
            conn = None

            if self.jira_conn_id is not None:
                conn = self.get_connection(self.jira_conn_id)
                if conn.extra is not None:
                    extra_options = conn.extra_dejson
                    # only required attributes are taken for now,
                    # more can be added ex: async, logging, max_retries

                    # verify
                    if 'verify' in extra_options \
                            and extra_options['verify'].lower() == 'false':
                        extra_options['verify'] = False

                    # validate
                    if 'validate' in extra_options \
                            and extra_options['validate'].lower() == 'false':
                        validate = False

                    if 'get_server_info' in extra_options \
                            and extra_options['get_server_info'].lower() == 'false':
                        get_server_info = False

                try:
                    self.client = JIRA(conn.host,
                                       options=extra_options,
                                       basic_auth=(conn.login, conn.password),
                                       get_server_info=get_server_info,
                                       validate=validate,
                                       proxies=self.proxies)
                except JIRAError as jira_error:
                    raise AirflowException('Failed to create jira client, jira error: %s'
                                           % str(jira_error))
                except Exception as e:
                    raise AirflowException('Failed to create jira client, error: %s'
                                           % str(e))

        return self.client
