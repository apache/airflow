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
"""Hook for Slack"""
from typing import Optional

from slackclient import SlackClient

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


# noinspection PyAbstractClass
class SlackHook(BaseHook):
    """
    Takes both Slack API token directly and connection that has Slack API token.

    If both supplied, Slack API token will be used.

    :param token: Slack API token
    :param slack_conn_id: connection that has Slack API token in the password field
    """
    def __init__(self, token: Optional[str] = None, slack_conn_id: Optional[str] = None) -> None:
        self.token = self.__get_token(token, slack_conn_id)

    def __get_token(self, token, slack_conn_id):
        if token is not None:
            return token
        elif slack_conn_id is not None:
            conn = self.get_connection(slack_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password
        else:
            raise AirflowException('Cannot get token: '
                                   'No valid Slack token nor slack_conn_id supplied.')

    def call(self, method: str, api_params: dict) -> None:
        """
        Calls the Slack client.

        :param method: method
        :param api_params: parameters of the API
        """
        slack_client = SlackClient(self.token)
        return_code = slack_client.api_call(method, **api_params)

        if not return_code['ok']:
            msg = "Slack API call failed ({})".format(return_code['error'])
            raise AirflowException(msg)
