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

from slack import WebClient
from slack.errors import SlackClientError

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SlackHook(BaseHook):
    """
       Interact with Slack, using slackclient library.
    """

    def __init__(self, token=None, slack_conn_id=None, **client_args):
        """
        Creates a Slack connection, to be used for calls.

        Takes both Slack API token directly and connection that has Slack API token.
        If both supplied, Slack API token will be used.

        Exposes also the rest of slack.WebClient args.

        :param token: Slack API token
        :type token: str
        :param slack_conn_id: connection that has Slack API token in the password field
        :type slack_conn_id: str
        :param use_session: A boolean specifying if the client should take advantage of
        connection pooling. Default is True.
        :type base_url: bool
        :param base_url: A string representing the Slack API base URL. Default is
        `https://www.slack.com/api/`
        :type base_url: str
        :param timeout: The maximum number of seconds the client will wait
            to connect and receive a response from Slack.
            Default is 30 seconds.
        :type timeout: int
        """
        token = self.__get_token(token, slack_conn_id)
        self.client = WebClient(token, **client_args)

    def __get_token(self, token, slack_conn_id):
        if token is not None:
            return token

        if slack_conn_id is not None:
            conn = self.get_connection(slack_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password
        else:
            raise AirflowException('Cannot get token: '
                                   'No valid Slack token nor slack_conn_id supplied.')

    def call(self, method, api_params):
        rc = self.client.api_call(method, **api_params)

        try:
            rc.validate()
        except SlackClientError as exc:
            msg = f"Slack API call failed ({exc})"
            raise AirflowException(msg)
