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
import json

import httplib2
import google.auth
import google_auth_httplib2
import google.oauth2.service_account

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.helpers import convert_to_int

_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


class GoogleCloudBaseHook(BaseHook, LoggingMixin):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call apiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.

    All hook derived from this base hook use the 'Google Cloud Platform' connection
    type. Two ways of authentication are supported:

    Default credentials: Only the 'Project Id' is required. You'll need to
    have set up default credentials, such as by the
    ``GOOGLE_APPLICATION_DEFAULT`` environment variable or from the metadata
    server on Google Compute Engine.

    JSON key file: Specify 'Project Id', 'Key Path' and 'Scope'.

    Legacy P12 key files are not supported.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        """
        :param gcp_conn_id: The connection ID to use when fetching connection info.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
        """
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(self.gcp_conn_id).extra_dejson

    def _get_credentials(self):
        """
        Returns the Credentials object for Google API
        """
        key_path = self._get_field('key_path', False)
        keyfile_dict = self._get_field('keyfile_dict', False)
        scope = self._get_field('scope', None)
        if scope is not None:
            scopes = [s.strip() for s in scope.split(',')]
        else:
            scopes = _DEFAULT_SCOPES

        if not key_path and not keyfile_dict:
            self.log.info('Getting connection using `google.auth.default()` '
                          'since no key file is defined for hook.')
            credentials, _ = google.auth.default(scopes=scopes)
        elif key_path:
            # Get credentials from a JSON file.
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_file(
                        key_path, scopes=scopes)
                )
            elif key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise AirflowException('Unrecognised extension for key file.')
        else:
            # Get credentials from JSON data provided in the UI.
            try:
                keyfile_dict = json.loads(keyfile_dict)

                # Depending on how the JSON was formatted, it may contain
                # escaped newlines. Convert those to actual newlines.
                keyfile_dict['private_key'] = keyfile_dict['private_key'].replace(
                    '\\n', '\n')

                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        keyfile_dict, scopes=scopes)
                )
            except json.decoder.JSONDecodeError:
                raise AirflowException('Invalid key JSON.')

        return credentials.with_subject(self.delegate_to) \
            if self.delegate_to else credentials

    def _get_access_token(self):
        """
        Returns a valid access token from Google API Credentials
        """
        return self._get_credentials().token

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        proxy_obj = self._get_proxy_obj()
        http = httplib2.Http(proxy_info=proxy_obj)
        authed_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=http)
        return authed_http

    def _get_field(self, f, default=None):
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The google_cloud_platform hook type adds custom UI elements
        to the hook page, which allow admins to specify service_account,
        key_path, etc. They get formatted as shown below.
        """
        long_f = 'extra__google_cloud_platform__{}'.format(f)
        if long_f in self.extras:
            return self.extras[long_f]
        else:
            return default

    def _get_proxy_obj(self):
        """
        Returns proxy object with proxy details auch as host, port and type
        """
        proxy_obj = None
        if self._get_useproxy() is True:
            proxy = self.get_proxyconfig()
            proxy_host = proxy.get('proxy_host')
            proxy_port = convert_to_int(proxy.get('proxy_port'), 8080)
            proxy_type = self._get_proxy_type(proxy)
            proxy_obj = httplib2.ProxyInfo(proxy_type, proxy_host, proxy_port)
        return proxy_obj

    def _get_proxy_type(self, proxy):
        """
        :param proxy: Proxy details fetched from configuration file
        :return: Proxy type
        """
        proxy_type_dictionary = {
            "SOCKS4": httplib2.socks.PROXY_TYPE_SOCKS4,
            "SOCKS5": httplib2.socks.PROXY_TYPE_SOCKS5,
            "HTTP": httplib2.socks.PROXY_TYPE_HTTP,
            "HTTP_NO_TUNNEL": httplib2.socks.PROXY_TYPE_HTTP_NO_TUNNEL
        }

        proxy_type_from_config = proxy.get('proxy_type')
        proxy_type = proxy_type_dictionary.get(proxy_type_from_config)

        if proxy_type is None:
            self.log.info("Proxy type does not exist returning proxy type as None")
        return proxy_type

    def _get_useproxy(self):
        """
        Fetch use_proxy field from config file
        """
        use_proxy = None
        try:
            use_proxy = conf.getboolean('core', 'use_proxy')
        except AirflowConfigException:
            use_proxy = False
        return use_proxy

    @property
    def project_id(self):
        return self._get_field('project')
