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


_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


class GoogleCloudBaseHook(BaseHook, LoggingMixin):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call googleapiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.

    All hook derived from this base hook use the 'Google Cloud Platform' connection
    type. Three ways of authentication are supported:

    Default credentials: Only the 'Project Id' is required. You'll need to
    have set up default credentials, such as by the
    ``GOOGLE_APPLICATION_DEFAULT`` environment variable or from the metadata
    server on Google Compute Engine.

    JSON key file: Specify 'Project Id', 'Keyfile Path' and 'Scope'.

    Legacy P12 key files are not supported.

    JSON data provided in the UI: Specify 'Keyfile JSON'.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        """
        :param gcp_conn_id: The connection ID to use when fetching connection info.
        :type gcp_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: str
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
        if scope:
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
                self.log.debug('Getting connection using JSON key file %s' % key_path)
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
        http = httplib2.Http()
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

    @property
    def project_id(self):
        return self._get_field('project')
