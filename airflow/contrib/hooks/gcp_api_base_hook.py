# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import httplib2
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


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

    Default credentials: Only specify 'Project Id'. Then you need to have executed
    ``gcloud auth`` on the Airflow worker machine.

    JSON key file: Specify 'Project Id', 'Key Path' and 'Scope'.

    Legacy P12 key files are not supported.
    """
    def __init__(self, conn_id, delegate_to=None):
        """
        :param conn_id: The connection ID to use when fetching connection info.
        :type conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
        """
        self.conn_id = conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(conn_id).extra_dejson

    def _get_credentials(self):
        """
        Returns the Credentials object for Google API
        """
        key_path = self._get_field('key_path', False)
        scope = self._get_field('scope', False)

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not key_path:
            self.log.info('Getting connection using `gcloud auth` user, since no key file '
                         'is defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        else:
            if not scope:
                raise AirflowException('Scope should be defined when using a key file.')
            scopes = [s.strip() for s in scope.split(',')]
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                credentials = ServiceAccountCredentials\
                    .from_json_keyfile_name(key_path, scopes)
            elif key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise AirflowException('Unrecognised extension for key file.')
        return credentials

    def _get_access_token(self):
        """
        Returns a valid access token from Google API Credentials
        """
        return self._get_credentials().get_access_token().access_token

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        http = httplib2.Http()
        return credentials.authorize(http)

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
