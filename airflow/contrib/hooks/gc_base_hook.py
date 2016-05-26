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
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from oauth2client.client import SignedJwtAssertionCredentials, GoogleCredentials

class GoogleCloudBaseHook(BaseHook):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call apiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.
    """

    def __init__(self, conn_id, delegate_to=None):
        """
        :param conn_id: The connection ID to use when fetching connection info.
        :type conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide delegation enabled.
        :type delegate_to: string
        """
        self.conn_id = conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(conn_id).extra_dejson

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        service_account = self._get_field('service_account', False)
        key_path = self._get_field('key_path', False)
        scope = self._get_field('scope', False)

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not key_path or not service_account:
            logging.info('Getting connection using `gcloud auth` user, since no service_account/key_path are defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        elif self.scope:
            with open(key_path, 'rb') as key_file:
                key = key_file.read()
                credentials = SignedJwtAssertionCredentials(
                    service_account,
                    key,
                    scope=self.scope,
                    **kwargs)
        else:
            raise AirflowException('Scope undefined, or either key_path/service_account config was missing.')

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
