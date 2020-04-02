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
"""This module is deprecated. Please use `airflow.providers.google.common.hooks.base_google`."""
import warnings

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.common.hooks.base_google`.",
    DeprecationWarning, stacklevel=2
)

_log = logging.getLogger(__name__)


class GoogleCloudBaseHook(GoogleBaseHook):
    """
    This class is deprecated. Please use `airflow.providers.google.common.hooks.base_google.GoogleBaseHook`.
    """
<<<<<<< HEAD

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

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        key_path = self._get_field('key_path', False)
        scope = self._get_field('scope', False)

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not key_path:
            _log.info('Getting connection using `gcloud auth` user, since no key file '
                         'is defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        else:
            if not scope:
                raise AirflowException('Scope should be defined when using a key file.')
            scopes = [s.strip() for s in scope.split(',')]
            if key_path.endswith('.json'):
                _log.info('Getting connection using a JSON key file.')
                credentials = ServiceAccountCredentials\
                    .from_json_keyfile_name(key_path, scopes)
            elif key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise AirflowException('Unrecognised extension for key file.')

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
=======
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This class is deprecated. Please use "
            "`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`.",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d
