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
import json
from unittest import mock

from airflow.models import Connection

GCP_PROJECT_ID_HOOK_UNIT_TEST = 'example-project'


def mock_base_gcp_hook_default_project_id(
    self,
    gcp_conn_id='google_cloud_default',
    delegate_to=None,
    impersonation_chain=None,
):
    self.extras = {'extra__google_cloud_platform__project': GCP_PROJECT_ID_HOOK_UNIT_TEST}
    self._conn = gcp_conn_id
    self.delegate_to = delegate_to
    self.impersonation_chain = impersonation_chain
    self._client = None
    self._conn = None
    self._cached_credentials = None
    self._cached_project_id = None


def mock_base_gcp_hook_no_default_project_id(
    self,
    gcp_conn_id='google_cloud_default',
    delegate_to=None,
    impersonation_chain=None,
):
    self.extras = {}
    self._conn = gcp_conn_id
    self.delegate_to = delegate_to
    self.impersonation_chain = impersonation_chain
    self._client = None
    self._conn = None
    self._cached_credentials = None
    self._cached_project_id = None


GCP_CONNECTION_WITH_PROJECT_ID = Connection(
    extra=json.dumps({'extra__google_cloud_platform__project': GCP_PROJECT_ID_HOOK_UNIT_TEST})
)

GCP_CONNECTION_WITHOUT_PROJECT_ID = Connection(extra=json.dumps({}))


def get_open_mock():
    mck = mock.mock_open()
    open_module = 'builtins'
    return mck, open_module
