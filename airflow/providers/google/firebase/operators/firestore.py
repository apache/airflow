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

from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.firebase.hooks.firestore import CloudFirestoreHook
from airflow.utils.decorators import apply_defaults


class CloudFirestoreExportDatabaseOperator(BaseOperator):
    """
    Exports a copy of all or a subset of documents from Google Cloud Firestore to another storage system,
    such as Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFirestoreExportDatabaseOperator`

    :param database_id: The Database ID.
    :type database_id: str
    :param body: The request body.
        See:
        https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
    :type body: dict
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (for example v1 or v1beta1).
    :type api_version: str
    """

    template_fields = ("body", "gcp_conn_id", "api_version")

    @apply_defaults
    def __init__(
        self,
        body: Dict,
        database_id: str = "(default)",
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.database_id = database_id
        self.body = body
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")

    def execute(self, context):
        hook = CloudFirestoreHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        return hook.export_documents(database_id=self.database_id, body=self.body, project_id=self.project_id)
