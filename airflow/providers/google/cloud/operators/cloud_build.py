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
"""Operators that integrat with Google Cloud Build service."""
import re
from copy import deepcopy
from typing import Any, Dict, Iterable, Optional
from urllib.parse import unquote, urlparse

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.utils.decorators import apply_defaults

REGEX_REPO_PATH = re.compile(r"^/p/(?P<project_id>[^/]+)/r/(?P<repo_name>[^/]+)")


class BuildProcessor:
    """
    Processes build configurations to add additional functionality to support the use of operators.

    The following improvements are made:

    * It is required to provide the source and only one type can be given,
    * It is possible to provide the source as the URL address instead dict.

    :param body: The request body.
        See: https://cloud.google.com/cloud-build/docs/api/reference/rest/Shared.Types/Build
    :type body: dict
    """
    def __init__(self, body: Dict) -> None:
        self.body = deepcopy(body)

    def _verify_source(self):
        is_storage = "storageSource" in self.body["source"]
        is_repo = "repoSource" in self.body["source"]

        sources_count = sum([is_storage, is_repo])

        if sources_count != 1:
            raise AirflowException(
                "The source could not be determined. Please choose one data source from: "
                "storageSource and repoSource."
            )

    def _reformat_source(self):
        self._reformat_repo_source()
        self._reformat_storage_source()

    def _reformat_repo_source(self):
        if "repoSource" not in self.body["source"]:
            return

        source = self.body["source"]["repoSource"]

        if not isinstance(source, str):
            return

        self.body["source"]["repoSource"] = self._convert_repo_url_to_dict(source)

    def _reformat_storage_source(self):
        if "storageSource" not in self.body["source"]:
            return

        source = self.body["source"]["storageSource"]

        if not isinstance(source, str):
            return

        self.body["source"]["storageSource"] = self._convert_storage_url_to_dict(source)

    def process_body(self):
        """
        Processes the body passed in the constructor

        :return: the body.
        :type: dict
        """
        self._verify_source()
        self._reformat_source()
        return self.body

    @staticmethod
    def _convert_repo_url_to_dict(source):
        """
        Convert url to repository in Google Cloud Source to a format supported by the API

        Example valid input:

        .. code-block:: none

            https://source.developers.google.com/p/airflow-project/r/airflow-repo#branch-name

        """
        url_parts = urlparse(source)

        match = REGEX_REPO_PATH.search(url_parts.path)

        if url_parts.scheme != "https" or url_parts.hostname != "source.developers.google.com" or not match:
            raise AirflowException(
                "Invalid URL. You must pass the URL in the format: "
                "https://source.developers.google.com/p/airflow-project/r/airflow-repo#branch-name"
            )

        project_id = unquote(match.group("project_id"))
        repo_name = unquote(match.group("repo_name"))

        source_dict = {"projectId": project_id, "repoName": repo_name, "branchName": "master"}

        if url_parts.fragment:
            source_dict["branchName"] = url_parts.fragment

        return source_dict

    @staticmethod
    def _convert_storage_url_to_dict(storage_url: str) -> Dict[str, Any]:
        """
        Convert url to object in Google Cloud Storage to a format supported by the API

        Example valid input:

        .. code-block:: none

            gs://bucket-name/object-name.tar.gz

        """
        url_parts = urlparse(storage_url)

        if url_parts.scheme != "gs" or not url_parts.hostname or not url_parts.path or url_parts.path == "/":
            raise AirflowException(
                "Invalid URL. You must pass the URL in the format: "
                "gs://bucket-name/object-name.tar.gz#24565443"
            )

        source_dict = {"bucket": url_parts.hostname, "object": url_parts.path[1:]}

        if url_parts.fragment:
            source_dict["generation"] = url_parts.fragment

        return source_dict


class CloudBuildCreateOperator(BaseOperator):
    """
    Starts a build with the specified configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudBuildCreateOperator`

    :param body: The request body.
        See: https://cloud.google.com/cloud-build/docs/api/reference/rest/Shared.Types/Build
    :type body: dict
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (for example v1 or v1beta1).
    :type api_version: str
    """

    template_fields = ("body", "gcp_conn_id", "api_version")  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 body: dict,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = "google_cloud_default",
                 api_version: str = "v1",
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.body = body
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        body = BuildProcessor(body=self.body).process_body()
        return hook.create_build(body=body, project_id=self.project_id)
