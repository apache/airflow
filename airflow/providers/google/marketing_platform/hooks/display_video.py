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
"""
This module contains Google DisplayVideo hook.
"""
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import Resource, build

from airflow.gcp.hooks.base import CloudBaseHook


class GoogleDisplayVideo360Hook(CloudBaseHook):
    """
    Hook for Google Display & Video 360.
    """

    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """
        Retrieves connection to DisplayVideo.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "doubleclickbidmanager",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def create_query(self, query: Dict[str, Any]) -> Dict:
        """
        Creates a query.

        :param query: Query object to be passed to request body.
        :type query: Dict[str, Any]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .createquery(body=query)
            .execute(num_retries=self.num_retries)
        )
        return response

    def delete_query(self, query_id: str) -> None:
        """
        Deletes a stored query as well as the associated stored reports.

        :param query_id: Query ID to delete.
        :type query_id: str
        """
        (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .deletequery(queryId=query_id)
            .execute(num_retries=self.num_retries)
        )

    def get_query(self, query_id: str) -> Dict:
        """
        Retrieves a stored query.

        :param query_id: Query ID to retrieve.
        :type query_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .getquery(queryId=query_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def list_queries(self, ) -> List[Dict]:
        """
        Retrieves stored queries.

        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .listqueries()
            .execute(num_retries=self.num_retries)
        )
        return response.get('queries', [])

    def run_query(self, query_id: str, params: Dict[str, Any]) -> None:
        """
        Runs a stored query to generate a report.

        :param query_id: Query ID to run.
        :type query_id: str
        :param params: Parameters for the report.
        :type params: Dict[str, Any]
        """
        (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .runquery(queryId=query_id, body=params)
            .execute(num_retries=self.num_retries)
        )
