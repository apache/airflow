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
from typing import Any, Dict, List

from googleapiclient.discovery import Resource, build

from airflow.providers.google.cloud.hooks.base import CloudBaseHook


class GoogleAnalyticsHook(CloudBaseHook):
    """
    Hook for Google Analytics 360.
    """

    def __init__(
        self,
        api_version: str = "v3",
        gcp_connection_id: str = "google cloud default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_version = api_version
        self.gcp_connection_is = gcp_connection_id
        self._conn = None

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Google Analytics 360.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "analytics",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def list_accounts(self) -> List[Dict[str, Any]]:
        """
        Lists accounts list from Google Analytics 360.
        """

        self.log.info("Retrieving accounts list...")
        result = []  # type: List[Dict]
        conn = self.get_conn()
        accounts = conn.management().accounts()  # pylint: disable=no-member
        while True:
            # start index has value 1
            request = accounts.list(start_index=len(result) + 1)
            response = request.execute(num_retries=self.num_retries)
            result.extend(response.get('items', []))
            # result is the number of fetched accounts from Analytics
            # when all accounts will be add to the result
            # the loop will be break
            if response["totalResults"] <= len(result):
                break
        return result
