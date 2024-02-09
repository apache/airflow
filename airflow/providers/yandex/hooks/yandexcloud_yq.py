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
from __future__ import annotations

import logging

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.exceptions import AirflowException
import requests
from requests.packages.urllib3.util.retry import Retry
from enum import Enum
import time
from datetime import timedelta, datetime
import aiohttp

class QueryType(Enum):
    ANALYTICS = 1
    STREAMING = 2

class YQHook(YandexCloudBaseHook):
    """
    A base hook for Yandex.Cloud Data Proc.

    :param yandex_conn_id: The connection ID to use when fetching connection info.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.query_id: str | None = None


    def start_execute_query(self, query_type: QueryType, query_text: str|None,  name: str|None=None, description: str | None = None) -> str:
        # self.default_folder_id
        type = "ANALYTICS" if query_type == QueryType.ANALYTICS else "STREAMING"

        with YQHook.create_session(self.get_iam_token()) as session:

            data = {"name": name,
                    "type": type,
                    "text": query_text,
                    "description": description}
            self.log.info(f"folder={self.default_folder_id}")
            response = session.post(f"https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries?project={self.default_folder_id}",
                        json=data)
            response.raise_for_status()

            self.query_id = response.json()["id"]

        return self.query_id

    def wait_for_query_to_complete(self, execution_timeout: timedelta):
        status = None
        try:
            status = self.wait_results(self.query_id, execution_timeout)
        except TimeoutError as err:
            self.stop_query(self.query_id)
            raise

    def get_query_result(self, query_id):
        self.log.info(f"get_query_result query_id={query_id}")
        query_info = self.get_queryinfo(query_id)
        if query_info["status"] == "FAILED":
            issues = query_info["issues"]
            raise RuntimeError("Query failed", issues=issues)

        result_set_count = len(query_info["result_sets"])
        self.log.debug(f"result set count {result_set_count}")

        query_results = self.query_results(query_id, result_set_count)
        self.log.debug(query_results)
        return query_results

    def get_pandas_df(self)-> pd.DataFrame:
        return pd.DataFrame()

    def query_results(self, query_id:str, result_set_count:int)->object:
        results = list()
        limit = 1000
        offset = 0

        iam_token = self.get_iam_token()
        with YQHook.create_session(iam_token) as session:
            for result_index in range(0, result_set_count):
                columns = None
                rows = []
                while True:
                    print(f"limit={limit} offset={offset}")
                    response = session.get(f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{query_id}/results/{result_index}?project={self.default_folder_id}&limit={limit}&offset={offset}')
                    response.raise_for_status()

                    qresults = response.json()
                    print(qresults)
                    if columns is None:
                        columns = qresults["columns"]

                    rows.extend( qresults["rows"])
                    if len(qresults["rows"]) != limit:
                        break
                    else:
                        offset += limit

                results.append({"rows":rows, "columns": columns})

        if len(results) == 1:
            return results[0]
        else:
            return results

    def stop_current_query(self)->None:
        self.stop_query(self.query_id)

    def get_queryinfo(self, query_id:str)->object:
        iam_token = self.get_iam_token()
        with YQHook.create_session(iam_token) as session:
            response = session.get(f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{query_id}?project={self.default_folder_id}')
            response.raise_for_status()
            print(response.json())

            return response.json()

    def stop_query(self, query_id:str)->None:
        iam_token = self.get_iam_token()
        with YQHook.create_session(iam_token) as session:
            session.get(f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{query_id}/stop?project={self.default_folder_id}')

    def get_query_status(self, query_id:str)->str:
        iam_token = self.get_iam_token()
        with YQHook.create_session(iam_token) as session:
            response = session.get(f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{query_id}/status?project={self.default_folder_id}')
            response.raise_for_status()
            status = response.json()["status"]
            return status

    async def get_query_status_async(self, query_id:str)->str:
        iam_token = self.get_iam_token()

        headers = YQHook.get_request_url_header_params(iam_token)
        url = f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{query_id}/status?project={self.default_folder_id}'

        self.log.info(f"Retrieving status for query id {query_id}, url {url}")

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                status_code = response.status
                assert status_code == 200
                resp = await response.json()
                return resp["status"]

    @staticmethod
    def get_request_url_header_params(iam_token: str|None=None)->dict[str,str]:
        print(f"get_request_url_header_params iam_token={iam_token}")
        headers = {}
        if iam_token is not None:
            headers['Authorization'] = f"{iam_token}"

        return headers

    def wait_results(self, query_id:str, execution_timeout: timedelta)->str:
        execution_timeout = execution_timeout if execution_timeout is not None else timedelta(minutes=30)

        start = datetime.now()
        while True:
            if datetime.now() > start + execution_timeout:
                raise TimeoutError("Query execution timeout")

            status = self.get_query_status(query_id)
            if status not in ["RUNNING", "PENDING"]:
                return status

            time.sleep(2)

    @staticmethod
    def create_session(iamtoken: str | None = None) -> requests.Session:
        session = requests.Session()
        session.verify = False
        session.timeout = 20
        retry = Retry(
            backoff_factor=0.3,
            total=10
        )
        session.mount(
            'http://',
            requests.adapters.HTTPAdapter(max_retries=retry)
        )
        session.mount(
            'https://',
            requests.adapters.HTTPAdapter(max_retries=retry)
        )

        headers = YQHook.get_request_url_header_params(iamtoken)
        for k, v in headers.items():
            session.headers[k] = v

        return session


