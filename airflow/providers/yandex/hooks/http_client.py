from __future__ import annotations

import logging
import time
from datetime import datetime
import requests

from requests.adapters import HTTPAdapter
from typing import Any
from urllib3.util.retry import Retry

MAX_RETRY_FOR_SESSION = 4
BACK_OFF_FACTOR = 0.3
TIME_BETWEEN_RETRIES = 1000
ERROR_CODES = (500, 502, 504)


def requests_retry_session(session,
                           retries=MAX_RETRY_FOR_SESSION,
                           back_off_factor=BACK_OFF_FACTOR,
                           status_force_list=ERROR_CODES):
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=back_off_factor,
                  status_forcelist=status_force_list,
                  allowed_methods=frozenset(['GET', 'POST']))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class YQHttpClientConfig(object):
    def __init__(self,
                 token: str | None = None,
                 project: str | None = None,
                 user_agent: str | None = "Python YQ HTTP SDK") -> None:

        assert len(token) > 0, "empty token"
        self.token = token
        self.project = project
        self.user_agent = user_agent

        # urls should not contain trailing /
        self.endpoint: str = "https://api.yandex-query.cloud.yandex.net"
        self.web_base_url: str = "https://yq.cloud.yandex.ru"
        self.token_prefix = "Bearer "


class YQHttpClientException(Exception):
    def __init__(self, message: str, status: str, msg: str, details: Any) -> None:
        super().__init__(message)
        self.status = status
        self.msg = msg
        self.details = details


class YQHttpClient(object):
    def __init__(self, config: YQHttpClientConfig):
        self.config = config
        self.session = requests_retry_session(session=requests.Session())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def _build_headers(self, idempotency_key=None, request_id=None) -> dict[str, str]:
        headers = {
            "Authorization": f"{self.config.token_prefix}{self.config.token}"
        }
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        if request_id is not None:
            headers["x-request-id"] = request_id

        if self.config.user_agent is not None:
            headers["User-Agent"] = self.config.user_agent

        return headers

    def _build_params(self) -> dict[str, str]:
        params = {}
        if self.config.project is not None:
            params["project"] = self.config.project

        return params

    def _compose_api_url(self, path: str) -> str:
        return self.config.endpoint + path

    def _compose_web_url(self, path: str) -> str:
        return self.config.web_base_url + path

    def _validate_http_error(self, response, expected_code=200) -> None:
        logging.info(f"Response: {response.status_code}, {response.text}")
        if response.status_code != expected_code:
            if response.headers.get("Content-Type", "").startswith("application/json"):
                body = response.json()
                status = body.get("status")
                msg = body.get("message")
                details = body.get("details")
                raise YQHttpClientException(f"Error occurred. http code={response.status_code}, status={status}, msg={msg}, details={details}",
                                            status=status,
                                            msg=msg,
                                            details=details
                                            )

            raise YQHttpClientException(f"Error occurred: {response.status_code}, {response.text}")

    def create_query(self,
                     query_text=None,
                     type=None,
                     name=None,
                     description=None,
                     idempotency_key=None,
                     request_id=None,
                     expected_code=200):
        body = dict()
        if query_text is not None:
            body["text"] = query_text

        if type is not None:
            body["type"] = type

        if name is not None:
            body["name"] = name

        if description is not None:
            body["description"] = description

        response = self.session.post(self._compose_api_url("/api/fq/v1/queries"),
                                     headers=self._build_headers(idempotency_key=idempotency_key,
                                                                 request_id=request_id),
                                     params=self._build_params(),
                                     json=body)

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()["id"]

    def get_query_status(self, query_id, request_id=None, expected_code=200) -> Any:
        response = self.session.get(
            self._compose_api_url(f"/api/fq/v1/queries/{query_id}/status"),
            headers=self._build_headers(request_id=request_id),
            params=self._build_params()
        )

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()["status"]

    def get_query(self, query_id, request_id=None, expected_code=200) -> Any:
        response = self.session.get(
            self._compose_api_url(f"/api/fq/v1/queries/{query_id}"),
            headers=self._build_headers(request_id=request_id),
            params=self._build_params()
        )

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def stop_query(self,
                   query_id: str,
                   idempotency_key: str | None = None,
                   request_id: str | None = None,
                   expected_code: int = 204) -> Any:

        headers = self._build_headers(
            idempotency_key=idempotency_key,
            request_id=request_id
        )
        response = self.session.post(self._compose_api_url(f"/api/fq/v1/queries/{query_id}/stop"),
                                     headers=headers,
                                     params=self._build_params())
        self._validate_http_error(response, expected_code=expected_code)
        return response

    def wait_query_to_complete(self, query_id, execution_timeout=None, stop_on_timeout=False) -> str:
        status = None
        delay = 0.2  # start with 0.2 sec
        try:
            start = datetime.now()
            while True:
                if execution_timeout is not None and datetime.now() > start + execution_timeout:
                    raise TimeoutError(f"Query {query_id} execution timeout, last status {status}")

                status = self.get_query_status(query_id)
                if status not in ["RUNNING", "PENDING"]:
                    return status

                time.sleep(delay)
                delay *= 2
                delay = min(2, delay)  # up to 2 seconds

        except TimeoutError:
            if stop_on_timeout:
                self.stop_query(query_id)
            raise

    def wait_query_to_succeed(self, query_id, execution_timeout=None, stop_on_timeout=False) -> int:
        status = self.wait_query_to_complete(
            query_id=query_id,
            execution_timeout=execution_timeout,
            stop_on_timeout=stop_on_timeout
        )

        query = self.get_query(query_id)
        if status != "COMPLETED":
            issues = query["issues"]
            raise RuntimeError(f"Query {query_id} failed", issues=issues)

        return len(query["result_sets"])

    def get_query_result_set_page(self,
                               query_id,
                               result_set_index,
                               offset=None,
                               limit=None,
                               raw_format=False,
                               request_id=None,
                               expected_code=200) -> Any:
        params = self._build_params()
        if offset is not None:
            params["offset"] = offset

        if limit is not None:
            params["limit"] = limit

        response = self.session.get(
            self._compose_api_url(f"/api/fq/v1/queries/{query_id}/results/{result_set_index}"),
            headers=self._build_headers(request_id=request_id),
            params=params
        )

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def get_query_result_set(self, query_id: str, result_set_index: int, raw_format: bool = False) -> Any:
        offset = 0
        limit = 1000
        columns = None
        rows = []
        while True:
            part = self.get_query_result_set_page(
                query_id,
                result_set_index=result_set_index,
                offset=offset,
                limit=limit,
                raw_format=raw_format
            )

            if columns is None:
                columns = part["columns"]

            r = part["rows"]
            rows.extend(r)
            if len(r) != limit:
                break

            offset += limit

        return {"rows": rows, "columns": columns}

    def get_query_all_result_sets(self, query_id: str, result_set_count: int, raw_format: bool = False) -> Any:
        result = list()
        for i in range(0, result_set_count):
            r = self.get_query_result_set(
                query_id,
                result_set_index=i,
                raw_format=raw_format
            )

            if result_set_count == 1:
                return r

            result.append(r)

        return result

    def get_openapi_spec(self) -> str:
        response = self.session.get(self._compose_api_url("/resources/v1/openapi.yaml"))
        self._validate_http_error(response)
        return response.text

    def compose_query_web_link(self, query_id) -> str:
        return self._compose_web_url(f"/folders/{self.config.project}/ide/queries/{query_id}")
