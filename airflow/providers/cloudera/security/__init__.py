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

"""Security module for handling authentication to Cloudera Services"""
from abc import ABC, abstractmethod
from http import HTTPStatus
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.providers.cloudera.hooks import CdpHookException
from airflow.utils.log.logging_mixin import LoggingMixin, logging  # type: ignore

LOG = logging.getLogger(__name__)


class SecurityError(CdpHookException):
    """Root security exception, to be used to catch any security issue"""


class TokenResponse(ABC, LoggingMixin):
    """Base class for token responses"""

    @abstractmethod
    def is_valid(self) -> bool:
        """Check if token is still valid

        Returns: True if token is valid, false otherwise
        """
        raise NotImplementedError


class ClientError(requests.exceptions.HTTPError):
    """When request fails because of a Client side error"""


class ServerError(requests.exceptions.HTTPError):
    """When request fails because of an Internal/Server side error"""


ALWAYS_RETRY_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    ServerError,
)


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(ALWAYS_RETRY_EXCEPTIONS),
    reraise=True,
)
def submit_request(method, uri, *args: Any, **kw_args) -> requests.Response:
    """
    Helper method for submitting HTTP request and handling common errors
    Args:
    method: http method, GET, POST, etc.
    uri: endpoint of the requests
    args: arguments given to the function
    kw_args: keyword arguments given to the function

    Returns:
    Response of the http request

    Raises:
    ClientError if response status code is 4xx
    ServerError if response status code is 5xx
    corresponding issued requests.exceptions.RequestException if requests throws an error and
    cannot complete successfully
    """
    try:
        LOG.debug("Issuing request: %s %s", method, uri)
        response = requests.request(method, uri, *args, **kw_args)
    except requests.exceptions.RequestException as err:
        print(err)
        LOG.debug("Failed to query endpoint %s %s, error: %s", method, uri, repr(err))
        raise

    if response.status_code >= HTTPStatus.BAD_REQUEST:
        status = str(response.status_code) + ":" + response.reason
        error_msg = (status + ":" + response.text.rstrip()) if response.text else status
        LOG.debug("Failed to query endpoint %s %s, error: %s", method, uri, error_msg)
        if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            raise ClientError(error_msg)
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            raise ServerError(error_msg)

    return response
