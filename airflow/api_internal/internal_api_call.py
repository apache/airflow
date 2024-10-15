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

import inspect
import json
import logging
from functools import wraps
from http import HTTPStatus
from typing import Callable, TypeVar
from urllib.parse import urlparse

import requests
import tenacity
from urllib3.exceptions import NewConnectionError

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.settings import _ENABLE_AIP_44, force_traceback_session_for_untrusted_components
from airflow.typing_compat import ParamSpec
from airflow.utils.jwt_signer import JWTSigner

PS = ParamSpec("PS")
RT = TypeVar("RT")

logger = logging.getLogger(__name__)

class AirflowHttpException(AirflowException):
    """Raise when there is a problem during an http request."""

    def __init__(self, message: str, status_code: HTTPStatus):
        super().__init__(message)
        self.status_code = status_code

class InternalApiConfig:
    """Stores and caches configuration for Internal API."""

    _use_internal_api = False
    _internal_api_endpoint = ""

    @staticmethod
    def set_use_database_access(component: str):
        """
        Block current component from using Internal API.

        All methods decorated with internal_api_call will always be executed locally.`
        This mode is needed for "trusted" components like Scheduler, Webserver, Internal Api server
        """
        InternalApiConfig._use_internal_api = False
        if not _ENABLE_AIP_44:
            raise RuntimeError("The AIP_44 is not enabled so you cannot use it. ")
        logger.info(
            "DB isolation mode. But this is a trusted component and DB connection is set. "
            "Using database direct access when running %s.",
            component,
        )

    @staticmethod
    def set_use_internal_api(component: str, allow_tests_to_use_db: bool = False):
        if not _ENABLE_AIP_44:
            raise RuntimeError("The AIP_44 is not enabled so you cannot use it. ")
        internal_api_url = conf.get("core", "internal_api_url")
        url_conf = urlparse(internal_api_url)
        api_path = url_conf.path
        if api_path in ["", "/"]:
            # Add the default path if not given in the configuration
            api_path = "/internal_api/v1/rpcapi"
        if url_conf.scheme not in ["http", "https"]:
            raise AirflowConfigException("[core]internal_api_url must start with http:// or https://")
        internal_api_endpoint = f"{url_conf.scheme}://{url_conf.netloc}{api_path}"
        InternalApiConfig._use_internal_api = True
        InternalApiConfig._internal_api_endpoint = internal_api_endpoint
        logger.info("DB isolation mode. Using internal_api when running %s.", component)
        force_traceback_session_for_untrusted_components(allow_tests_to_use_db=allow_tests_to_use_db)

    @staticmethod
    def get_use_internal_api():
        return InternalApiConfig._use_internal_api

    @staticmethod
    def get_internal_api_endpoint():
        return InternalApiConfig._internal_api_endpoint


def internal_api_call(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Allow methods to be executed in database isolation mode.

    If [core]database_access_isolation is true then such method are not executed locally,
    but instead RPC call is made to Database API (aka Internal API). This makes some components
    decouple from direct Airflow database access.
    Each decorated method must be present in METHODS list in airflow.api_internal.endpoints.rpc_api_endpoint.
    Only static methods can be decorated. This decorator must be before "provide_session".

    See [AIP-44](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-44+Airflow+Internal+API)
    for more information .
    """

    from requests.exceptions import ConnectionError

    def _is_retryable_exception(exception: BaseException) -> bool:
    """
    Evaluates which exception types should be retried.
    
    This is especially demanded for cases where an application gateway or Kubernetes ingress can
    not find a running instance of a webserver hostring the API (HTTP 502+504) or when the
    HTTP request fails in general on network level.
    
    Note that we want to fail on other general errors on the webserver not to send bad requests in an endless loop.
    """
    
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(min=1),
        retry=tenacity.retry_if_exception(_is_retryable_exception),
        before_sleep=tenacity.before_log(logger, logging.WARNING),
    )
    def make_jsonrpc_request(method_name: str, params_json: str) -> bytes:
        signer = JWTSigner(
            secret_key=conf.get("core", "internal_api_secret_key"),
            expiration_time_in_seconds=conf.getint("core", "internal_api_clock_grace", fallback=30),
            audience="api",
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": method_name}),
        }
        data = {"jsonrpc": "2.0", "method": method_name, "params": params_json}
        internal_api_endpoint = InternalApiConfig.get_internal_api_endpoint()
        response = requests.post(url=internal_api_endpoint, data=json.dumps(data), headers=headers)
        if response.status_code != 200:
            raise AirflowHttpException(
                f"Got {response.status_code}:{response.reason} when sending "
                f"the internal api request: {response.text}",
                HTTPStatus(response.status_code),
            )
        return response.content

    @wraps(func)
    def wrapper(*args, **kwargs):
        use_internal_api = InternalApiConfig.get_use_internal_api()
        if not use_internal_api:
            return func(*args, **kwargs)
        import traceback

        tb = traceback.extract_stack()
        if any(filename.endswith("conftest.py") for filename, _, _, _ in tb):
            # This is a test fixture, we should not use internal API for it
            return func(*args, **kwargs)

        from airflow.serialization.serialized_objects import BaseSerialization  # avoid circular import

        bound = inspect.signature(func).bind(*args, **kwargs)
        arguments_dict = dict(bound.arguments)
        if "session" in arguments_dict:
            del arguments_dict["session"]
        if "cls" in arguments_dict:  # used by @classmethod
            del arguments_dict["cls"]

        args_dict = BaseSerialization.serialize(arguments_dict, use_pydantic_models=True)
        method_name = f"{func.__module__}.{func.__qualname__}"
        result = make_jsonrpc_request(method_name, args_dict)
        if result is None or result == b"":
            return None
        result = BaseSerialization.deserialize(json.loads(result), use_pydantic_models=True)
        if isinstance(result, (KeyError, AttributeError, AirflowException)):
            raise result
        return result

    return wrapper
