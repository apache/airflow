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

import abc
import time
from functools import wraps
from typing import TYPE_CHECKING, Any

import requests
from google.auth.exceptions import RefreshError
from google.auth.identity_pool import SubjectTokenSupplier

if TYPE_CHECKING:
    from google.auth.external_account import SupplierContext
    from google.auth.transport import Request

from airflow.utils.log.logging_mixin import LoggingMixin


def cache_token_decorator(get_subject_token_method):
    """
    Cache calls to ``SubjectTokenSupplier`` instances' ``get_token_supplier`` methods.

    Different instances of a same SubjectTokenSupplier class with the same attributes
    share the OIDC token cache.

    :param get_subject_token_method: A method that returns both a token and an integer specifying
        the time in seconds until the token expires

    See also:
        https://googleapis.dev/python/google-auth/latest/reference/google.auth.identity_pool.html#google.auth.identity_pool.SubjectTokenSupplier.get_subject_token
    """
    cache = {}

    @wraps(get_subject_token_method)
    def wrapper(supplier_instance: CacheTokenSupplier, *args, **kwargs) -> str:
        """
        Obeys the interface set by ``SubjectTokenSupplier`` for ``get_subject_token`` methods.

        :param supplier_instance: the SubjectTokenSupplier instance whose get_subject_token method is being decorated
        :return: The token string
        """
        nonlocal cache

        cache_key = supplier_instance.get_subject_key()
        token: dict[str, str | float] = {}

        if cache_key not in cache or cache[cache_key]["expiration_time"] < time.monotonic():
            supplier_instance.log.info("OIDC token missing or expired")
            try:
                access_token, expires_in = get_subject_token_method(supplier_instance, *args, **kwargs)
                if not isinstance(expires_in, int) or not isinstance(access_token, str):
                    raise RefreshError  # assume error if strange values are provided

            except RefreshError:
                supplier_instance.log.error("Failed retrieving new OIDC Token from IdP")
                raise

            expiration_time = time.monotonic() + float(expires_in)
            token["access_token"] = access_token
            token["expiration_time"] = expiration_time
            cache[cache_key] = token

            supplier_instance.log.info("New OIDC token retrieved, expires in %s seconds.", expires_in)

        return cache[cache_key]["access_token"]

    return wrapper


class CacheTokenSupplier(LoggingMixin, SubjectTokenSupplier):
    """
    A superclass for all Subject Token Supplier classes that wish to implement a caching mechanism.

    Child classes must implement the ``get_subject_key`` method to generate a string that serves as the cache key,
    ensuring that tokens are shared appropriately among instances.

    Methods:
        get_subject_key: Abstract method to be implemented by child classes. It should return a string that serves as the cache key.
    """

    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def get_subject_key(self) -> str:
        raise NotImplementedError("")


class ClientCredentialsGrantFlowTokenSupplier(CacheTokenSupplier):
    """
    Class that retrieves an OIDC token from an external IdP using OAuth2.0 Client Credentials Grant flow.

    This class implements the ``SubjectTokenSupplier`` interface class used by ``google.auth.identity_pool.Credentials``

    :params oidc_issuer_url: URL of the IdP that performs OAuth2.0 Client Credentials Grant flow and returns an OIDC token.
    :params client_id: Client ID of the application requesting the token
    :params client_secret: Client secret of the application requesting the token
    :params extra_params_kwargs: Extra parameters to be passed in the payload of the POST request to the `oidc_issuer_url`

    See also:
        https://googleapis.dev/python/google-auth/latest/reference/google.auth.identity_pool.html#google.auth.identity_pool.SubjectTokenSupplier
    """

    def __init__(
        self,
        oidc_issuer_url: str,
        client_id: str,
        client_secret: str,
        **extra_params_kwargs: Any,
    ) -> None:
        super().__init__()
        self.oidc_issuer_url = oidc_issuer_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.extra_params_kwargs = extra_params_kwargs

    @cache_token_decorator
    def get_subject_token(self, context: SupplierContext, request: Request) -> tuple[str, int]:
        """Perform Client Credentials Grant flow with IdP and retrieves an OIDC token and expiration time."""
        self.log.info("Requesting new OIDC token from external IdP.")
        try:
            response = requests.post(
                self.oidc_issuer_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    **self.extra_params_kwargs,
                },
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RefreshError(str(e))
        except requests.ConnectionError as e:
            raise RefreshError(str(e))

        try:
            response_dict = response.json()
        except requests.JSONDecodeError:
            raise RefreshError(f"Didn't get a json response from {self.oidc_issuer_url}")

        # These fields are required
        if {"access_token", "expires_in"} - set(response_dict.keys()):
            # TODO more information about the error can be provided in the exception by inspecting the response
            raise RefreshError(f"No access token returned from {self.oidc_issuer_url}")

        return response_dict["access_token"], response_dict["expires_in"]

    def get_subject_key(self) -> str:
        """
        Create a cache key using the OIDC issuer URL, client ID, client secret and additional parameters.

        Instances with the same credentials will share tokens.
        """
        cache_key = (
            self.oidc_issuer_url
            + self.client_id
            + self.client_secret
            + ",".join(sorted(self.extra_params_kwargs))
        )
        return cache_key
