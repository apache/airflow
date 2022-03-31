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

"""Handles CDE authentication"""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Optional, Union
from urllib.parse import urlparse

import requests

from airflow.providers.cloudera.model.cdp.cde import VirtualCluster
from airflow.providers.cloudera.security import TokenResponse, submit_request
from airflow.providers.cloudera.security.cdp_security import CdpAuth, CdpSecurityError
from airflow.providers.cloudera.security.token_cache import (
    Cache,
    CacheableTokenAuth,
    GetAuthTokenError,
    TokenCacheStrategy,
)
from airflow.utils.log.logging_mixin import LoggingMixin, logging  # type: ignore

LOG = logging.getLogger(__name__)


class BearerAuth(requests.auth.AuthBase):
    """Helper class for defining the Bearer token bases authentication mechanism."""

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:  # pragma: no cover
        # ( since it is executed in  the requests.get call)
        r.headers["authorization"] = f"Bearer {self.token}"
        return r


class CdeTokenAuthResponse(TokenResponse, LoggingMixin):
    """CDE Token Response object"""

    def __init__(self, access_token: str, expires_in: int):
        self.access_token = access_token
        self.expires_in = expires_in

    @classmethod
    def from_response(cls, response: requests.Response) -> "CdeTokenAuthResponse":
        """Factory method for creating a new instance of CdeTokenAuthResponse
        from a json formatted valid request response

        Args:
            response: Response obtained from the Knox endpoint

        Returns:
            New, corresponding instance of CdeTokenAuthResponse
        """
        response_json = response.json()
        return cls(response_json.get("access_token"), response_json.get("expires_in"))

    def is_valid(self) -> bool:
        current_time = datetime.utcnow()
        token_time = datetime.utcfromtimestamp(self.expires_in / 1000)
        # Making the token invalid earlier to avoid edge cases when current time is too close
        # from the token expiry time (it would cause that the token would become invalid by the
        # time the CDEHook would use it)
        max_valid_time = token_time - timedelta(minutes=5)

        LOG.debug(
            "Current time is : %s. Token expires at: %s. Token must be renewed from: %s",
            current_time,
            token_time,
            max_valid_time,
        )

        return current_time <= max_valid_time

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CdeTokenAuthResponse):
            return False
        # Following line for autocompletion
        other_response: CdeTokenAuthResponse = other
        return (
            self.access_token == other_response.access_token and self.expires_in == other_response.expires_in
        )

    def __repr__(self) -> str:
        return (
            f"{CdeTokenAuthResponse.__name__}"
            f"{{Token: {self.access_token}, Expires In: {self.expires_in}}}"
        )


class CdeAuth(ABC):
    """Interface for CDE Authentication"""

    @abstractmethod
    def get_cde_authentication_token(self) -> CdeTokenAuthResponse:
        """Obtains a CDE access token.

        Returns:
            CDE JWT token Response

        Raises:
            GetAuthTokenError if it is not possible to retrieve the CDE token
        """
        raise NotImplementedError


class CdeApiTokenAuth(CdeAuth, CacheableTokenAuth):
    """Authentication class for obtaining CDE token from CDP API token"""

    def __init__(
        self,
        cde_vcluster: VirtualCluster,
        cdp_auth: CdpAuth,
        token_cache_strategy: Optional[TokenCacheStrategy] = None,
        custom_ca_certificate_path: Optional[str] = None,
        insecure: Optional[bool] = False,
    ) -> None:
        self.cde_vcluster = cde_vcluster
        self.cdp_auth = cdp_auth
        if token_cache_strategy:
            super().__init__(token_cache_strategy)
        self.custom_ca_certificate_path = custom_ca_certificate_path
        self.insecure = insecure

    @Cache(token_response_type=CdeTokenAuthResponse)
    def get_cde_authentication_token(self) -> CdeTokenAuthResponse:
        return self.fetch_authentication_token()

    def fetch_authentication_token(self) -> CdeTokenAuthResponse:
        """Obtains a fresh token directly from the target system

        Returns:
            valid fresh token

        Raises:
            GetAuthTokenError if there was an issue while retrieving the token
        """
        try:
            workload_name = "DE"
            cdp_token = self.cdp_auth.generate_workload_auth_token(workload_name)
        except CdpSecurityError as err:
            LOG.error("Could not obtain CDP token %s", err)
            raise GetAuthTokenError(err) from err

        # Exchange the CDP access token for a CDE/CDW access token
        try:
            auth_endpoint = self.cde_vcluster.get_auth_endpoint()
        except ValueError as err:
            LOG.error("Could not determine authentication endpoint: %s", err)
            raise GetAuthTokenError(err) from err

        try:
            kw_args: Dict[str, Union[str, bool, BearerAuth]] = {
                "auth": BearerAuth(cdp_token.token),
            }

            if self.insecure:
                kw_args = {**kw_args, "verify": False}
            elif self.custom_ca_certificate_path is not None:
                kw_args = {**kw_args, "verify": self.custom_ca_certificate_path}

            response = submit_request("GET", auth_endpoint, **kw_args)
        except Exception as err:
            LOG.error("Could not execute auth request: %s", repr(err))
            raise GetAuthTokenError(err) from err

        cde_token = CdeTokenAuthResponse.from_response(response)

        LOG.info(
            "Acquired CDE token expiring at %s",
            datetime.fromtimestamp(cde_token.expires_in / 1000),
        )

        return cde_token

    def get_cache_key(self) -> str:
        vcluster_url = urlparse(self.cde_vcluster.vcluster_endpoint)
        vcluster_host = vcluster_url.netloc
        return f"{self.cdp_auth.get_auth_identifier()}____{vcluster_host}"
