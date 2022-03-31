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

"""Handles CDP authentication"""

import json
from abc import ABC, abstractmethod
from typing import NamedTuple, Optional

import requests

from airflow.providers.cloudera.security import SecurityError, TokenResponse
from airflow.providers.cloudera.security.cdp_requests.cdpcurl import make_request
from airflow.utils.log.logging_mixin import LoggingMixin, logging  # type: ignore

LOG = logging.getLogger(__name__)


class CdpSecurityError(SecurityError):
    """Root exception for CDP authentication issues"""


class GetCrnError(CdpSecurityError):
    """Exception used when there is an issue while retrieving the environment CRN"""


class CdpApiAError(CdpSecurityError):
    """Exception used when there is an issue while interacting with CDP API"""


class GetWorkloadAuthTokenError(CdpSecurityError):
    """Exception used when there is an issue while retrieving the workload token"""


class CdpTokenAuthResponse(TokenResponse):
    """CDP Token Response object"""

    def __init__(self, response: requests.Response):
        response_dict = json.loads(response.content)
        self.token = response_dict.get("token")
        self.expires_at = response_dict.get("expiresAt")

    def is_valid(self) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{CdpTokenAuthResponse.__name__}" f"{{Token: {self.token}, Expires At: {self.expires_at}}}"


class CdpAuth(ABC, LoggingMixin):
    """Interface for CDP Authentication"""

    @abstractmethod
    def get_auth_identifier(self) -> str:
        """Gets the identifier of the connection

        Returns:
            identifier of the connection
        """
        raise NotImplementedError

    @abstractmethod
    def get_auth_secret(self) -> str:
        """Gets the secret of the connection

        Returns:
            secret of the connection
        """
        raise NotImplementedError

    @abstractmethod
    def generate_workload_auth_token(self, workload_name: str) -> CdpTokenAuthResponse:
        """Obtains a CDP access token.

        Args:
            workload_name: kind of workload for which we request the CDP token

        Returns:
            CDP JWT token Response

        Raises:
            CdpApiAError if it is not possible to retrieve the CDP token
        """
        raise NotImplementedError


class CdpAccessKeyCredentials(NamedTuple):
    """Represent access/private key pair for CDP Access Key V2 authentication"""

    access_key: str
    private_key: str


class CdpAccessKeyV2TokenAuth(CdpAuth):
    """Authentication class for obtaining CDP token from access key/private key credentials"""

    CDP_ENDPOINT_DEFAULT = "https://api.us-west-1.cdp.cloudera.com"
    CDP_DESCRIBE_SERVICE_ROUTE = "/api/v1/de/describeService"
    ALTUS_IAM_ENDPOINT_DEFAULT = "https://iamapi.us-west-1.altus.cloudera.com"
    ALTUS_IAM_GEN_WORKLOAD_AUTH_TOKEN_ROUTE = "/iam/generateWorkloadAuthToken"

    def __init__(
        self,
        service_id: str,
        cdp_cred: CdpAccessKeyCredentials,
        cdp_endpoint: Optional[str] = None,
        altus_iam_endpoint: Optional[str] = None,
    ) -> None:
        self.service_id = service_id
        self.cdp_cred = cdp_cred
        self.cdp_describe_service_endpoint = cdp_endpoint if cdp_endpoint else self.CDP_ENDPOINT_DEFAULT
        self.cdp_describe_service_endpoint += self.CDP_DESCRIBE_SERVICE_ROUTE
        self.altus_iam_gen_workload_auth_endpoint = (
            altus_iam_endpoint if altus_iam_endpoint else self.ALTUS_IAM_ENDPOINT_DEFAULT
        )
        self.altus_iam_gen_workload_auth_endpoint += self.ALTUS_IAM_GEN_WORKLOAD_AUTH_TOKEN_ROUTE

    def generate_workload_auth_token(self, workload_name: str) -> CdpTokenAuthResponse:
        LOG.debug("Authenticating with access key: %s", self.cdp_cred.access_key)
        LOG.debug("Using Cluster ID: %s", self.service_id)

        # Get the environment-crn
        env_crn = self.get_env_crn()
        LOG.debug("Using environment-crn %s", env_crn)

        # Exchange the access key for a CDP access token
        cdp_token = self._generate_workload_auth_token(env_crn, workload_name)
        LOG.debug("Exchanged access key for CDP access token")

        return cdp_token

    def get_env_crn(self) -> str:
        """
        Gets the associated environment CRN of the given cluster

        Returns:
            environment Cloudera Resource Name

        Raises:
            GetCrnError if it is not possible to retrieve the environment CRN
        """
        headers = {"Content-Type": "application/json"}
        # make_request only accepts a string for the request body
        request_body = f'{{"clusterId": "{self.service_id}"}}'
        try:
            response = make_request(
                "POST",
                self.cdp_describe_service_endpoint,
                headers,
                request_body,
                self.cdp_cred.access_key,
                self.cdp_cred.private_key,
                False,
                True,
            )
        except Exception as err:
            LOG.error("Issue while performing request to fetch environment-crn: %s", repr(err))
            raise GetCrnError(err) from err

        environment_crn = response.json().get("service").get("environmentCrn")
        return environment_crn

    def get_auth_identifier(self) -> str:
        return self.cdp_cred.access_key

    def get_auth_secret(self) -> str:
        return self.cdp_cred.private_key

    def _generate_workload_auth_token(self, env_crn: str, workload_name: str) -> CdpTokenAuthResponse:
        headers = {"Content-Type": "application/json"}
        # make_request only accepts a string for the request body
        request_body = f'{{"workloadName": "{workload_name}", "environmentCRN": "{env_crn}"}}'
        try:
            response = make_request(
                "POST",
                self.altus_iam_gen_workload_auth_endpoint,
                headers,
                request_body,
                self.cdp_cred.access_key,
                self.cdp_cred.private_key,
                False,
                True,
            )
        except Exception as err:
            LOG.error("Could not exchange cdp token with access key %s", repr(err))
            raise CdpApiAError(err) from err

        cdp_token = CdpTokenAuthResponse(response)
        return cdp_token
