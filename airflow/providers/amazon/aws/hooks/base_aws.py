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
This module contains Base AWS Hook.

.. seealso::
    For more information on how to use this hook, take a look at the guide:
    :ref:`howto/connection:aws`
"""

from __future__ import annotations

import datetime
import inspect
import json
import logging
import os
from copy import deepcopy
from functools import cached_property, wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, Union

import boto3
import botocore
import botocore.session
import jinja2
import requests
import tenacity
from botocore.config import Config
from botocore.waiter import Waiter, WaiterModel
from dateutil.tz import tzlocal
from slugify import slugify

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowNotFoundException,
)
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from airflow.providers.amazon.aws.utils.identifiers import generate_uuid
from airflow.providers.amazon.aws.utils.suppress import return_on_error
from airflow.providers_manager import ProvidersManager
from airflow.utils.helpers import exactly_one
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret

BaseAwsConnection = TypeVar("BaseAwsConnection", bound=Union[boto3.client, boto3.resource])

if TYPE_CHECKING:
    from botocore.client import ClientMeta
    from botocore.credentials import ReadOnlyCredentials

    from airflow.models.connection import Connection  # Avoid circular imports.

_loader = botocore.loaders.Loader()
"""
botocore data loader to be used with async sessions

By default, a botocore session creates and caches an instance of JSONDecoder which
consumes a lot of memory.  This issue was reported here https://github.com/boto/botocore/issues/3078.
In the context of triggers which use boto sessions, this can result in excessive
memory usage and as a result reduced capacity on the triggerer.  We can reduce
memory footprint by sharing the loader instance across the sessions.

:meta private:
"""


class BaseSessionFactory(LoggingMixin):
    """
    Base AWS Session Factory class.

    This handles synchronous and async boto session creation. It can handle most
    of the AWS supported authentication methods.

    User can also derive from this class to have full control of boto3 session
    creation or to support custom federation.

    .. note::
        Not all features implemented for synchronous sessions are available
        for async sessions.

    .. seealso::
        - :ref:`howto/connection:aws:session-factory`
    """

    def __init__(
        self,
        conn: Connection | AwsConnectionWrapper | None,
        region_name: str | None = None,
        config: Config | None = None,
    ) -> None:
        super().__init__()
        self._conn = conn
        self._region_name = region_name
        self._config = config

    @cached_property
    def conn(self) -> AwsConnectionWrapper:
        """Cached AWS Connection Wrapper."""
        return AwsConnectionWrapper(
            conn=self._conn,
            region_name=self._region_name,
            botocore_config=self._config,
        )

    @cached_property
    def basic_session(self) -> boto3.session.Session:
        """Cached property with basic boto3.session.Session."""
        return self._create_basic_session(session_kwargs=self.conn.session_kwargs)

    @property
    def extra_config(self) -> dict[str, Any]:
        """AWS Connection extra_config."""
        return self.conn.extra_config

    @property
    def region_name(self) -> str | None:
        """AWS Region Name read-only property."""
        return self.conn.region_name

    @property
    def config(self) -> Config | None:
        """Configuration for botocore client read-only property."""
        return self.conn.botocore_config

    @property
    def role_arn(self) -> str | None:
        """Assume Role ARN from AWS Connection."""
        return self.conn.role_arn

    def _apply_session_kwargs(self, session):
        if self.conn.session_kwargs.get("profile_name", None) is not None:
            session.set_config_variable("profile", self.conn.session_kwargs["profile_name"])

        if (
            self.conn.session_kwargs.get("aws_access_key_id", None)
            or self.conn.session_kwargs.get("aws_secret_access_key", None)
            or self.conn.session_kwargs.get("aws_session_token", None)
        ):
            session.set_credentials(
                access_key=self.conn.session_kwargs.get("aws_access_key_id"),
                secret_key=self.conn.session_kwargs.get("aws_secret_access_key"),
                token=self.conn.session_kwargs.get("aws_session_token"),
            )

        if self.conn.session_kwargs.get("region_name", None) is not None:
            session.set_config_variable("region", self.conn.session_kwargs["region_name"])

    def get_async_session(self):
        from aiobotocore.session import get_session as async_get_session

        session = async_get_session()
        session.register_component("data_loader", _loader)
        return session

    def create_session(
        self, deferrable: bool = False
    ) -> boto3.session.Session | aiobotocore.session.AioSession:
        """Create boto3 or aiobotocore Session from connection config."""
        if not self.conn:
            self.log.info(
                "No connection ID provided. Fallback on boto3 credential strategy (region_name=%r). "
                "See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html",
                self.region_name,
            )
            if deferrable:
                session = self.get_async_session()
                self._apply_session_kwargs(session)
                return session
            else:
                return boto3.session.Session(region_name=self.region_name)
        elif not self.role_arn:
            if deferrable:
                session = self.get_async_session()
                self._apply_session_kwargs(session)
                return session
            else:
                return self.basic_session

        # Values stored in ``AwsConnectionWrapper.session_kwargs`` are intended to be used only
        # to create the initial boto3 session.
        # If the user wants to use the 'assume_role' mechanism then only the 'region_name' needs to be
        # provided, otherwise other parameters might conflict with the base botocore session.
        # Unfortunately it is not a part of public boto3 API, see source of boto3.session.Session:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/session.html#Session
        # If we provide 'aws_access_key_id' or 'aws_secret_access_key' or 'aws_session_token'
        # as part of session kwargs it will use them instead of assumed credentials.
        assume_session_kwargs = {}
        if self.conn.region_name:
            assume_session_kwargs["region_name"] = self.conn.region_name
        return self._create_session_with_assume_role(
            session_kwargs=assume_session_kwargs, deferrable=deferrable
        )

    def _create_basic_session(self, session_kwargs: dict[str, Any]) -> boto3.session.Session:
        return boto3.session.Session(**session_kwargs)

    def _create_session_with_assume_role(
        self, session_kwargs: dict[str, Any], deferrable: bool = False
    ) -> boto3.session.Session | aiobotocore.session.AioSession:
        if self.conn.assume_role_method == "assume_role_with_web_identity":
            # Deferred credentials have no initial credentials
            credential_fetcher = self._get_web_identity_credential_fetcher()

            params = {
                "method": "assume-role-with-web-identity",
                "refresh_using": credential_fetcher.fetch_credentials,
                "time_fetcher": lambda: datetime.datetime.now(tz=tzlocal()),
            }

            if deferrable:
                from aiobotocore.credentials import AioDeferredRefreshableCredentials

                credentials = AioDeferredRefreshableCredentials(**params)
            else:
                credentials = botocore.credentials.DeferredRefreshableCredentials(**params)
        else:
            # Refreshable credentials do have initial credentials
            params = {
                "metadata": self._refresh_credentials(),
                "refresh_using": self._refresh_credentials,
                "method": "sts-assume-role",
            }
            if deferrable:
                from aiobotocore.credentials import AioRefreshableCredentials

                credentials = AioRefreshableCredentials.create_from_metadata(**params)
            else:
                credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(**params)

        if deferrable:
            from aiobotocore.session import get_session as async_get_session

            session = async_get_session()
        else:
            session = botocore.session.get_session()

        session._credentials = credentials
        session.set_config_variable("region", self.basic_session.region_name)

        if not deferrable:
            return boto3.session.Session(botocore_session=session, **session_kwargs)

        return session

    def _refresh_credentials(self) -> dict[str, Any]:
        self.log.debug("Refreshing credentials")
        assume_role_method = self.conn.assume_role_method
        if assume_role_method not in ("assume_role", "assume_role_with_saml"):
            raise NotImplementedError(f"assume_role_method={assume_role_method} not expected")

        sts_client = self.basic_session.client(
            "sts",
            config=self.config,
            endpoint_url=self.conn.get_service_endpoint_url("sts", sts_connection_assume=True),
        )

        if assume_role_method == "assume_role":
            sts_response = self._assume_role(sts_client=sts_client)
        else:
            sts_response = self._assume_role_with_saml(sts_client=sts_client)

        sts_response_http_status = sts_response["ResponseMetadata"]["HTTPStatusCode"]
        if sts_response_http_status != 200:
            raise RuntimeError(f"sts_response_http_status={sts_response_http_status}")

        credentials = sts_response["Credentials"]
        expiry_time = credentials.get("Expiration").isoformat()
        self.log.debug("New credentials expiry_time: %s", expiry_time)
        credentials = {
            "access_key": credentials.get("AccessKeyId"),
            "secret_key": credentials.get("SecretAccessKey"),
            "token": credentials.get("SessionToken"),
            "expiry_time": expiry_time,
        }
        return credentials

    def _assume_role(self, sts_client: boto3.client) -> dict:
        kw = {
            "RoleSessionName": self._strip_invalid_session_name_characters(f"Airflow_{self.conn.conn_id}"),
            **self.conn.assume_role_kwargs,
            "RoleArn": self.role_arn,
        }
        return sts_client.assume_role(**kw)

    def _assume_role_with_saml(self, sts_client: boto3.client) -> dict[str, Any]:
        saml_config = self.extra_config["assume_role_with_saml"]
        principal_arn = saml_config["principal_arn"]

        idp_auth_method = saml_config["idp_auth_method"]
        if idp_auth_method == "http_spegno_auth":
            saml_assertion = self._fetch_saml_assertion_using_http_spegno_auth(saml_config)
        else:
            raise NotImplementedError(
                f"idp_auth_method={idp_auth_method} in Connection {self.conn.conn_id} Extra."
                'Currently only "http_spegno_auth" is supported, and must be specified.'
            )

        self.log.debug("Doing sts_client.assume_role_with_saml to role_arn=%s", self.role_arn)
        return sts_client.assume_role_with_saml(
            RoleArn=self.role_arn,
            PrincipalArn=principal_arn,
            SAMLAssertion=saml_assertion,
            **self.conn.assume_role_kwargs,
        )

    def _get_idp_response(
        self, saml_config: dict[str, Any], auth: requests.auth.AuthBase
    ) -> requests.models.Response:
        idp_url = saml_config["idp_url"]
        self.log.debug("idp_url= %s", idp_url)

        session = requests.Session()

        # Configurable Retry when querying the IDP endpoint
        if "idp_request_retry_kwargs" in saml_config:
            idp_request_retry_kwargs = saml_config["idp_request_retry_kwargs"]
            self.log.info("idp_request_retry_kwargs= %s", idp_request_retry_kwargs)
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            retry_strategy = Retry(**idp_request_retry_kwargs)
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)

        idp_request_kwargs = {}
        if "idp_request_kwargs" in saml_config:
            idp_request_kwargs = saml_config["idp_request_kwargs"]

        idp_response = session.get(idp_url, auth=auth, **idp_request_kwargs)
        idp_response.raise_for_status()

        return idp_response

    def _fetch_saml_assertion_using_http_spegno_auth(self, saml_config: dict[str, Any]) -> str:
        # requests_gssapi will need paramiko > 2.6 since you'll need
        # 'gssapi' not 'python-gssapi' from PyPi.
        # https://github.com/paramiko/paramiko/pull/1311
        import requests_gssapi
        from lxml import etree

        auth = requests_gssapi.HTTPSPNEGOAuth()
        if "mutual_authentication" in saml_config:
            mutual_auth = saml_config["mutual_authentication"]
            if mutual_auth == "REQUIRED":
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.REQUIRED)
            elif mutual_auth == "OPTIONAL":
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.OPTIONAL)
            elif mutual_auth == "DISABLED":
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.DISABLED)
            else:
                raise NotImplementedError(
                    f"mutual_authentication={mutual_auth} in Connection {self.conn.conn_id} Extra."
                    'Currently "REQUIRED", "OPTIONAL" and "DISABLED" are supported.'
                    "(Exclude this setting will default to HTTPSPNEGOAuth() )."
                )
        # Query the IDP
        idp_response = self._get_idp_response(saml_config, auth=auth)
        # Assist with debugging. Note: contains sensitive info!
        xpath = saml_config["saml_response_xpath"]
        log_idp_response = "log_idp_response" in saml_config and saml_config["log_idp_response"]
        if log_idp_response:
            self.log.warning(
                "The IDP response contains sensitive information, but log_idp_response is ON (%s).",
                log_idp_response,
            )
            self.log.debug("idp_response.content= %s", idp_response.content)
            self.log.debug("xpath= %s", xpath)
        # Extract SAML Assertion from the returned HTML / XML
        xml = etree.fromstring(idp_response.content)
        saml_assertion = xml.xpath(xpath)
        if isinstance(saml_assertion, list):
            if len(saml_assertion) == 1:
                saml_assertion = saml_assertion[0]
        if not saml_assertion:
            raise ValueError("Invalid SAML Assertion")
        return saml_assertion

    def _get_web_identity_credential_fetcher(
        self,
    ) -> botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher:
        base_session = self.basic_session._session or botocore.session.get_session()
        client_creator = base_session.create_client
        federation = str(self.extra_config.get("assume_role_with_web_identity_federation"))

        web_identity_token_loader = {
            "file": self._get_file_token_loader,
            "google": self._get_google_identity_token_loader,
        }.get(federation)

        if not web_identity_token_loader:
            raise AirflowException(f"Unsupported federation: {federation}.")

        return botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher(
            client_creator=client_creator,
            web_identity_token_loader=web_identity_token_loader(),
            role_arn=self.role_arn,
            extra_args=self.conn.assume_role_kwargs,
        )

    def _get_file_token_loader(self):
        from botocore.credentials import FileWebIdentityTokenLoader

        token_file = self.extra_config.get("assume_role_with_web_identity_token_file") or os.getenv(
            "AWS_WEB_IDENTITY_TOKEN_FILE"
        )

        return FileWebIdentityTokenLoader(token_file)

    def _get_google_identity_token_loader(self):
        from google.auth.transport import requests as requests_transport

        from airflow.providers.google.common.utils.id_token_credentials import (
            get_default_id_token_credentials,
        )

        audience = self.extra_config.get("assume_role_with_web_identity_federation_audience")

        google_id_token_credentials = get_default_id_token_credentials(target_audience=audience)

        def web_identity_token_loader():
            if not google_id_token_credentials.valid:
                request_adapter = requests_transport.Request()
                google_id_token_credentials.refresh(request=request_adapter)
            return google_id_token_credentials.token

        return web_identity_token_loader

    def _strip_invalid_session_name_characters(self, role_session_name: str) -> str:
        return slugify(role_session_name, regex_pattern=r"[^\w+=,.@-]+")


class AwsGenericHook(BaseHook, Generic[BaseAwsConnection]):
    """
    Generic class for interact with AWS.

    This class provide a thin wrapper around the boto3 Python library.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: Reference to :external:py:meth:`boto3.client service_name \
        <boto3.session.Session.client>`, e.g. 'emr', 'batch', 's3', etc.
        Mutually exclusive with ``resource_type``.
    :param resource_type: Reference to :external:py:meth:`boto3.resource service_name \
        <boto3.session.Session.resource>`, e.g. 's3', 'ec2', 'dynamodb', etc.
        Mutually exclusive with ``client_type``.
    :param config: Configuration for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    conn_name_attr = "aws_conn_id"
    default_conn_name = "aws_default"
    conn_type = "aws"
    hook_name = "Amazon Web Services"

    def __init__(
        self,
        aws_conn_id: str | None = default_conn_name,
        verify: bool | str | None = None,
        region_name: str | None = None,
        client_type: str | None = None,
        resource_type: str | None = None,
        config: Config | dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.client_type = client_type
        self.resource_type = resource_type

        self._region_name = region_name
        if isinstance(config, dict):
            config = Config(**config)
        self._config = config
        self._verify = verify

    @classmethod
    @return_on_error("Unknown")
    def _get_provider_version(cls) -> str:
        """Check the Providers Manager for the package version."""
        manager = ProvidersManager()
        hook = manager.hooks[cls.conn_type]
        if not hook:
            # This gets caught immediately, but without it MyPy complains
            # Item "None" of "Optional[HookInfo]" has no attribute "package_name"
            # on the following line and static checks fail.
            raise ValueError(f"Hook info for {cls.conn_type} not found in the Provider Manager.")
        return manager.providers[hook.package_name].version

    @staticmethod
    def _find_operator_class_name(target_function_name: str) -> str | None:
        """
        Given a frame off the stack, return the name of the class that made the call.

        This method may raise a ValueError or an IndexError. The caller is
        responsible with catching and handling those.
        """
        stack = inspect.stack()
        # Find the index of the most recent frame which called the provided function name
        # and pull that frame off the stack.
        target_frames = [frame for frame in stack if frame.function == target_function_name]
        if target_frames:
            target_frame = target_frames[0][0]
        else:
            return None
        # Get the local variables for that frame.
        frame_variables = target_frame.f_locals["self"]
        # Get the class object for that frame.
        frame_class_object = frame_variables.__class__
        # Return the name of the class object.
        return frame_class_object.__name__

    @staticmethod
    def _find_executor_class_name() -> str | None:
        """Inspect the call stack looking for any executor classes and returning the first found."""
        stack = inspect.stack()
        # Fetch class objects on all frames, looking for one containing an executor (since it
        # will inherit from BaseExecutor)
        for frame in stack:
            classes = []
            for name, obj in frame[0].f_globals.items():
                if inspect.isclass(obj):
                    classes.append(name)
            if "BaseExecutor" in classes:
                return classes[-1]
        return None

    @return_on_error("Unknown")
    def _get_caller(self, target_function_name: str = "execute") -> str:
        """Try to determine the caller of this hook. Whether that be an AWS Operator, Sensor or Executor."""
        caller = self._find_operator_class_name(target_function_name)
        if caller == "BaseSensorOperator":
            # If the result is a BaseSensorOperator, then look for whatever last called "poke".
            caller = self._find_operator_class_name("poke")
        if not caller:
            # Check if we can find an executor
            caller = self._find_executor_class_name()
        return caller if caller else "Unknown"

    @staticmethod
    @return_on_error("00000000-0000-0000-0000-000000000000")
    def _generate_dag_key() -> str:
        """
        Generate a DAG key.

        The Object Identifier (OID) namespace is used to salt the dag_id value.
        That salted value is used to generate a SHA-1 hash which, by definition,
        can not (reasonably) be reversed.  No personal data can be inferred or
        extracted from the resulting UUID.
        """
        return generate_uuid(os.environ.get("AIRFLOW_CTX_DAG_ID"))

    @staticmethod
    @return_on_error("Unknown")
    def _get_airflow_version() -> str:
        """Fetch and return the current Airflow version."""
        # This can be a circular import under specific configurations.
        # Importing locally to either avoid or catch it if it does happen.
        from airflow import __version__ as airflow_version

        return airflow_version

    def _generate_user_agent_extra_field(self, existing_user_agent_extra: str) -> str:
        user_agent_extra_values = [
            f"Airflow/{self._get_airflow_version()}",
            f"AmPP/{self._get_provider_version()}",
            f"Caller/{self._get_caller()}",
            f"DagRunKey/{self._generate_dag_key()}",
            existing_user_agent_extra or "",
        ]
        return " ".join(user_agent_extra_values).strip()

    @cached_property
    def conn_config(self) -> AwsConnectionWrapper:
        """Get the Airflow Connection object and wrap it in helper (cached)."""
        connection = None
        if self.aws_conn_id:
            try:
                connection = self.get_connection(self.aws_conn_id)
            except AirflowNotFoundException:
                self.log.warning(
                    "Unable to find AWS Connection ID '%s', switching to empty.", self.aws_conn_id
                )

        return AwsConnectionWrapper(
            conn=connection, region_name=self._region_name, botocore_config=self._config, verify=self._verify
        )

    def _resolve_service_name(self, is_resource_type: bool = False) -> str:
        """Resolve service name based on type or raise an error."""
        if exactly_one(self.client_type, self.resource_type):
            # It is possible to write simple conditions, however it make mypy unhappy.
            if self.client_type:
                if is_resource_type:
                    raise LookupError("Requested `resource_type`, but `client_type` was set instead.")
                return self.client_type
            elif self.resource_type:
                if not is_resource_type:
                    raise LookupError("Requested `client_type`, but `resource_type` was set instead.")
                return self.resource_type

        raise ValueError(
            f"Either client_type={self.client_type!r} or "
            f"resource_type={self.resource_type!r} must be provided, not both."
        )

    @property
    def service_name(self) -> str:
        """Extracted botocore/boto3 service name from hook parameters."""
        return self._resolve_service_name(is_resource_type=bool(self.resource_type))

    @property
    def service_config(self) -> dict:
        """Config for hook-specific service from AWS Connection."""
        return self.conn_config.get_service_config(service_name=self.service_name)

    @property
    def region_name(self) -> str | None:
        """AWS Region Name read-only property."""
        return self.conn_config.region_name

    @property
    def config(self) -> Config:
        """Configuration for botocore client read-only property."""
        return self.conn_config.botocore_config or botocore.config.Config()

    @property
    def verify(self) -> bool | str | None:
        """Verify or not SSL certificates boto3 client/resource read-only property."""
        return self.conn_config.verify

    @cached_property
    def account_id(self) -> str:
        """Return associated AWS Account ID."""
        return (
            self.get_session(region_name=self.region_name)
            .client(
                service_name="sts",
                endpoint_url=self.conn_config.get_service_endpoint_url("sts"),
                config=self.config,
                verify=self.verify,
            )
            .get_caller_identity()["Account"]
        )

    def get_session(self, region_name: str | None = None, deferrable: bool = False) -> boto3.session.Session:
        """Get the underlying boto3.session.Session(region_name=region_name)."""
        return SessionFactory(
            conn=self.conn_config, region_name=region_name, config=self.config
        ).create_session(deferrable=deferrable)

    def _get_config(self, config: Config | None = None) -> Config:
        """
        No AWS Operators use the config argument to this method.

        Keep backward compatibility with other users who might use it.
        """
        if config is None:
            config = deepcopy(self.config)

        # ignore[union-attr] is required for this block to appease MyPy
        # because the user_agent_extra field is generated at runtime.
        user_agent_config = Config(
            user_agent_extra=self._generate_user_agent_extra_field(
                existing_user_agent_extra=config.user_agent_extra  # type: ignore[union-attr]
            )
        )
        return config.merge(user_agent_config)  # type: ignore[union-attr]

    def get_client_type(
        self,
        region_name: str | None = None,
        config: Config | None = None,
        deferrable: bool = False,
    ) -> boto3.client:
        """Get the underlying boto3 client using boto3 session."""
        service_name = self._resolve_service_name(is_resource_type=False)
        session = self.get_session(region_name=region_name, deferrable=deferrable)
        endpoint_url = self.conn_config.get_service_endpoint_url(service_name=service_name)
        if not isinstance(session, boto3.session.Session):
            return session.create_client(
                service_name=service_name,
                endpoint_url=endpoint_url,
                config=self._get_config(config),
                verify=self.verify,
            )

        return session.client(
            service_name=service_name,
            endpoint_url=endpoint_url,
            config=self._get_config(config),
            verify=self.verify,
        )

    def get_resource_type(
        self,
        region_name: str | None = None,
        config: Config | None = None,
    ) -> boto3.resource:
        """Get the underlying boto3 resource using boto3 session."""
        service_name = self._resolve_service_name(is_resource_type=True)
        session = self.get_session(region_name=region_name)
        return session.resource(
            service_name=service_name,
            endpoint_url=self.conn_config.get_service_endpoint_url(service_name=service_name),
            config=self._get_config(config),
            verify=self.verify,
        )

    @cached_property
    def conn(self) -> BaseAwsConnection:
        """
        Get the underlying boto3 client/resource (cached).

        :return: boto3.client or boto3.resource
        """
        if self.client_type:
            return self.get_client_type(region_name=self.region_name)
        return self.get_resource_type(region_name=self.region_name)

    @property
    def async_conn(self):
        """Get an aiobotocore client to use for async operations."""
        if not self.client_type:
            raise ValueError("client_type must be specified.")

        return self.get_client_type(region_name=self.region_name, deferrable=True)

    @cached_property
    def _client(self) -> botocore.client.BaseClient:
        conn = self.conn
        if isinstance(conn, botocore.client.BaseClient):
            return conn
        return conn.meta.client

    @property
    def conn_client_meta(self) -> ClientMeta:
        """Get botocore client metadata from Hook connection (cached)."""
        return self._client.meta

    @property
    def conn_region_name(self) -> str:
        """Get actual AWS Region Name from Hook connection (cached)."""
        return self.conn_client_meta.region_name

    @property
    def conn_partition(self) -> str:
        """Get associated AWS Region Partition from Hook connection (cached)."""
        return self.conn_client_meta.partition

    def get_conn(self) -> BaseAwsConnection:
        """
        Get the underlying boto3 client/resource (cached).

        Implemented so that caching works as intended. It exists for compatibility
        with subclasses that rely on a super().get_conn() method.

        :return: boto3.client or boto3.resource
        """
        # Compat shim
        return self.conn

    def get_credentials(self, region_name: str | None = None) -> ReadOnlyCredentials:
        """
        Get the underlying `botocore.Credentials` object.

        This contains the following authentication attributes: access_key, secret_key and token.
        By use this method also secret_key and token will mask in tasks logs.
        """
        # Credentials are refreshable, so accessing your access key and
        # secret key separately can lead to a race condition.
        # See https://stackoverflow.com/a/36291428/8283373
        creds = self.get_session(region_name=region_name).get_credentials().get_frozen_credentials()
        mask_secret(creds.secret_key)
        if creds.token:
            mask_secret(creds.token)
        return creds

    def expand_role(self, role: str, region_name: str | None = None) -> str:
        """
        Get the Amazon Resource Name (ARN) for the role.

        If IAM role is already an IAM role ARN, the value is returned unchanged.

        :param role: IAM role name or ARN
        :param region_name: Optional region name to get credentials for
        :return: IAM role ARN
        """
        if "/" in role:
            return role
        else:
            session = self.get_session(region_name=region_name)
            _client = session.client(
                service_name="iam",
                endpoint_url=self.conn_config.get_service_endpoint_url("iam"),
                config=self.config,
                verify=self.verify,
            )
            return _client.get_role(RoleName=role)["Role"]["Arn"]

    @staticmethod
    def retry(should_retry: Callable[[Exception], bool]):
        """Repeat requests in response to exceeding a temporary quote limit."""

        def retry_decorator(fun: Callable):
            @wraps(fun)
            def decorator_f(self, *args, **kwargs):
                retry_args = getattr(self, "retry_args", None)
                if retry_args is None:
                    return fun(self, *args, **kwargs)
                multiplier = retry_args.get("multiplier", 1)
                min_limit = retry_args.get("min", 1)
                max_limit = retry_args.get("max", 1)
                stop_after_delay = retry_args.get("stop_after_delay", 10)
                tenacity_before_logger = tenacity.before_log(self.log, logging.INFO) if self.log else None
                tenacity_after_logger = tenacity.after_log(self.log, logging.INFO) if self.log else None
                default_kwargs = {
                    "wait": tenacity.wait_exponential(multiplier=multiplier, max=max_limit, min=min_limit),
                    "retry": tenacity.retry_if_exception(should_retry),
                    "stop": tenacity.stop_after_delay(stop_after_delay),
                    "before": tenacity_before_logger,
                    "after": tenacity_after_logger,
                }
                return tenacity.retry(**default_kwargs)(fun)(self, *args, **kwargs)

            return decorator_f

        return retry_decorator

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for AWS Connection."""
        return {
            "hidden_fields": ["host", "schema", "port"],
            "relabeling": {
                "login": "AWS Access Key ID",
                "password": "AWS Secret Access Key",
            },
            "placeholders": {
                "login": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "extra": json.dumps(
                    {
                        "region_name": "us-east-1",
                        "session_kwargs": {"profile_name": "default"},
                        "config_kwargs": {"retries": {"mode": "standard", "max_attempts": 10}},
                        "role_arn": "arn:aws:iam::123456789098:role/role-name",
                        "assume_role_method": "assume_role",
                        "assume_role_kwargs": {"RoleSessionName": "airflow"},
                        "aws_session_token": "AQoDYXdzEJr...EXAMPLETOKEN",
                        "endpoint_url": "http://localhost:4566",
                    },
                    indent=2,
                ),
            },
        }

    def test_connection(self):
        """
        Test the AWS connection by call AWS STS (Security Token Service) GetCallerIdentity API.

        .. seealso::
            https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html
        """
        try:
            session = self.get_session()
            conn_info = session.client(
                service_name="sts",
                endpoint_url=self.conn_config.get_service_endpoint_url("sts", sts_test_connection=True),
            ).get_caller_identity()
            metadata = conn_info.pop("ResponseMetadata", {})
            if metadata.get("HTTPStatusCode") != 200:
                try:
                    return False, json.dumps(metadata)
                except TypeError:
                    return False, str(metadata)
            conn_info["credentials_method"] = session.get_credentials().method
            conn_info["region_name"] = session.region_name
            return True, ", ".join(f"{k}={v!r}" for k, v in conn_info.items())

        except Exception as e:
            return False, f"{type(e).__name__!r} error occurred while testing connection: {e}"

    @cached_property
    def waiter_path(self) -> os.PathLike[str] | None:
        filename = self.client_type if self.client_type else self.resource_type
        path = Path(__file__).parents[1].joinpath(f"waiters/{filename}.json").resolve()
        return path if path.exists() else None

    def get_waiter(
        self,
        waiter_name: str,
        parameters: dict[str, str] | None = None,
        deferrable: bool = False,
        client=None,
    ) -> Waiter:
        """
        Get a waiter by name.

        First checks if there is a custom waiter with the provided waiter_name and
        uses that if it exists, otherwise it will check the service client for a
        waiter that matches the name and pass that through.

        If `deferrable` is True, the waiter will be an AIOWaiter, generated from the
        client that is passed as a parameter. If `deferrable` is True, `client` must be
        provided.

        :param waiter_name: The name of the waiter.  The name should exactly match the
            name of the key in the waiter model file (typically this is CamelCase).
        :param parameters: will scan the waiter config for the keys of that dict,
            and replace them with the corresponding value. If a custom waiter has
            such keys to be expanded, they need to be provided here.
        :param deferrable: If True, the waiter is going to be an async custom waiter.
            An async client must be provided in that case.
        :param client: The client to use for the waiter's operations
        """
        from airflow.providers.amazon.aws.waiters.base_waiter import BaseBotoWaiter

        if deferrable and not client:
            raise ValueError("client must be provided for a deferrable waiter.")
        # Currently, the custom waiter doesn't work with resource_type, only client_type is supported.
        client = client or self._client
        if self.waiter_path and (waiter_name in self._list_custom_waiters()):
            # Technically if waiter_name is in custom_waiters then self.waiter_path must
            # exist but MyPy doesn't like the fact that self.waiter_path could be None.
            with open(self.waiter_path) as config_file:
                config = json.loads(config_file.read())

            config = self._apply_parameters_value(config, waiter_name, parameters)
            return BaseBotoWaiter(client=client, model_config=config, deferrable=deferrable).waiter(
                waiter_name
            )
        # If there is no custom waiter found for the provided name,
        # then try checking the service's official waiters.
        return client.get_waiter(waiter_name)

    @staticmethod
    def _apply_parameters_value(config: dict, waiter_name: str, parameters: dict[str, str] | None) -> dict:
        """Replace potential jinja templates in acceptors definition."""
        # only process the waiter we're going to use to not raise errors for missing params for other waiters.
        acceptors = config["waiters"][waiter_name]["acceptors"]
        for a in acceptors:
            arg = a["argument"]
            template = jinja2.Template(arg, autoescape=False, undefined=jinja2.StrictUndefined)
            try:
                a["argument"] = template.render(parameters or {})
            except jinja2.UndefinedError as e:
                raise AirflowException(
                    f"Parameter was not supplied for templated waiter's acceptor '{arg}'", e
                )
        return config

    def list_waiters(self) -> list[str]:
        """Return a list containing the names of all waiters for the service, official and custom."""
        return [*self._list_official_waiters(), *self._list_custom_waiters()]

    def _list_official_waiters(self) -> list[str]:
        return self._client.waiter_names

    def _list_custom_waiters(self) -> list[str]:
        if not self.waiter_path:
            return []
        with open(self.waiter_path) as config_file:
            model_config = json.load(config_file)
            return WaiterModel(model_config).waiter_names


class AwsBaseHook(AwsGenericHook[Union[boto3.client, boto3.resource]]):
    """
    Base class for interact with AWS.

    This class provide a thin wrapper around the boto3 Python library.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: Reference to :external:py:meth:`boto3.client service_name \
        <boto3.session.Session.client>`, e.g. 'emr', 'batch', 's3', etc.
        Mutually exclusive with ``resource_type``.
    :param resource_type: Reference to :external:py:meth:`boto3.resource service_name \
        <boto3.session.Session.resource>`, e.g. 's3', 'ec2', 'dynamodb', etc.
        Mutually exclusive with ``client_type``.
    :param config: Configuration for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """


def resolve_session_factory() -> type[BaseSessionFactory]:
    """Resolve custom SessionFactory class."""
    clazz = conf.getimport("aws", "session_factory", fallback=None)
    if not clazz:
        return BaseSessionFactory
    if not issubclass(clazz, BaseSessionFactory):
        raise TypeError(
            f"Your custom AWS SessionFactory class `{clazz.__name__}` is not a subclass "
            f"of `{BaseSessionFactory.__name__}`."
        )
    return clazz


SessionFactory = resolve_session_factory()


def _parse_s3_config(config_file_name: str, config_format: str | None = "boto", profile: str | None = None):
    """For compatibility with airflow.contrib.hooks.aws_hook."""
    from airflow.providers.amazon.aws.utils.connection_wrapper import _parse_s3_config

    return _parse_s3_config(
        config_file_name=config_file_name,
        config_format=config_format,
        profile=profile,
    )


try:
    import aiobotocore.credentials
    from aiobotocore.session import AioSession, get_session
except ImportError:
    pass
