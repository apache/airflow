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
    :ref:`howto/connection:AWSHook`
"""
from __future__ import annotations

import datetime
import json
import logging
import warnings
from functools import wraps
from typing import Any, Callable, Generic, TypeVar, Union

import boto3
import botocore
import botocore.session
import requests
import tenacity
from botocore.client import ClientMeta
from botocore.config import Config
from botocore.credentials import ReadOnlyCredentials
from dateutil.tz import tzlocal
from slugify import slugify

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret

BaseAwsConnection = TypeVar("BaseAwsConnection", bound=Union[boto3.client, boto3.resource])


class BaseSessionFactory(LoggingMixin):
    """
    Base AWS Session Factory class to handle boto3 session creation.
    It can handle most of the AWS supported authentication methods.

    User can also derive from this class to have full control of boto3 session
    creation or to support custom federation.

    .. seealso::
        :ref:`howto/connection:aws:session-factory`
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
        """Assume Role ARN from AWS Connection"""
        return self.conn.role_arn

    def create_session(self) -> boto3.session.Session:
        """Create boto3 Session from connection config."""
        if not self.conn:
            self.log.info(
                "No connection ID provided. Fallback on boto3 credential strategy (region_name=%r). "
                "See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html",
                self.region_name,
            )
            return boto3.session.Session(region_name=self.region_name)
        elif not self.role_arn:
            return self.basic_session
        return self._create_session_with_assume_role(session_kwargs=self.conn.session_kwargs)

    def _create_basic_session(self, session_kwargs: dict[str, Any]) -> boto3.session.Session:
        return boto3.session.Session(**session_kwargs)

    def _create_session_with_assume_role(self, session_kwargs: dict[str, Any]) -> boto3.session.Session:
        if self.conn.assume_role_method == 'assume_role_with_web_identity':
            # Deferred credentials have no initial credentials
            credential_fetcher = self._get_web_identity_credential_fetcher()
            credentials = botocore.credentials.DeferredRefreshableCredentials(
                method='assume-role-with-web-identity',
                refresh_using=credential_fetcher.fetch_credentials,
                time_fetcher=lambda: datetime.datetime.now(tz=tzlocal()),
            )
        else:
            # Refreshable credentials do have initial credentials
            credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
                metadata=self._refresh_credentials(),
                refresh_using=self._refresh_credentials,
                method="sts-assume-role",
            )

        session = botocore.session.get_session()
        session._credentials = credentials
        region_name = self.basic_session.region_name
        session.set_config_variable("region", region_name)

        return boto3.session.Session(botocore_session=session, **session_kwargs)

    def _refresh_credentials(self) -> dict[str, Any]:
        self.log.debug('Refreshing credentials')
        assume_role_method = self.conn.assume_role_method
        if assume_role_method not in ('assume_role', 'assume_role_with_saml'):
            raise NotImplementedError(f'assume_role_method={assume_role_method} not expected')

        sts_client = self.basic_session.client("sts", config=self.config)

        if assume_role_method == 'assume_role':
            sts_response = self._assume_role(sts_client=sts_client)
        else:
            sts_response = self._assume_role_with_saml(sts_client=sts_client)

        sts_response_http_status = sts_response['ResponseMetadata']['HTTPStatusCode']
        if sts_response_http_status != 200:
            raise RuntimeError(f'sts_response_http_status={sts_response_http_status}')

        credentials = sts_response['Credentials']
        expiry_time = credentials.get('Expiration').isoformat()
        self.log.debug('New credentials expiry_time: %s', expiry_time)
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
        saml_config = self.extra_config['assume_role_with_saml']
        principal_arn = saml_config['principal_arn']

        idp_auth_method = saml_config['idp_auth_method']
        if idp_auth_method == 'http_spegno_auth':
            saml_assertion = self._fetch_saml_assertion_using_http_spegno_auth(saml_config)
        else:
            raise NotImplementedError(
                f'idp_auth_method={idp_auth_method} in Connection {self.conn.conn_id} Extra.'
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
            from requests.packages.urllib3.util.retry import Retry

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
        if 'mutual_authentication' in saml_config:
            mutual_auth = saml_config['mutual_authentication']
            if mutual_auth == 'REQUIRED':
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.REQUIRED)
            elif mutual_auth == 'OPTIONAL':
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.OPTIONAL)
            elif mutual_auth == 'DISABLED':
                auth = requests_gssapi.HTTPSPNEGOAuth(requests_gssapi.DISABLED)
            else:
                raise NotImplementedError(
                    f'mutual_authentication={mutual_auth} in Connection {self.conn.conn_id} Extra.'
                    'Currently "REQUIRED", "OPTIONAL" and "DISABLED" are supported.'
                    '(Exclude this setting will default to HTTPSPNEGOAuth() ).'
                )
        # Query the IDP
        idp_response = self._get_idp_response(saml_config, auth=auth)
        # Assist with debugging. Note: contains sensitive info!
        xpath = saml_config['saml_response_xpath']
        log_idp_response = 'log_idp_response' in saml_config and saml_config['log_idp_response']
        if log_idp_response:
            self.log.warning(
                'The IDP response contains sensitive information, but log_idp_response is ON (%s).',
                log_idp_response,
            )
            self.log.debug('idp_response.content= %s', idp_response.content)
            self.log.debug('xpath= %s', xpath)
        # Extract SAML Assertion from the returned HTML / XML
        xml = etree.fromstring(idp_response.content)
        saml_assertion = xml.xpath(xpath)
        if isinstance(saml_assertion, list):
            if len(saml_assertion) == 1:
                saml_assertion = saml_assertion[0]
        if not saml_assertion:
            raise ValueError('Invalid SAML Assertion')
        return saml_assertion

    def _get_web_identity_credential_fetcher(
        self,
    ) -> botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher:
        base_session = self.basic_session._session or botocore.session.get_session()
        client_creator = base_session.create_client
        federation = self.extra_config.get('assume_role_with_web_identity_federation')
        if federation == 'google':
            web_identity_token_loader = self._get_google_identity_token_loader()
        else:
            raise AirflowException(
                f'Unsupported federation: {federation}. Currently "google" only are supported.'
            )
        return botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher(
            client_creator=client_creator,
            web_identity_token_loader=web_identity_token_loader,
            role_arn=self.role_arn,
            extra_args=self.conn.assume_role_kwargs,
        )

    def _get_google_identity_token_loader(self):
        from google.auth.transport import requests as requests_transport

        from airflow.providers.google.common.utils.id_token_credentials import (
            get_default_id_token_credentials,
        )

        audience = self.extra_config.get('assume_role_with_web_identity_federation_audience')

        google_id_token_credentials = get_default_id_token_credentials(target_audience=audience)

        def web_identity_token_loader():
            if not google_id_token_credentials.valid:
                request_adapter = requests_transport.Request()
                google_id_token_credentials.refresh(request=request_adapter)
            return google_id_token_credentials.token

        return web_identity_token_loader

    def _strip_invalid_session_name_characters(self, role_session_name: str) -> str:
        return slugify(role_session_name, regex_pattern=r'[^\w+=,.@-]+')

    def _get_region_name(self) -> str | None:
        warnings.warn(
            "`BaseSessionFactory._get_region_name` method deprecated and will be removed "
            "in a future releases. Please use `BaseSessionFactory.region_name` property instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.region_name

    def _read_role_arn_from_extra_config(self) -> str | None:
        warnings.warn(
            "`BaseSessionFactory._read_role_arn_from_extra_config` method deprecated and will be removed "
            "in a future releases. Please use `BaseSessionFactory.role_arn` property instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.role_arn

    def _read_credentials_from_connection(self) -> tuple[str | None, str | None]:
        warnings.warn(
            "`BaseSessionFactory._read_credentials_from_connection` method deprecated and will be removed "
            "in a future releases. Please use `BaseSessionFactory.conn.aws_access_key_id` and "
            "`BaseSessionFactory.aws_secret_access_key` properties instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.conn.aws_access_key_id, self.conn.aws_secret_access_key


class AwsGenericHook(BaseHook, Generic[BaseAwsConnection]):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    conn_name_attr = 'aws_conn_id'
    default_conn_name = 'aws_default'
    conn_type = 'aws'
    hook_name = 'Amazon Web Services'

    def __init__(
        self,
        aws_conn_id: str | None = default_conn_name,
        verify: bool | str | None = None,
        region_name: str | None = None,
        client_type: str | None = None,
        resource_type: str | None = None,
        config: Config | None = None,
    ) -> None:
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.client_type = client_type
        self.resource_type = resource_type

        self._region_name = region_name
        self._config = config
        self._verify = verify

    @cached_property
    def conn_config(self) -> AwsConnectionWrapper:
        """Get the Airflow Connection object and wrap it in helper (cached)."""
        connection = None
        if self.aws_conn_id:
            try:
                connection = self.get_connection(self.aws_conn_id)
            except AirflowNotFoundException:
                warnings.warn(
                    f"Unable to find AWS Connection ID '{self.aws_conn_id}', switching to empty. "
                    "This behaviour is deprecated and will be removed in a future releases. "
                    "Please provide existed AWS connection ID or if required boto3 credential strategy "
                    "explicit set AWS Connection ID to None.",
                    DeprecationWarning,
                    stacklevel=2,
                )

        return AwsConnectionWrapper(
            conn=connection, region_name=self._region_name, botocore_config=self._config, verify=self._verify
        )

    @property
    def region_name(self) -> str | None:
        """AWS Region Name read-only property."""
        return self.conn_config.region_name

    @property
    def config(self) -> Config | None:
        """Configuration for botocore client read-only property."""
        return self.conn_config.botocore_config

    @property
    def verify(self) -> bool | str | None:
        """Verify or not SSL certificates boto3 client/resource read-only property."""
        return self.conn_config.verify

    def get_session(self, region_name: str | None = None) -> boto3.session.Session:
        """Get the underlying boto3.session.Session(region_name=region_name)."""
        return SessionFactory(
            conn=self.conn_config, region_name=region_name, config=self.config
        ).create_session()

    def get_client_type(
        self,
        region_name: str | None = None,
        config: Config | None = None,
    ) -> boto3.client:
        """Get the underlying boto3 client using boto3 session"""
        client_type = self.client_type

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        session = self.get_session(region_name=region_name)
        return session.client(
            client_type, endpoint_url=self.conn_config.endpoint_url, config=config, verify=self.verify
        )

    def get_resource_type(
        self,
        region_name: str | None = None,
        config: Config | None = None,
    ) -> boto3.resource:
        """Get the underlying boto3 resource using boto3 session"""
        resource_type = self.resource_type

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        session = self.get_session(region_name=region_name)
        return session.resource(
            resource_type, endpoint_url=self.conn_config.endpoint_url, config=config, verify=self.verify
        )

    @cached_property
    def conn(self) -> BaseAwsConnection:
        """
        Get the underlying boto3 client/resource (cached)

        :return: boto3.client or boto3.resource
        :rtype: Union[boto3.client, boto3.resource]
        """
        if not ((not self.client_type) ^ (not self.resource_type)):
            raise ValueError(
                f"Either client_type={self.client_type!r} or "
                f"resource_type={self.resource_type!r} must be provided, not both."
            )
        elif self.client_type:
            return self.get_client_type(region_name=self.region_name)
        else:
            return self.get_resource_type(region_name=self.region_name)

    @cached_property
    def conn_client_meta(self) -> ClientMeta:
        """Get botocore client metadata from Hook connection (cached)."""
        conn = self.conn
        if isinstance(conn, botocore.client.BaseClient):
            return conn.meta
        return conn.meta.client.meta

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
        Get the underlying boto3 client/resource (cached)

        Implemented so that caching works as intended. It exists for compatibility
        with subclasses that rely on a super().get_conn() method.

        :return: boto3.client or boto3.resource
        :rtype: Union[boto3.client, boto3.resource]
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
        If the IAM role is a role name, get the Amazon Resource Name (ARN) for the role.
        If IAM role is already an IAM role ARN, no change is made.

        :param role: IAM role name or ARN
        :param region_name: Optional region name to get credentials for
        :return: IAM role ARN
        """
        if "/" in role:
            return role
        else:
            session = self.get_session(region_name=region_name)
            _client = session.client(
                'iam', endpoint_url=self.conn_config.endpoint_url, config=self.config, verify=self.verify
            )
            return _client.get_role(RoleName=role)["Role"]["Arn"]

    @staticmethod
    def retry(should_retry: Callable[[Exception], bool]):
        """
        A decorator that provides a mechanism to repeat requests in response to exceeding a temporary quote
        limit.
        """

        def retry_decorator(fun: Callable):
            @wraps(fun)
            def decorator_f(self, *args, **kwargs):
                retry_args = getattr(self, 'retry_args', None)
                if retry_args is None:
                    return fun(self, *args, **kwargs)
                multiplier = retry_args.get('multiplier', 1)
                min_limit = retry_args.get('min', 1)
                max_limit = retry_args.get('max', 1)
                stop_after_delay = retry_args.get('stop_after_delay', 10)
                tenacity_before_logger = tenacity.before_log(self.log, logging.INFO) if self.log else None
                tenacity_after_logger = tenacity.after_log(self.log, logging.INFO) if self.log else None
                default_kwargs = {
                    'wait': tenacity.wait_exponential(multiplier=multiplier, max=max_limit, min=min_limit),
                    'retry': tenacity.retry_if_exception(should_retry),
                    'stop': tenacity.stop_after_delay(stop_after_delay),
                    'before': tenacity_before_logger,
                    'after': tenacity_after_logger,
                }
                return tenacity.retry(**default_kwargs)(fun)(self, *args, **kwargs)

            return decorator_f

        return retry_decorator

    def _get_credentials(self, region_name: str | None) -> tuple[boto3.session.Session, str | None]:
        warnings.warn(
            "`AwsGenericHook._get_credentials` method deprecated and will be removed in a future releases. "
            "Please use `AwsGenericHook.get_session` method and "
            "`AwsGenericHook.conn_config.endpoint_url` property instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        return self.get_session(region_name=region_name), self.conn_config.endpoint_url

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom UI field behaviour for AWS Connection."""
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
        Tests the AWS connection by call AWS STS (Security Token Service) GetCallerIdentity API.

        .. seealso::
            https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html
        """
        orig_client_type, self.client_type = self.client_type, 'sts'
        try:
            res = self.get_client_type().get_caller_identity()
            metadata = res.pop("ResponseMetadata", {})
            if metadata.get("HTTPStatusCode") == 200:
                return True, json.dumps(res)
            else:
                try:
                    return False, json.dumps(metadata)
                except TypeError:
                    return False, str(metadata)
        except Exception as e:
            return False, str(e)
        finally:
            self.client_type = orig_client_type


class AwsBaseHook(AwsGenericHook[Union[boto3.client, boto3.resource]]):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library
    with basic conn annotation.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`
    """


def resolve_session_factory() -> type[BaseSessionFactory]:
    """Resolves custom SessionFactory class"""
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
    """For compatibility with airflow.contrib.hooks.aws_hook"""
    from airflow.providers.amazon.aws.utils.connection_wrapper import _parse_s3_config

    return _parse_s3_config(
        config_file_name=config_file_name,
        config_format=config_format,
        profile=profile,
    )
