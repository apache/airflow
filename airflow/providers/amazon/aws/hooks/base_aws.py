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

import configparser
import datetime
import json
import logging
import warnings
from functools import wraps
from typing import Any, Callable, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

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
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.utils.log.logging_mixin import LoggingMixin

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

    def __init__(self, conn: Connection, region_name: Optional[str], config: Config) -> None:
        super().__init__()
        self.conn = conn
        self.region_name = region_name
        self.config = config
        self.extra_config = self.conn.extra_dejson

        self.basic_session: Optional[boto3.session.Session] = None
        self.role_arn: Optional[str] = None

    def create_session(self) -> boto3.session.Session:
        """Create AWS session."""
        session_kwargs = {}
        if "session_kwargs" in self.extra_config:
            self.log.info(
                "Retrieving session_kwargs from Connection.extra_config['session_kwargs']: %s",
                self.extra_config["session_kwargs"],
            )
            session_kwargs = self.extra_config["session_kwargs"]
        self.basic_session = self._create_basic_session(session_kwargs=session_kwargs)
        self.role_arn = self._read_role_arn_from_extra_config()
        # If role_arn was specified then STS + assume_role
        if self.role_arn is None:
            return self.basic_session

        return self._create_session_with_assume_role(session_kwargs=session_kwargs)

    def _get_region_name(self) -> Optional[str]:
        region_name = self.region_name
        if self.region_name is None and 'region_name' in self.extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            region_name = self.extra_config["region_name"]
        return region_name

    def _create_basic_session(self, session_kwargs: Dict[str, Any]) -> boto3.session.Session:
        aws_access_key_id, aws_secret_access_key = self._read_credentials_from_connection()
        aws_session_token = self.extra_config.get("aws_session_token")
        region_name = self._get_region_name()
        self.log.debug(
            "Creating session with aws_access_key_id=%s region_name=%s",
            aws_access_key_id,
            region_name,
        )

        return boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            aws_session_token=aws_session_token,
            **session_kwargs,
        )

    def _create_session_with_assume_role(self, session_kwargs: Dict[str, Any]) -> boto3.session.Session:
        assume_role_method = self.extra_config.get('assume_role_method', 'assume_role')
        self.log.debug("assume_role_method=%s", assume_role_method)
        supported_methods = ['assume_role', 'assume_role_with_saml', 'assume_role_with_web_identity']
        if assume_role_method not in supported_methods:
            raise NotImplementedError(
                f'assume_role_method={assume_role_method} in Connection {self.conn.conn_id} Extra.'
                f'Currently {supported_methods} are supported.'
                '(Exclude this setting will default to "assume_role").'
            )
        if assume_role_method == 'assume_role_with_web_identity':
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

        if self.basic_session is None:
            raise RuntimeError("The basic session should be created here!")

        region_name = self.basic_session.region_name
        session.set_config_variable("region", region_name)

        return boto3.session.Session(botocore_session=session, **session_kwargs)

    def _refresh_credentials(self) -> Dict[str, Any]:
        self.log.debug('Refreshing credentials')
        assume_role_method = self.extra_config.get('assume_role_method', 'assume_role')
        sts_session = self.basic_session

        if sts_session is None:
            raise RuntimeError(
                "Session should be initialized when refresh credentials with assume_role is used!"
            )

        sts_client = sts_session.client("sts", config=self.config)

        if assume_role_method == 'assume_role':
            sts_response = self._assume_role(sts_client=sts_client)
        elif assume_role_method == 'assume_role_with_saml':
            sts_response = self._assume_role_with_saml(sts_client=sts_client)
        else:
            raise NotImplementedError(f'assume_role_method={assume_role_method} not expected')
        sts_response_http_status = sts_response['ResponseMetadata']['HTTPStatusCode']
        if not sts_response_http_status == 200:
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

    def _read_role_arn_from_extra_config(self) -> Optional[str]:
        aws_account_id = self.extra_config.get("aws_account_id")
        aws_iam_role = self.extra_config.get("aws_iam_role")
        role_arn = self.extra_config.get("role_arn")
        if role_arn is None and aws_account_id is not None and aws_iam_role is not None:
            self.log.info("Constructing role_arn from aws_account_id and aws_iam_role")
            role_arn = f"arn:aws:iam::{aws_account_id}:role/{aws_iam_role}"
        self.log.debug("role_arn is %s", role_arn)
        return role_arn

    def _read_credentials_from_connection(self) -> Tuple[Optional[str], Optional[str]]:
        aws_access_key_id = None
        aws_secret_access_key = None
        if self.conn.login:
            aws_access_key_id = self.conn.login
            aws_secret_access_key = self.conn.password
            self.log.info("Credentials retrieved from login")
        elif "aws_access_key_id" in self.extra_config and "aws_secret_access_key" in self.extra_config:
            aws_access_key_id = self.extra_config["aws_access_key_id"]
            aws_secret_access_key = self.extra_config["aws_secret_access_key"]
            self.log.info("Credentials retrieved from extra_config")
        elif "s3_config_file" in self.extra_config:
            aws_access_key_id, aws_secret_access_key = _parse_s3_config(
                self.extra_config["s3_config_file"],
                self.extra_config.get("s3_config_format"),
                self.extra_config.get("profile"),
            )
            self.log.info("Credentials retrieved from extra_config['s3_config_file']")
        return aws_access_key_id, aws_secret_access_key

    def _strip_invalid_session_name_characters(self, role_session_name: str) -> str:
        return slugify(role_session_name, regex_pattern=r'[^\w+=,.@-]+')

    def _assume_role(self, sts_client: boto3.client) -> Dict:
        assume_role_kwargs = self.extra_config.get("assume_role_kwargs", {})
        if "external_id" in self.extra_config:  # Backwards compatibility
            assume_role_kwargs["ExternalId"] = self.extra_config.get("external_id")
        role_session_name = self._strip_invalid_session_name_characters(f"Airflow_{self.conn.conn_id}")
        self.log.debug(
            "Doing sts_client.assume_role to role_arn=%s (role_session_name=%s)",
            self.role_arn,
            role_session_name,
        )
        return sts_client.assume_role(
            RoleArn=self.role_arn, RoleSessionName=role_session_name, **assume_role_kwargs
        )

    def _assume_role_with_saml(self, sts_client: boto3.client) -> Dict[str, Any]:
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
        assume_role_kwargs = self.extra_config.get("assume_role_kwargs", {})
        return sts_client.assume_role_with_saml(
            RoleArn=self.role_arn,
            PrincipalArn=principal_arn,
            SAMLAssertion=saml_assertion,
            **assume_role_kwargs,
        )

    def _get_idp_response(
        self, saml_config: Dict[str, Any], auth: requests.auth.AuthBase
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

    def _fetch_saml_assertion_using_http_spegno_auth(self, saml_config: Dict[str, Any]) -> str:
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
        if self.basic_session is None:
            raise Exception("Session should be set where identity is fetched!")
        base_session = self.basic_session._session or botocore.session.get_session()
        client_creator = base_session.create_client
        federation = self.extra_config.get('assume_role_with_web_identity_federation')
        if federation == 'google':
            web_identity_token_loader = self._get_google_identity_token_loader()
        else:
            raise AirflowException(
                f'Unsupported federation: {federation}. Currently "google" only are supported.'
            )
        assume_role_kwargs = self.extra_config.get("assume_role_kwargs", {})
        return botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher(
            client_creator=client_creator,
            web_identity_token_loader=web_identity_token_loader,
            role_arn=self.role_arn,
            extra_args=assume_role_kwargs,
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


class AwsGenericHook(BaseHook, Generic[BaseAwsConnection]):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    """

    conn_name_attr = 'aws_conn_id'
    default_conn_name = 'aws_default'
    conn_type = 'aws'
    hook_name = 'Amazon Web Services'

    def __init__(
        self,
        aws_conn_id: Optional[str] = default_conn_name,
        verify: Union[bool, str, None] = None,
        region_name: Optional[str] = None,
        client_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> None:
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.client_type = client_type
        self.resource_type = resource_type
        self.region_name = region_name
        self.config = config

    def _get_credentials(self, region_name: Optional[str]) -> Tuple[boto3.session.Session, Optional[str]]:

        if not self.aws_conn_id:
            session = boto3.session.Session(region_name=region_name)
            return session, None

        self.log.debug("Airflow Connection: aws_conn_id=%s", self.aws_conn_id)

        try:
            # Fetch the Airflow connection object
            connection_object = self.get_connection(self.aws_conn_id)
            extra_config = connection_object.extra_dejson
            endpoint_url = extra_config.get("host")

            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html#botocore.config.Config
            if "config_kwargs" in extra_config:
                self.log.debug(
                    "Retrieving config_kwargs from Connection.extra_config['config_kwargs']: %s",
                    extra_config["config_kwargs"],
                )
                self.config = Config(**extra_config["config_kwargs"])

            session = SessionFactory(
                conn=connection_object, region_name=region_name, config=self.config
            ).create_session()

            return session, endpoint_url

        except AirflowException:
            self.log.warning("Unable to use Airflow Connection for credentials.")
            self.log.debug("Fallback on boto3 credential strategy")
            # http://boto3.readthedocs.io/en/latest/guide/configuration.html

        self.log.debug(
            "Creating session using boto3 credential strategy region_name=%s",
            region_name,
        )
        session = boto3.session.Session(region_name=region_name)
        return session, None

    def get_client_type(
        self,
        client_type: Optional[str] = None,
        region_name: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> boto3.client:
        """Get the underlying boto3 client using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name=region_name)

        if client_type:
            warnings.warn(
                "client_type is deprecated. Set client_type from class attribute.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            client_type = self.client_type

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        return session.client(client_type, endpoint_url=endpoint_url, config=config, verify=self.verify)

    def get_resource_type(
        self,
        resource_type: Optional[str] = None,
        region_name: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> boto3.resource:
        """Get the underlying boto3 resource using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name=region_name)

        if resource_type:
            warnings.warn(
                "resource_type is deprecated. Set resource_type from class attribute.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            resource_type = self.resource_type

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        return session.resource(resource_type, endpoint_url=endpoint_url, config=config, verify=self.verify)

    @cached_property
    def conn(self) -> Union[boto3.client, boto3.resource]:
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
        conn = self.conn
        if isinstance(conn, botocore.client.BaseClient):
            return conn.meta
        return conn.meta.client.meta

    @property
    def conn_region_name(self) -> str:
        return self.conn_client_meta.region_name

    @property
    def conn_partition(self) -> str:
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

    def get_session(self, region_name: Optional[str] = None) -> boto3.session.Session:
        """Get the underlying boto3.session."""
        session, _ = self._get_credentials(region_name=region_name)
        return session

    def get_credentials(self, region_name: Optional[str] = None) -> ReadOnlyCredentials:
        """
        Get the underlying `botocore.Credentials` object.

        This contains the following authentication attributes: access_key, secret_key and token.
        """
        session, _ = self._get_credentials(region_name=region_name)
        # Credentials are refreshable, so accessing your access key and
        # secret key separately can lead to a race condition.
        # See https://stackoverflow.com/a/36291428/8283373
        return session.get_credentials().get_frozen_credentials()

    def expand_role(self, role: str, region_name: Optional[str] = None) -> str:
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
            session, endpoint_url = self._get_credentials(region_name=region_name)
            _client = session.client('iam', endpoint_url=endpoint_url, config=self.config, verify=self.verify)
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


def _parse_s3_config(
    config_file_name: str, config_format: Optional[str] = "boto", profile: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses a config file for s3 credentials. Can currently
    parse boto, s3cmd.conf and AWS SDK config formats

    :param config_file_name: path to the config file
    :param config_format: config type. One of "boto", "s3cmd" or "aws".
        Defaults to "boto"
    :param profile: profile name in AWS type config file
    """
    config = configparser.ConfigParser()
    if config.read(config_file_name):  # pragma: no cover
        sections = config.sections()
    else:
        raise AirflowException(f"Couldn't read {config_file_name}")
    # Setting option names depending on file format
    if config_format is None:
        config_format = "boto"
    conf_format = config_format.lower()
    if conf_format == "boto":  # pragma: no cover
        if profile is not None and "profile " + profile in sections:
            cred_section = "profile " + profile
        else:
            cred_section = "Credentials"
    elif conf_format == "aws" and profile is not None:
        cred_section = profile
    else:
        cred_section = "default"
    # Option names
    if conf_format in ("boto", "aws"):  # pragma: no cover
        key_id_option = "aws_access_key_id"
        secret_key_option = "aws_secret_access_key"
        # security_token_option = 'aws_security_token'
    else:
        key_id_option = "access_key"
        secret_key_option = "secret_key"
    # Actual Parsing
    if cred_section not in sections:
        raise AirflowException("This config file format is not recognized")
    else:
        try:
            access_key = config.get(cred_section, key_id_option)
            secret_key = config.get(cred_section, secret_key_option)
        except Exception:
            logging.warning("Option Error in parsing s3 config file")
            raise
        return access_key, secret_key


def resolve_session_factory() -> Type[BaseSessionFactory]:
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
