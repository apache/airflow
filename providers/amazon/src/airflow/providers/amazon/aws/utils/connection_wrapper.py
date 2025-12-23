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

import json
import warnings
from copy import deepcopy
from dataclasses import MISSING, InitVar, dataclass, field, fields
from functools import cached_property
from typing import TYPE_CHECKING, Any

from botocore import UNSIGNED
from botocore.config import Config

from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.providers.amazon.version_compat import NOTSET, ArgNotSet
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


@dataclass
class _ConnectionMetadata:
    """
    Connection metadata data-class.

    This class implements main :ref:`~airflow.models.connection.Connection` attributes
    and use in AwsConnectionWrapper for avoid circular imports.

    Only for internal usage, this class might change or removed in the future.
    """

    conn_id: str | None = None
    conn_type: str | None = None
    description: str | None = None
    host: str | None = None
    login: str | None = None
    password: str | None = None
    schema: str | None = None
    port: int | None = None
    extra: str | dict | None = None

    @property
    def extra_dejson(self):
        if not self.extra:
            return {}
        extra = deepcopy(self.extra)
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except json.JSONDecodeError as err:
                raise AirflowException(
                    f"'extra' expected valid JSON-Object string. Original error:\n * {err}"
                ) from None
        if not isinstance(extra, dict):
            raise TypeError(f"Expected JSON-Object or dict, got {type(extra).__name__}.")
        return extra


@dataclass
class AwsConnectionWrapper(LoggingMixin):
    """
    AWS Connection Wrapper class helper.

    Use for validate and resolve AWS Connection parameters.

    ``conn`` references an Airflow Connection object or AwsConnectionWrapper
        if it set to ``None`` than default values would use.

    The precedence rules for ``region_name``
        1. Explicit set (in Hook) ``region_name``.
        2. Airflow Connection Extra 'region_name'.

    The precedence rules for ``botocore_config``
        1. Explicit set (in Hook) ``botocore_config``.
        2. Construct from Airflow Connection Extra 'botocore_kwargs'.
        3. The wrapper's default value
    """

    conn: InitVar[Connection | AwsConnectionWrapper | _ConnectionMetadata | None]
    region_name: str | None = field(default=None)
    # boto3 client/resource configs
    botocore_config: Config | None = field(default=None)
    verify: bool | str | None = field(default=None)

    # Reference to Airflow Connection attributes
    # ``extra_config`` contains original Airflow Connection Extra.
    conn_id: str | ArgNotSet | None = field(init=False, default=NOTSET)
    conn_type: str | None = field(init=False, default=None)
    login: str | None = field(init=False, repr=False, default=None)
    password: str | None = field(init=False, repr=False, default=None)
    schema: str | None = field(init=False, repr=False, default=None)
    extra_config: dict[str, Any] = field(init=False, repr=False, default_factory=dict)

    # AWS Credentials from connection.
    aws_access_key_id: str | None = field(init=False, default=None)
    aws_secret_access_key: str | None = field(init=False, default=None)
    aws_session_token: str | None = field(init=False, default=None)

    # AWS Shared Credential profile_name
    profile_name: str | None = field(init=False, default=None)
    # Custom endpoint_url for boto3.client and boto3.resource
    endpoint_url: str | None = field(init=False, default=None)

    # Assume Role Configurations
    role_arn: str | None = field(init=False, default=None)
    assume_role_method: str | None = field(init=False, default=None)
    assume_role_kwargs: dict[str, Any] = field(init=False, default_factory=dict)

    # Per AWS Service configuration dictionary where key is name of boto3 ``service_name``
    service_config: dict[str, dict[str, Any]] = field(init=False, default_factory=dict)

    @cached_property
    def conn_repr(self):
        return f"AWS Connection (conn_id={self.conn_id!r}, conn_type={self.conn_type!r})"

    def get_service_config(self, service_name: str) -> dict[str, Any]:
        """
        Get AWS Service related config dictionary.

        :param service_name: Name of botocore/boto3 service.
        """
        return self.service_config.get(service_name, {})

    def get_service_endpoint_url(
        self, service_name: str, *, sts_connection_assume: bool = False, sts_test_connection: bool = False
    ) -> str | None:
        service_config = self.get_service_config(service_name=service_name)
        global_endpoint_url = self.endpoint_url

        if service_name == "sts" and True in (sts_connection_assume, sts_test_connection):
            # There are different logics exists historically for STS Client
            # 1. For assume role we never use global endpoint_url
            # 2. For test connection we also use undocumented `test_endpoint`\
            # 3. For STS as service we might use endpoint_url (default for other services)
            global_endpoint_url = None
            if sts_connection_assume and sts_test_connection:
                raise AirflowException(
                    "Can't resolve STS endpoint when both "
                    "`sts_connection` and `sts_test_connection` set to True."
                )

        return service_config.get("endpoint_url", global_endpoint_url)

    def __post_init__(self, conn: Connection | AwsConnectionWrapper | _ConnectionMetadata | None) -> None:
        """Initialize the AwsConnectionWrapper object after instantiation."""
        if isinstance(conn, type(self)):
            # For every field with init=False we copy reference value from original wrapper
            # For every field with init=True we use init values if it not equal default
            # We can't use ``dataclasses.replace`` in classmethod because
            # we limited by InitVar arguments since it not stored in object,
            # and also we do not want to run __post_init__ method again which print all logs/warnings again.
            for fl in fields(conn):
                value = getattr(conn, fl.name)
                if not fl.init:
                    setattr(self, fl.name, value)
                else:
                    if fl.default is not MISSING:
                        default = fl.default
                    elif fl.default_factory is not MISSING:
                        default = fl.default_factory()  # zero-argument callable
                    else:
                        continue  # Value mandatory, skip

                    orig_value = getattr(self, fl.name)
                    if orig_value == default:
                        # Only replace value if it not equal default value
                        setattr(self, fl.name, value)
            return
        if not conn:
            return

        if TYPE_CHECKING:
            assert isinstance(conn, (Connection, _ConnectionMetadata))

        # Assign attributes from AWS Connection
        self.conn_id = conn.conn_id
        self.conn_type = conn.conn_type or "aws"
        self.login = conn.login
        self.password = conn.password
        self.schema = conn.schema or None
        self.extra_config = deepcopy(conn.extra_dejson)

        if self.conn_type != "aws":
            warnings.warn(
                f"{self.conn_repr} expected connection type 'aws', got {self.conn_type!r}. "
                "This connection might not work correctly. "
                "Please use Amazon Web Services Connection type.",
                UserWarning,
                stacklevel=2,
            )

        extra = deepcopy(conn.extra_dejson)
        self.service_config = extra.get("service_config", {})

        # Retrieve initial connection credentials
        init_credentials = self._get_credentials(**extra)
        self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token = init_credentials

        if not self.region_name:
            if "region_name" in extra:
                self.region_name = extra["region_name"]
                self.log.debug("Retrieving region_name=%s from %s extra.", self.region_name, self.conn_repr)

        if self.verify is None and "verify" in extra:
            self.verify = extra["verify"]
            self.log.debug("Retrieving verify=%s from %s extra.", self.verify, self.conn_repr)

        if "profile_name" in extra:
            self.profile_name = extra["profile_name"]
            self.log.debug("Retrieving profile_name=%s from %s extra.", self.profile_name, self.conn_repr)

        # Warn the user that an invalid parameter is being used which actually not related to 'profile_name'.
        # ToDo: Remove this check entirely as soon as drop support credentials from s3_config_file
        if "profile" in extra and "s3_config_file" not in extra and not self.profile_name:
            warnings.warn(
                f"Found 'profile' without specifying 's3_config_file' in {self.conn_repr} extra. "
                "If required profile from AWS Shared Credentials please "
                f"set 'profile_name' in {self.conn_repr} extra.",
                UserWarning,
                stacklevel=2,
            )

        config_kwargs = extra.get("config_kwargs")
        if not self.botocore_config and config_kwargs:
            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
            self.log.debug("Retrieving botocore config=%s from %s extra.", config_kwargs, self.conn_repr)
            if config_kwargs.get("signature_version") == "unsigned":
                config_kwargs["signature_version"] = UNSIGNED
            self.botocore_config = Config(**config_kwargs)

        if "endpoint_url" not in extra:
            self.log.debug(
                "Missing endpoint_url in extra config of AWS Connection with id %s. Using default AWS service endpoint",
                conn.conn_id,
            )

        self.endpoint_url = extra.get("endpoint_url")

        # Retrieve Assume Role Configuration
        assume_role_configs = self._get_assume_role_configs(**extra)
        self.role_arn, self.assume_role_method, self.assume_role_kwargs = assume_role_configs

    @classmethod
    def from_connection_metadata(
        cls,
        conn_id: str | None = None,
        login: str | None = None,
        password: str | None = None,
        extra: dict[str, Any] | None = None,
    ):
        """
        Create config from connection metadata.

        :param conn_id: Custom connection ID.
        :param login: AWS Access Key ID.
        :param password: AWS Secret Access Key.
        :param extra: Connection Extra metadata.
        """
        conn_meta = _ConnectionMetadata(
            conn_id=conn_id, conn_type="aws", login=login, password=password, extra=extra
        )
        return cls(conn=conn_meta)

    @property
    def extra_dejson(self):
        """Compatibility with `airflow.models.Connection.extra_dejson` property."""
        return self.extra_config

    @property
    def session_kwargs(self) -> dict[str, Any]:
        """Additional kwargs passed to boto3.session.Session."""
        return trim_none_values(
            {
                "aws_access_key_id": self.aws_access_key_id,
                "aws_secret_access_key": self.aws_secret_access_key,
                "aws_session_token": self.aws_session_token,
                "region_name": self.region_name,
                "profile_name": self.profile_name,
            }
        )

    def __bool__(self):
        """Return the truth value of the AwsConnectionWrapper instance."""
        return self.conn_id is not NOTSET

    def _get_credentials(
        self,
        *,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        session_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> tuple[str | None, str | None, str | None]:
        """
        Get AWS credentials from connection login/password and extra.

        ``aws_access_key_id`` and ``aws_secret_access_key`` order:

        1. From Connection login and password
        2. From Connection ``extra['aws_access_key_id']`` and
           ``extra['aws_access_key_id']``
        3. (deprecated) Form Connection ``extra['session_kwargs']``
        4. (deprecated) From a local credentials file

        Get ``aws_session_token`` from ``extra['aws_access_key_id']``.
        """
        session_kwargs = session_kwargs or {}
        session_aws_access_key_id = session_kwargs.get("aws_access_key_id")
        session_aws_secret_access_key = session_kwargs.get("aws_secret_access_key")
        session_aws_session_token = session_kwargs.get("aws_session_token")

        if self.login and self.password:
            self.log.info("%s credentials retrieved from login and password.", self.conn_repr)
            aws_access_key_id, aws_secret_access_key = self.login, self.password
        elif aws_access_key_id and aws_secret_access_key:
            self.log.info("%s credentials retrieved from extra.", self.conn_repr)
        elif session_aws_access_key_id and session_aws_secret_access_key:
            aws_access_key_id = session_aws_access_key_id
            aws_secret_access_key = session_aws_secret_access_key
            self.log.info("%s credentials retrieved from extra['session_kwargs'].", self.conn_repr)

        if aws_session_token:
            self.log.info(
                "%s session token retrieved from extra, please note you are responsible for renewing these.",
                self.conn_repr,
            )
        elif session_aws_session_token:
            aws_session_token = session_aws_session_token
            self.log.info(
                "%s session token retrieved from extra['session_kwargs'], "
                "please note you are responsible for renewing these.",
                self.conn_repr,
            )

        return aws_access_key_id, aws_secret_access_key, aws_session_token

    def _get_assume_role_configs(
        self,
        *,
        role_arn: str | None = None,
        assume_role_method: str = "assume_role",
        assume_role_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> tuple[str | None, str | None, dict[Any, str]]:
        """Get assume role configs from Connection extra."""
        if role_arn:
            self.log.debug("Retrieving role_arn=%r from %s extra.", role_arn, self.conn_repr)
        else:
            # There is no reason obtain `assume_role_method` and `assume_role_kwargs` if `role_arn` not set.
            return None, None, {}

        supported_methods = ["assume_role", "assume_role_with_saml", "assume_role_with_web_identity"]
        if assume_role_method not in supported_methods:
            raise NotImplementedError(
                f"Found assume_role_method={assume_role_method!r} in {self.conn_repr} extra."
                f" Currently {supported_methods} are supported."
                ' (Exclude this setting will default to "assume_role").'
            )
        self.log.debug("Retrieve assume_role_method=%r from %s.", assume_role_method, self.conn_repr)

        assume_role_kwargs = assume_role_kwargs or {}

        return role_arn, assume_role_method, assume_role_kwargs
