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

import warnings
from copy import deepcopy
from dataclasses import MISSING, InitVar, dataclass, field, fields
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

from botocore.config import Config

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from airflow.utils.types import NOTSET, ArgNotSet
except ImportError:  # TODO: Remove when the provider has an Airflow 2.3+ requirement.

    class ArgNotSet:  # type: ignore[no-redef]
        """Sentinel type for annotations, useful when None is not viable."""

    NOTSET = ArgNotSet()

if TYPE_CHECKING:
    from airflow.models.connection import Connection


@dataclass
class AwsConnectionWrapper(LoggingMixin):
    """
    AWS Connection Wrapper class helper.
    Use for validate and resolve AWS Connection parameters.

    ``conn`` reference to Airflow Connection object or AwsConnectionWrapper
        if it set to ``None`` than default values would use.

    The precedence rules for ``region_name``
        1. Explicit set (in Hook) ``region_name``.
        2. Airflow Connection Extra 'region_name'.

    The precedence rules for ``botocore_config``
        1. Explicit set (in Hook) ``botocore_config``.
        2. Construct from Airflow Connection Extra 'botocore_kwargs'.
        3. The wrapper's default value
    """

    conn: InitVar[Optional[Union["Connection", "AwsConnectionWrapper"]]]
    region_name: Optional[str] = field(default=None)
    # boto3 client/resource configs
    botocore_config: Optional[Config] = field(default=None)
    verify: Optional[Union[bool, str]] = field(default=None)

    # Reference to Airflow Connection attributes
    # ``extra_config`` contains original Airflow Connection Extra.
    conn_id: Optional[Union[str, ArgNotSet]] = field(init=False, default=NOTSET)
    conn_type: Optional[str] = field(init=False, default=None)
    login: Optional[str] = field(init=False, repr=False, default=None)
    password: Optional[str] = field(init=False, repr=False, default=None)
    extra_config: Dict[str, Any] = field(init=False, repr=False, default_factory=dict)

    # AWS Credentials from connection.
    aws_access_key_id: Optional[str] = field(init=False, default=None)
    aws_secret_access_key: Optional[str] = field(init=False, default=None)
    aws_session_token: Optional[str] = field(init=False, default=None)

    # AWS Shared Credential profile_name
    profile_name: Optional[str] = field(init=False, default=None)
    # Custom endpoint_url for boto3.client and boto3.resource
    endpoint_url: Optional[str] = field(init=False, default=None)

    # Assume Role Configurations
    role_arn: Optional[str] = field(init=False, default=None)
    assume_role_method: Optional[str] = field(init=False, default=None)
    assume_role_kwargs: Dict[str, Any] = field(init=False, default_factory=dict)

    @cached_property
    def conn_repr(self):
        return f"AWS Connection (conn_id={self.conn_id!r}, conn_type={self.conn_type!r})"

    def __post_init__(self, conn: "Connection"):
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
        elif not conn:
            return

        # Assign attributes from AWS Connection
        self.conn_id = conn.conn_id
        self.conn_type = conn.conn_type or "aws"
        self.login = conn.login
        self.password = conn.password
        self.extra_config = deepcopy(conn.extra_dejson)

        if self.conn_type != "aws":
            warnings.warn(
                f"{self.conn_repr} expected connection type 'aws', got {self.conn_type!r}.",
                UserWarning,
                stacklevel=2,
            )

        extra = deepcopy(conn.extra_dejson)
        session_kwargs = extra.get("session_kwargs", {})
        if session_kwargs:
            warnings.warn(
                "'session_kwargs' in extra config is deprecated and will be removed in a future releases. "
                f"Please specify arguments passed to boto3 Session directly in {self.conn_repr} extra.",
                DeprecationWarning,
                stacklevel=2,
            )

        # Retrieve initial connection credentials
        init_credentials = self._get_credentials(**extra)
        self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token = init_credentials

        if not self.region_name:
            if "region_name" in extra:
                self.region_name = extra["region_name"]
                self.log.debug("Retrieving region_name=%s from %s extra.", self.region_name, self.conn_repr)
            elif "region_name" in session_kwargs:
                self.region_name = session_kwargs["region_name"]
                self.log.debug(
                    "Retrieving region_name=%s from %s extra['session_kwargs'].",
                    self.region_name,
                    self.conn_repr,
                )

        if self.verify is None and "verify" in extra:
            self.verify = extra["verify"]
            self.log.debug("Retrieving verify=%s from %s extra.", self.verify, self.conn_repr)

        if "profile_name" in extra:
            self.profile_name = extra["profile_name"]
            self.log.debug("Retrieving profile_name=%s from %s extra.", self.profile_name, self.conn_repr)
        elif "profile_name" in session_kwargs:
            self.profile_name = session_kwargs["profile_name"]
            self.log.debug(
                "Retrieving profile_name=%s from %s extra['session_kwargs'].",
                self.profile_name,
                self.conn_repr,
            )

        # Warn the user that an invalid parameter is being used which actually not related to 'profile_name'.
        if "profile" in extra and "s3_config_file" not in extra:
            if "profile_name" not in self.session_kwargs:
                warnings.warn(
                    f"Found 'profile' without specifying 's3_config_file' in {self.conn_repr} extra. "
                    "If required profile from AWS Shared Credentials please "
                    f"set 'profile_name' in {self.conn_repr} extra['session_kwargs'].",
                    UserWarning,
                    stacklevel=2,
                )

        config_kwargs = extra.get("config_kwargs")
        if not self.botocore_config and config_kwargs:
            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
            self.log.debug("Retrieving botocore config=%s from %s extra.", config_kwargs, self.conn_repr)
            self.botocore_config = Config(**config_kwargs)

        if conn.host:
            warnings.warn(
                f"Host {conn.host} specified in the connection is not used."
                " Please, set it on extra['endpoint_url'] instead",
                DeprecationWarning,
                stacklevel=2,
            )

        self.endpoint_url = extra.get("host")
        if self.endpoint_url:
            warnings.warn(
                "extra['host'] is deprecated and will be removed in a future release."
                " Please set extra['endpoint_url'] instead",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            self.endpoint_url = extra.get("endpoint_url")

        # Retrieve Assume Role Configuration
        assume_role_configs = self._get_assume_role_configs(**extra)
        self.role_arn, self.assume_role_method, self.assume_role_kwargs = assume_role_configs

    @classmethod
    def from_connection_metadata(
        cls,
        conn_id: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        """
        Create config from connection metadata.

        :param conn_id: Custom connection ID.
        :param login: AWS Access Key ID.
        :param password: AWS Secret Access Key.
        :param extra: Connection Extra metadata.
        """
        from airflow.models.connection import Connection

        return cls(
            conn=Connection(conn_id=conn_id, conn_type="aws", login=login, password=password, extra=extra)
        )

    @property
    def extra_dejson(self):
        """Compatibility with `airflow.models.Connection.extra_dejson` property."""
        return self.extra_config

    @property
    def session_kwargs(self) -> Dict[str, Any]:
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
        return self.conn_id is not NOTSET

    def _get_credentials(
        self,
        *,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # Deprecated Values
        s3_config_file: Optional[str] = None,
        s3_config_format: Optional[str] = None,
        profile: Optional[str] = None,
        session_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Get AWS credentials from connection login/password and extra.

        ``aws_access_key_id`` and ``aws_secret_access_key`` order
        1. From Connection login and password
        2. From Connection extra['aws_access_key_id'] and extra['aws_access_key_id']
        3. (deprecated) Form Connection extra['session_kwargs']
        4. (deprecated) From local credentials file

        Get ``aws_session_token`` from extra['aws_access_key_id']

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
        elif s3_config_file:
            aws_access_key_id, aws_secret_access_key = _parse_s3_config(
                s3_config_file,
                s3_config_format,
                profile,
            )
            self.log.info("%s credentials retrieved from extra['s3_config_file']", self.conn_repr)

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
        role_arn: Optional[str] = None,
        assume_role_method: str = "assume_role",
        assume_role_kwargs: Optional[Dict[str, Any]] = None,
        # Deprecated Values
        aws_account_id: Optional[str] = None,
        aws_iam_role: Optional[str] = None,
        external_id: Optional[str] = None,
        **kwargs,
    ) -> Tuple[Optional[str], Optional[str], Dict[Any, str]]:
        """Get assume role configs from Connection extra."""
        if role_arn:
            self.log.debug("Retrieving role_arn=%r from %s extra.", role_arn, self.conn_repr)
        elif aws_account_id and aws_iam_role:
            warnings.warn(
                "Constructing 'role_arn' from extra['aws_account_id'] and extra['aws_iam_role'] is deprecated"
                f" and will be removed in a future releases."
                f" Please set 'role_arn' in {self.conn_repr} extra.",
                DeprecationWarning,
                stacklevel=3,
            )
            role_arn = f"arn:aws:iam::{aws_account_id}:role/{aws_iam_role}"
            self.log.debug(
                "Constructions role_arn=%r from %s extra['aws_account_id'] and extra['aws_iam_role'].",
                role_arn,
                self.conn_repr,
            )

        if not role_arn:
            # There is no reason obtain `assume_role_method` and `assume_role_kwargs` if `role_arn` not set.
            return None, None, {}

        supported_methods = ['assume_role', 'assume_role_with_saml', 'assume_role_with_web_identity']
        if assume_role_method not in supported_methods:
            raise NotImplementedError(
                f'Found assume_role_method={assume_role_method!r} in {self.conn_repr} extra.'
                f' Currently {supported_methods} are supported.'
                ' (Exclude this setting will default to "assume_role").'
            )
        self.log.debug("Retrieve assume_role_method=%r from %s.", assume_role_method, self.conn_repr)

        assume_role_kwargs = assume_role_kwargs or {}
        if "ExternalId" not in assume_role_kwargs and external_id:
            warnings.warn(
                "'external_id' in extra config is deprecated and will be removed in a future releases. "
                f"Please set 'ExternalId' in 'assume_role_kwargs' in {self.conn_repr} extra.",
                DeprecationWarning,
                stacklevel=3,
            )
            assume_role_kwargs["ExternalId"] = external_id

        return role_arn, assume_role_method, assume_role_kwargs


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
    warnings.warn(
        "Use local credentials file is never documented and well tested. "
        "Obtain credentials by this way deprecated and will be removed in a future releases.",
        DeprecationWarning,
        stacklevel=4,
    )

    import configparser

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
            raise AirflowException("Option Error in parsing s3 config file")
        return access_key, secret_key
