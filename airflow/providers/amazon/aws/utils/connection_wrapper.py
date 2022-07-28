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
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from botocore.config import Config

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection


@dataclass
class AwsConnectionWrapper(LoggingMixin):
    """
    AWS Connection Wrapper class helper.
    Use for validate and resolve AWS Connection parameters.
    """

    conn: InitVar[Optional["Connection"]]

    conn_id: Optional[str] = field(init=False, default=None)
    conn_type: Optional[str] = field(init=False, default=None)
    login: Optional[str] = field(init=False, repr=False, default=None)
    password: Optional[str] = field(init=False, repr=False, default=None)
    extra_config: Dict[str, Any] = field(init=False, repr=False, default_factory=dict)

    aws_access_key_id: Optional[str] = field(init=False)
    aws_secret_access_key: Optional[str] = field(init=False)
    aws_session_token: Optional[str] = field(init=False)

    region_name: Optional[str] = field(init=False, default=None)
    session_kwargs: Dict[str, Any] = field(init=False, default_factory=dict)
    botocore_config: Optional[Config] = field(init=False, default=None)
    endpoint_url: Optional[str] = field(init=False, default=None)

    role_arn: Optional[str] = field(init=False, default=None)
    assume_role_method: Optional[str] = field(init=False, default=None)
    assume_role_kwargs: Dict[str, Any] = field(init=False, default_factory=dict)

    @cached_property
    def conn_repr(self):
        return f"AWS Connection (conn_id={self.conn_id!r}, conn_type={self.conn_type!r})"

    def __post_init__(self, conn: "Connection"):
        if not conn:
            return

        extra = deepcopy(conn.extra_dejson)

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

        # Retrieve initial connection credentials
        init_credentials = self._get_credentials(**extra)
        self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token = init_credentials

        if "region_name" in extra:
            self.region_name = extra["region_name"]
            self.log.info("Retrieving region_name=%s from %s extra.", self.region_name, self.conn_repr)

        if "session_kwargs" in extra:
            self.session_kwargs = extra["session_kwargs"]
            self.log.info("Retrieving session_kwargs=%s from %s extra.", self.session_kwargs, self.conn_repr)

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
        if config_kwargs:
            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
            self.log.info("Retrieving botocore config=%s from %s extra.", config_kwargs, self.conn_repr)
            self.botocore_config = Config(**config_kwargs)

        self.endpoint_url = extra.get("host")

        # Retrieve Assume Role Configuration
        assume_role_configs = self._get_assume_role_configs(**extra)
        self.role_arn, self.assume_role_method, self.assume_role_kwargs = assume_role_configs

    @property
    def extra_dejson(self):
        return self.extra_config

    def __bool__(self):
        return self.conn_id is not None

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
        **kwargs,
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Get AWS credentials from connection login/password and extra.

        ``aws_access_key_id`` and ``aws_secret_access_key`` order
        1. From Connection login and password
        2. From Connection extra['aws_access_key_id'] and extra['aws_access_key_id']
        3. (deprecated) From local credentials file

        Get ``aws_session_token`` from extra['aws_access_key_id']

        """
        if self.login and self.password:
            self.log.info("%s credentials retrieved from login and password.", self.conn_repr)
            aws_access_key_id, aws_secret_access_key = self.login, self.password
        elif aws_access_key_id and aws_secret_access_key:
            self.log.info("%s credentials retrieved from extra.", self.conn_repr)
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
            self.log.info("Retrieving role_arn=%r from %s extra.", role_arn, self.conn_repr)
        elif aws_account_id and aws_iam_role:
            warnings.warn(
                "Constructing 'role_arn' from extra['aws_account_id'] and extra['aws_iam_role'] is deprecated"
                f" and will be removed in a future releases."
                f" Please set 'role_arn' in {self.conn_repr} extra.",
                DeprecationWarning,
                stacklevel=3,
            )
            role_arn = f"arn:aws:iam::{aws_account_id}:role/{aws_iam_role}"
            self.log.info(
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
        self.log.info("Retrieve assume_role_method=%r from %s.", assume_role_method, self.conn_repr)

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
