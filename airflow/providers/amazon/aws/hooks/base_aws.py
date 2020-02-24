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
import logging

import boto3
from botocore.config import Config

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class AwsBaseHook(BaseHook):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None then the default boto3 behaviour is used. If running Airflow
        in a distributed manner and aws_conn_id is None, then default boto3 configuration
        would be used (and must be maintained on each worker node).
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :type verify: str or bool
    """

    def __init__(self, aws_conn_id="aws_default", verify=None):
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.config = None

    def _get_credentials(self, region_name):
        aws_access_key_id = None
        aws_secret_access_key = None
        aws_session_token = None
        endpoint_url = None
        session_kwargs = dict()

        if self.aws_conn_id:  # pylint: disable=too-many-nested-blocks
            self.log.info("Airflow Connection: aws_conn_id=%s",
                          self.aws_conn_id)
            try:
                # Fetch the Airflow connection object
                connection_object = self.get_connection(self.aws_conn_id)
                extra_config = connection_object.extra_dejson
                creds_from = None
                if connection_object.login:
                    creds_from = "login"
                    aws_access_key_id = connection_object.login
                    aws_secret_access_key = connection_object.password

                elif (
                    "aws_access_key_id" in extra_config and
                    "aws_secret_access_key" in extra_config
                ):
                    creds_from = "extra_config"
                    aws_access_key_id = extra_config["aws_access_key_id"]
                    aws_secret_access_key = extra_config["aws_secret_access_key"]

                elif "s3_config_file" in extra_config:
                    creds_from = "extra_config['s3_config_file']"
                    aws_access_key_id, aws_secret_access_key = _parse_s3_config(
                        extra_config["s3_config_file"],
                        extra_config.get("s3_config_format"),
                        extra_config.get("profile"),
                    )

                if "aws_session_token" in extra_config:
                    aws_session_token = extra_config["aws_session_token"]

                if creds_from:
                    self.log.info(
                        "Credentials retrieved from %s.%s", self.aws_conn_id, creds_from
                    )
                else:
                    self.log.info(
                        "No credentials retrieved from Connection %s", self.aws_conn_id)

                # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html#botocore.config.Config
                if "config_kwargs" in extra_config:
                    self.log.info(
                        "Retrieving config_kwargs from Connection.extra_config['config_kwargs']: %s",
                        extra_config["config_kwargs"]
                    )
                    self.config = Config(**extra_config["config_kwargs"])

                if region_name is None and 'region_name' in extra_config:
                    self.log.info(
                        "Retrieving region_name from Connection.extra_config['region_name']"
                    )
                    region_name = extra_config.get("region_name")
                self.log.info("region_name=%s", region_name)

                role_arn = extra_config.get("role_arn")

                aws_account_id = extra_config.get("aws_account_id")
                aws_iam_role = extra_config.get("aws_iam_role")

                if (
                    role_arn is None and
                    aws_account_id is not None and
                    aws_iam_role is not None
                ):
                    self.log.info(
                        "Constructing role_arn from aws_account_id and aws_iam_role"
                    )
                    role_arn = "arn:aws:iam::{}:role/{}".format(
                        aws_account_id, aws_iam_role
                    )
                self.log.info("role_arn is %s", role_arn)

                if "session_kwargs" in extra_config:
                    self.log.info(
                        "Retrieving session_kwargs from Connection.extra_config['session_kwargs']: %s",
                        extra_config["session_kwargs"]
                    )
                    session_kwargs = extra_config["session_kwargs"]

                # If role_arn was specified then STS + assume_role
                if role_arn is not None:
                    # Create STS session and client
                    self.log.info(
                        "Creating sts_session with aws_access_key_id=%s",
                        aws_access_key_id,
                    )
                    sts_session = boto3.session.Session(
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name,
                        aws_session_token=aws_session_token,
                        **session_kwargs
                    )
                    sts_client = sts_session.client("sts", config=self.config)
                    # Assume role
                    assume_role_kwargs = dict()
                    if "assume_role_kwargs" in extra_config:
                        assume_role_kwargs = extra_config["assume_role_kwargs"]
                    if "external_id" in extra_config:  # Backwards compatibility
                        assume_role_kwargs["ExternalId"] = extra_config.get(
                            "external_id"
                        )

                    role_session_name = "Airflow_" + self.aws_conn_id
                    self.log.info(
                        "Doing assume_role to role_arn=%s role_session_name=%s",
                        role_arn,
                        role_session_name,
                    )
                    sts_response = sts_client.assume_role(
                        RoleArn=role_arn,
                        RoleSessionName=role_session_name,
                        **assume_role_kwargs
                    )
                    # Use credentials retrieved from STS
                    credentials = sts_response["Credentials"]
                    aws_access_key_id = credentials["AccessKeyId"]
                    aws_secret_access_key = credentials["SecretAccessKey"]
                    aws_session_token = credentials["SessionToken"]

                endpoint_url = extra_config.get("host")

            except AirflowException:
                self.log.warning(
                    "Unable to use Airflow Connection for credentials.")
                self.log.info("Fallback on boto3 credential strategy")
                # http://boto3.readthedocs.io/en/latest/guide/configuration.html

        self.log.info(
            "Creating session with aws_access_key_id=%s region_name=%s",
            aws_access_key_id,
            region_name,
        )
        return (
            boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
                **session_kwargs
            ),
            endpoint_url,
        )

    def get_client_type(self, client_type, region_name=None, config=None):
        """ Get the underlying boto3 client using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        return session.client(
            client_type, endpoint_url=endpoint_url, config=config, verify=self.verify
        )

    def get_resource_type(self, resource_type, region_name=None, config=None):
        """ Get the underlying boto3 resource using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        # No AWS Operators use the config argument to this method.
        # Keep backward compatibility with other users who might use it
        if config is None:
            config = self.config

        return session.resource(
            resource_type, endpoint_url=endpoint_url, config=config, verify=self.verify
        )

    def get_session(self, region_name=None):
        """Get the underlying boto3.session."""
        session, _ = self._get_credentials(region_name)
        return session

    def get_credentials(self, region_name=None):
        """Get the underlying `botocore.Credentials` object.

        This contains the following authentication attributes: access_key, secret_key and token.
        """
        session, _ = self._get_credentials(region_name)
        # Credentials are refreshable, so accessing your access key and
        # secret key separately can lead to a race condition.
        # See https://stackoverflow.com/a/36291428/8283373
        return session.get_credentials().get_frozen_credentials()

    def expand_role(self, role):
        """
        If the IAM role is a role name, get the Amazon Resource Name (ARN) for the role.
        If IAM role is already an IAM role ARN, no change is made.

        :param role: IAM role name or ARN
        :return: IAM role ARN
        """
        if "/" in role:
            return role
        else:
            return self.get_client_type("iam").get_role(RoleName=role)["Role"]["Arn"]


def _parse_s3_config(config_file_name, config_format="boto", profile=None):
    """
    Parses a config file for s3 credentials. Can currently
    parse boto, s3cmd.conf and AWS SDK config formats

    :param config_file_name: path to the config file
    :type config_file_name: str
    :param config_format: config type. One of "boto", "s3cmd" or "aws".
        Defaults to "boto"
    :type config_format: str
    :param profile: profile name in AWS type config file
    :type profile: str
    """
    config = configparser.ConfigParser()
    if config.read(config_file_name):  # pragma: no cover
        sections = config.sections()
    else:
        raise AirflowException("Couldn't read {0}".format(config_file_name))
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
