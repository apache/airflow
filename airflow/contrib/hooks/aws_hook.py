# -*- coding: utf-8 -*-
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
This module contains Base AWS Hook
"""

import logging
import configparser
import boto3

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


def _parse_s3_config(config_file_name, config_format='boto', profile=None):
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
        config_format = 'boto'
    conf_format = config_format.lower()
    if conf_format == 'boto':  # pragma: no cover
        if profile is not None and 'profile ' + profile in sections:
            cred_section = 'profile ' + profile
        else:
            cred_section = 'Credentials'
    elif conf_format == 'aws' and profile is not None:
        cred_section = profile
    else:
        cred_section = 'default'
    # Option names
    if conf_format in ('boto', 'aws'):  # pragma: no cover
        key_id_option = 'aws_access_key_id'
        secret_key_option = 'aws_secret_access_key'
        # security_token_option = 'aws_security_token'
    else:
        key_id_option = 'access_key'
        secret_key_option = 'secret_key'
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


class AwsHook(BaseHook):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.
    """

    def __init__(self, aws_conn_id='aws_default', verify=None):
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def _get_credentials(self, region_name):
        aws_access_key_id = None
        aws_secret_access_key = None
        aws_session_token = None
        endpoint_url = None

        if self.aws_conn_id:  # pylint: disable=too-many-nested-blocks
            try:
                connection_object = self.get_connection(self.aws_conn_id)
                extra_config = connection_object.extra_dejson
                if connection_object.login:
                    aws_access_key_id = connection_object.login
                    aws_secret_access_key = connection_object.password

                elif 'aws_secret_access_key' in extra_config:
                    aws_access_key_id = extra_config[
                        'aws_access_key_id']
                    aws_secret_access_key = extra_config[
                        'aws_secret_access_key']

                elif 's3_config_file' in extra_config:
                    aws_access_key_id, aws_secret_access_key = \
                        _parse_s3_config(
                            extra_config['s3_config_file'],
                            extra_config.get('s3_config_format'),
                            extra_config.get('profile'))

                if region_name is None:
                    region_name = extra_config.get('region_name')

                role_arn = extra_config.get('role_arn')
                external_id = extra_config.get('external_id')
                aws_account_id = extra_config.get('aws_account_id')
                aws_iam_role = extra_config.get('aws_iam_role')
                if 'aws_session_token' in extra_config and aws_session_token is None:
                    aws_session_token = extra_config['aws_session_token']

                if role_arn is None and aws_account_id is not None and aws_iam_role is not None:
                    role_arn = "arn:aws:iam::{}:role/{}" \
                        .format(aws_account_id, aws_iam_role)

                if role_arn is not None:

                    sts_session = boto3.session.Session(
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name,
                        aws_session_token=aws_session_token
                    )

                    sts_client = sts_session.client('sts')

                    if external_id is None:
                        sts_response = sts_client.assume_role(
                            RoleArn=role_arn,
                            RoleSessionName='Airflow_' + self.aws_conn_id)
                    else:
                        sts_response = sts_client.assume_role(
                            RoleArn=role_arn,
                            RoleSessionName='Airflow_' + self.aws_conn_id,
                            ExternalId=external_id)

                    credentials = sts_response['Credentials']
                    aws_access_key_id = credentials['AccessKeyId']
                    aws_secret_access_key = credentials['SecretAccessKey']
                    aws_session_token = credentials['SessionToken']

                endpoint_url = extra_config.get('host')

            except AirflowException:
                # No connection found: fallback on boto3 credential strategy
                # http://boto3.readthedocs.io/en/latest/guide/configuration.html
                pass

        return boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name), endpoint_url

    def get_client_type(self, client_type, region_name=None, config=None):
        """ Get the underlying boto3 client using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        return session.client(client_type, endpoint_url=endpoint_url,
                              config=config, verify=self.verify)

    def get_resource_type(self, resource_type, region_name=None, config=None):
        """ Get the underlying boto3 resource using boto3 session"""
        session, endpoint_url = self._get_credentials(region_name)

        return session.resource(resource_type, endpoint_url=endpoint_url,
                                config=config, verify=self.verify)

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
        if '/' in role:
            return role
        else:
            return self.get_client_type('iam').get_role(RoleName=role)['Role']['Arn']
