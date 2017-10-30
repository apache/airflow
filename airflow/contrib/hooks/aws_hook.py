# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import boto3
import configparser

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
    Config = configparser.ConfigParser()
    if Config.read(config_file_name):  # pragma: no cover
        sections = Config.sections()
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
            access_key = Config.get(cred_section, key_id_option)
            secret_key = Config.get(cred_section, secret_key_option)
        except:
            logging.warning("Option Error in parsing s3 config file")
            raise
        return (access_key, secret_key)


class AwsHook(BaseHook):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.
    """

    def __init__(self, aws_conn_id='aws_default'):
        self.aws_conn_id = aws_conn_id

    def _get_credentials(self, region_name):
        aws_access_key_id = None
        aws_secret_access_key = None
        s3_endpoint_url = None

        if self.aws_conn_id:
            try:
                connection_object = self.get_connection(self.aws_conn_id)
                if connection_object.login:
                    aws_access_key_id = connection_object.login
                    aws_secret_access_key = connection_object.password

                elif 'aws_secret_access_key' in connection_object.extra_dejson:
                    aws_access_key_id = connection_object.extra_dejson['aws_access_key_id']
                    aws_secret_access_key = connection_object.extra_dejson['aws_secret_access_key']

                elif 's3_config_file' in connection_object.extra_dejson:
                    aws_access_key_id, aws_secret_access_key = \
                        _parse_s3_config(connection_object.extra_dejson['s3_config_file'],
                                         connection_object.extra_dejson.get('s3_config_format'))

                if region_name is None:
                    region_name = connection_object.extra_dejson.get('region_name')

                s3_endpoint_url = connection_object.extra_dejson.get('host')

            except AirflowException:
                # No connection found: fallback on boto3 credential strategy
                # http://boto3.readthedocs.io/en/latest/guide/configuration.html
                pass

        return aws_access_key_id, aws_secret_access_key, region_name, s3_endpoint_url

    def get_client_type(self, client_type, region_name=None):
        aws_access_key_id, aws_secret_access_key, region_name, endpoint_url = \
            self._get_credentials(region_name)

        return boto3.client(
            client_type,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )

    def get_resource_type(self, resource_type, region_name=None):
        aws_access_key_id, aws_secret_access_key, region_name, endpoint_url = \
            self._get_credentials(region_name)

        return boto3.resource(
            resource_type,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )
