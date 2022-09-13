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
from __future__ import annotations

import base64
import json

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SecretsManagerHook(AwsBaseHook):
    """
    Interact with Amazon SecretsManager Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='secretsmanager', *args, **kwargs)

    def get_secret(self, secret_name: str) -> str | bytes:
        """
        Retrieve secret value from AWS Secrets Manager as a str or bytes
        reflecting format it stored in the AWS Secrets Manager

        :param secret_name: name of the secrets.
        :return: Union[str, bytes] with the information about the secrets
        :rtype: Union[str, bytes]
        """
        # Depending on whether the secret is a string or binary, one of
        # these fields will be populated.
        get_secret_value_response = self.get_conn().get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret

    def get_secret_as_dict(self, secret_name: str) -> dict:
        """
        Retrieve secret value from AWS Secrets Manager in a dict representation

        :param secret_name: name of the secrets.
        :return: dict with the information about the secrets
        :rtype: dict
        """
        return json.loads(self.get_secret(secret_name))
