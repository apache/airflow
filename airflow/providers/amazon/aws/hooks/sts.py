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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StsHook(AwsBaseHook):
    """
    Interact with AWS Security Token Service (STS)

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.
    .. seealso::
    :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="sts", *args, **kwargs)

    def get_account_number(self) -> str:
        """Get the account Number"""
        try:
            return self.get_conn().get_caller_identity()['Account']
        except Exception as general_error:
            self.log.error("Failed to get the AWS Account Number, error: %s", general_error)
            raise
