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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

if TYPE_CHECKING:
    from mypy_boto3_verifiedpermissions.client import VerifiedPermissionsClient  # noqa


class VerifiedPermissionsHook(AwsGenericHook["VerifiedPermissionsClient"]):
    """
    Interact with Amazon Verified Permissions.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("verifiedpermissions")
    <VerifiedPermissions.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `Amazon Appflow API Reference <https://docs.aws.amazon.com/verifiedpermissions/latest/apireference/Welcome.html>`__
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "verifiedpermissions"
        super().__init__(*args, **kwargs)
