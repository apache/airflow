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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

if TYPE_CHECKING:
    from mypy_boto3_redshift_data import RedshiftDataAPIServiceClient  # noqa


class RedshiftDataHook(AwsGenericHook["RedshiftDataAPIServiceClient"]):
    """
    Interact with Amazon Redshift Data API.
    Provide thin wrapper around
    :external+boto3:py:class:`boto3.client("redshift-data") <RedshiftDataAPIService.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `Amazon Redshift Data API \
        <https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html>`__
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift-data"
        super().__init__(*args, **kwargs)
