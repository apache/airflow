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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit

from deprecated.classic import deprecated

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OSSKeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on OSS) to be present in an OSS bucket.

    OSS being a key/value, it does not support folders. The path is just a key resource.

    :param bucket_key: The key being waited on. Supports full oss:// style url
        or relative path from root level. When it's specified as a full oss://
        url, please leave bucket_name as `None`.
    :param region: OSS region
    :param bucket_name: OSS bucket name
    :param oss_conn_id: The Airflow connection used for OSS credentials.
    """

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        bucket_key: str,
        region: str,
        bucket_name: str | None = None,
        oss_conn_id: str | None = "oss_default",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.region = region
        self.oss_conn_id = oss_conn_id

    def poke(self, context: Context):
        """
        Check if the object exists in the bucket to pull key.

        :param self: the object itself
        :param context: the context of the object
        :returns: True if the object exists, False otherwise
        """
        parsed_url = urlsplit(self.bucket_key)
        if self.bucket_name is None:
            if parsed_url.netloc == "":
                message = (
                    "If key is a relative path from root, please provide a bucket_name"
                )
                raise AirflowException(message)
            self.bucket_name = parsed_url.netloc
            self.bucket_key = parsed_url.path.lstrip("/")
        else:
            if parsed_url.scheme != "" or parsed_url.netloc != "":
                message = (
                    "If bucket_name is provided, bucket_key"
                    " should be relative path from root"
                    " level, rather than a full oss:// url"
                )
                raise AirflowException(message)

        self.log.info("Poking for key : oss://%s/%s", self.bucket_name, self.bucket_key)
        return self.hook.object_exists(key=self.bucket_key, bucket_name=self.bucket_name)

    @property
    @deprecated(
        reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning
    )
    def get_hook(self) -> OSSHook:
        """Create and return an OSSHook."""
        return self.hook

    @cached_property
    def hook(self) -> OSSHook:
        """Create and return an OSSHook."""
        return OSSHook(oss_conn_id=self.oss_conn_id, region=self.region)
