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

import json
from functools import cached_property
from typing import Any, Optional

from airflow.process_state import AbstractProcessStateBackend
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class AwsS3ProcessStateBackend(AbstractProcessStateBackend):
    """Class for interacting with S3 as a store of state for airflow processes."""

    suffix = '.txt'

    def __init__(
        self,
        bucket_name: str,
        prefix: str = '/airflow/process-state',
        default_value: Optional[Any] = None,
        **kwargs,
    ):
        self.prefix = prefix
        self.default_value = default_value
        self.kwargs = kwargs
        self.bucket_name = bucket_name

    @cached_property
    def s3_hook(self):
        return S3Hook(**self.kwargs)

    @cached_property
    def client(self):
        return self.s3_hook.get_conn()

    def _build_path(self, namespace, process_name):
        if not (namespace.islower() and process_name.islower()):
            raise ValueError(f"`namespace` and `process_name` must be lowercase")
        return f"{self.prefix}/{namespace}/{process_name}{self.suffix}"

    def get_state(self, process_name: str, namespace: str = 'default') -> Optional[Any]:
        try:
            value = self.s3_hook.read_key(
                bucket_name=self.bucket_name, key=self._build_path(namespace, process_name)
            )
            return json.loads(value)
        except self.client.exceptions.ClientError:
            return self.default_value

    def set_state(self, process_name, value, namespace='default'):
        self.s3_hook.load_string(
            bucket_name=self.bucket_name,
            string_data=json.dumps(value),
            key=self._build_path(namespace=namespace, process_name=process_name),
            replace=True,
        )

    def delete(self, process_name: str, namespace: str = 'default'):
        self.s3_hook.delete_objects(
            bucket=self.bucket_name, keys=[self._build_path(namespace=namespace, process_name=process_name)]
        )
