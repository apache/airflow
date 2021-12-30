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

# This stub is to handle `default_args` use for these operators in the example DAGs. Mypy is native in
# validating how required arguments can be passed to operators/sensors via `default_args`.

from typing import Optional

from airflow.models import BaseOperator

class OSSCreateBucketOperator(BaseOperator):
    def __init__(
        self,
        region: Optional[str] = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteBucketOperator(BaseOperator):
    def __init__(
        self,
        region: Optional[str] = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSUploadObjectOperator(BaseOperator):
    def __init__(
        self,
        key: Optional[str] = ...,
        file: Optional[str] = ...,
        region: Optional[str] = None,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDownloadObjectOperator(BaseOperator):
    def __init__(
        self,
        key: Optional[str] = ...,
        file: Optional[str] = ...,
        region: Optional[str] = None,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteBatchObjectOperator(BaseOperator):
    def __init__(
        self,
        keys: Optional[list] = ...,
        region: Optional[str] = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteObjectOperator(BaseOperator):
    def __init__(
        self,
        key: Optional[str] = ...,
        region: Optional[str] = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...
