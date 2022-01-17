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

# This stub exists to work around false MyPY errors in examples due to default_args handling.
# The difference in the stub file vs. original class are Optional args which are passed
# by default_args.
#
# TODO: Remove this file once we implement a proper solution (MyPy plugin?) that will handle default_args.

from typing import Dict, List, Optional, Union

from airflow.models import BaseOperator

class S3CreateBucketOperator(BaseOperator):
    def __init__(
        self,
        *,
        bucket_name: Optional[str] = None,
        aws_conn_id: Optional[str] = "aws_default",
        region_name: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class S3DeleteBucketOperator(BaseOperator):
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        force_delete: bool = False,
        aws_conn_id: Optional[str] = "aws_default",
        **kwargs,
    ) -> None: ...

class S3GetBucketTaggingOperator(BaseOperator):
    def __init__(
        self, bucket_name: Optional[str] = None, aws_conn_id: Optional[str] = "aws_default", **kwargs
    ) -> None: ...

class S3PutBucketTaggingOperator(BaseOperator):
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        key: Optional[str] = None,
        value: Optional[str] = None,
        tag_set: Optional[List[Dict[str, str]]] = None,
        aws_conn_id: Optional[str] = "aws_default",
        **kwargs,
    ) -> None: ...

class S3DeleteBucketTaggingOperator(BaseOperator):
    def __init__(
        self, bucket_name: Optional[str] = None, aws_conn_id: Optional[str] = "aws_default", **kwargs
    ) -> None: ...

class S3CopyObjectOperator(BaseOperator):
    def __init__(
        self,
        *,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: Optional[str] = None,
        dest_bucket_name: Optional[str] = None,
        source_version_id: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        acl_policy: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class S3DeleteObjectsOperator(BaseOperator):
    def __init__(
        self,
        *,
        bucket: str,
        keys: Optional[Union[str, list]] = None,
        prefix: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ) -> None: ...
