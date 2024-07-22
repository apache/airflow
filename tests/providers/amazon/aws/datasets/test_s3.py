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

from airflow.datasets import Dataset
from airflow.providers.amazon.aws.datasets.s3 import create_dataset


def test_create_dataset():
    assert create_dataset(bucket="test-bucket", key="test-path") == Dataset(uri="s3://test-bucket/test-path")
    assert create_dataset(bucket="test-bucket", key="test-dir/test-path") == Dataset(
        uri="s3://test-bucket/test-dir/test-path"
    )
