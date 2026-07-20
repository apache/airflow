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

from unittest import mock

from airflow.providers.amazon.aws.hooks.s3_tables import S3TablesHook


class TestS3TablesHook:
    def test_client_type(self):
        hook = S3TablesHook()
        assert hook.client_type == "s3tables"

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_get_table_bucket_arn_by_name_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "tableBuckets": [
                    {"name": "my-bucket", "arn": "arn:aws:s3tables:us-east-1:123:bucket/my-bucket"}
                ]
            }
        ]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = S3TablesHook()
        assert (
            hook.get_table_bucket_arn_by_name("my-bucket")
            == "arn:aws:s3tables:us-east-1:123:bucket/my-bucket"
        )

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_get_table_bucket_arn_by_name_not_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"tableBuckets": []}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = S3TablesHook()
        assert hook.get_table_bucket_arn_by_name("nonexistent") is None
