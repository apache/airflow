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

from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


class TestRedshiftDataHook:
    def test_conn_attribute(self):
        hook = RedshiftDataHook(aws_conn_id='aws_default', region_name='us-east-1')
        assert hasattr(hook, 'conn')
        assert hook.conn.__class__.__name__ == 'RedshiftDataAPIService'
        conn = hook.conn
        assert conn is hook.conn  # Cached property
        assert conn is hook.get_conn()  # Same object as returned by `conn` property
