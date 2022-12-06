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

from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook

task_id = "test_emr_serverless_create_application_operator"
application_id = "test_application_id"
release_label = "test"
job_type = "test"
client_request_token = "eac427d0-1c6d4df=-96aa-32423412"
config = {"name": "test_application_emr_serverless"}


class TestEmrServerlessHook:
    def test_conn_attribute(self):
        hook = EmrServerlessHook(aws_conn_id="aws_default")
        assert hasattr(hook, "conn")
        # Testing conn is a cached property
        conn = hook.conn
        conn2 = hook.conn
        assert conn is conn2
