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

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator


class TestConnectionOperator(BaseOperator):
    def __init__(self, conn_id: str, *args, **kwargs):
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = BaseHook.get_hook(conn_id=self.conn_id)
        if test_conn := getattr(hook, "test_connection"):
            (is_success, status) = test_conn()
            if not is_success:
                raise RuntimeError(status)
            return status
        raise RuntimeError(f"Connection {self.conn_id} does not support test_connection method")
