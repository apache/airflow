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

from airflow.api_fastapi.auth.managers.models.system_user import SystemUser


class TestSystemUser:
    def test_get_id(self):
        user = SystemUser(process_type="dag_processor")
        assert user.get_id() == "airflow:process:dag_processor"

    def test_get_name(self):
        user = SystemUser(process_type="scheduler")
        assert user.get_name() == "System (scheduler)"

    def test_different_process_types(self):
        for process_type in ("dag_processor", "scheduler", "worker", "triggerer"):
            user = SystemUser(process_type=process_type)
            assert process_type in user.get_id()
            assert process_type in user.get_name()
