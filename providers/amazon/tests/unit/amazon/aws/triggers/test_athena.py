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

from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger


class TestAthenaTrigger:
    def test_serialization(self):
        query_execution_id = "test_query_execution_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = AthenaTrigger(
            query_execution_id=query_execution_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.athena.AthenaTrigger"
        assert kwargs == {
            "query_execution_id": "test_query_execution_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }
