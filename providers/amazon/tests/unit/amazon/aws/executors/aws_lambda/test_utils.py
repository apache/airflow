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

from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    AllLambdaConfigKeys,
    InvokeLambdaKwargsConfigKeys,
)
from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys


class TestConfigKeys:
    def test_invoke_lambda_kwargs_config_keys(self):
        assert issubclass(InvokeLambdaKwargsConfigKeys, BaseConfigKeys)
        assert InvokeLambdaKwargsConfigKeys.FUNCTION_NAME == "function_name"
        assert InvokeLambdaKwargsConfigKeys.QUALIFIER == "function_qualifier"

    def test_all_lambda_config_keys_inheritance(self):
        assert issubclass(AllLambdaConfigKeys, InvokeLambdaKwargsConfigKeys)
        assert issubclass(AllLambdaConfigKeys, BaseConfigKeys)

    def test_all_lambda_config_keys_values(self):
        assert AllLambdaConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS == "max_invoke_attempts"
        assert AllLambdaConfigKeys.REGION_NAME == "region_name"
        assert AllLambdaConfigKeys.QUEUE_URL == "queue_url"
        assert AllLambdaConfigKeys.DLQ_URL == "dead_letter_queue_url"
        assert AllLambdaConfigKeys.END_WAIT_TIMEOUT == "end_wait_timeout"
        assert AllLambdaConfigKeys.FUNCTION_NAME == "function_name"
        assert AllLambdaConfigKeys.QUALIFIER == "function_qualifier"
