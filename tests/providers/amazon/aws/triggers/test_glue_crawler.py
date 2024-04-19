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

from unittest.mock import patch

from airflow.providers.amazon.aws.triggers.glue_crawler import GlueCrawlerCompleteTrigger


class TestGlueCrawlerCompleteTrigger:
    @patch("airflow.providers.amazon.aws.triggers.glue_crawler.warnings.warn")
    def test_serialization(self, mock_warn):
        crawler_name = "test_crawler"
        poll_interval = 10
        aws_conn_id = "aws_default"

        trigger = GlueCrawlerCompleteTrigger(
            crawler_name=crawler_name,
            poll_interval=poll_interval,
            aws_conn_id=aws_conn_id,
        )

        assert mock_warn.call_count == 1
        args, kwargs = mock_warn.call_args
        assert args[0] == "please use waiter_delay instead of poll_interval."
        assert kwargs == {"stacklevel": 2}

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.glue_crawler.GlueCrawlerCompleteTrigger"
        assert kwargs == {
            "crawler_name": "test_crawler",
            "waiter_delay": 10,
            "waiter_max_attempts": 1500,
            "aws_conn_id": "aws_default",
        }
