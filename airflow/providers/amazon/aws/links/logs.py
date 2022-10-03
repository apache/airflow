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

from urllib.parse import quote_plus

from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink


class CloudWatchEventsLink(BaseAwsLink):
    """Helper class for constructing AWS CloudWatch Events Link"""

    name = "CloudWatch Events"
    key = "cloudwatch_events"
    format_str = (
        BASE_AWS_CONSOLE_LINK
        + "/cloudwatch/home?region={awslogs_region}#logsV2:log-groups/log-group/{awslogs_group}"
        + "/log-events/{awslogs_stream_name}"
    )

    def format_link(self, **kwargs) -> str:
        for field in ("awslogs_stream_name", "awslogs_group"):
            if field in kwargs:
                kwargs[field] = quote_plus(kwargs[field])
            else:
                return ""

        return super().format_link(**kwargs)
