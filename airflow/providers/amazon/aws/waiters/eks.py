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

from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from airflow.providers.amazon.aws.waiters.base_waiter import BaseBotoWaiter

eks_waiter_model_config = {
    "version": 2,
    "waiters": {
        "all_nodegroups_deleted": {
            "operation": "ListNodegroups",
            "delay": 30,
            "maxAttempts": 60,
            "acceptors": [
                {
                    "matcher": "path",
                    "argument": "length(nodegroups[]) == `0`",
                    "expected": True,
                    "state": "success",
                },
                {
                    "matcher": "path",
                    "expected": True,
                    "argument": "length(nodegroups[]) > `0`",
                    "state": "retry",
                },
            ],
        }
    },
}


class EksBotoWaiter(BaseBotoWaiter):
    """
    Custom EKS boto waiters.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.waiters.base_waiter.BaseBotoWaiter`
    """

    def __init__(self, client: BaseAwsConnection):
        super().__init__(client=client, model_config=eks_waiter_model_config)
