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

import boto3
from botocore.waiter import Waiter, WaiterModel, create_waiter_with_client


class BaseBotoWaiter:
    """
    Used to create custom Boto3 Waiters.

    For more details, see airflow/providers/amazon/aws/waiters/README.md
    """

    def __init__(self, client: boto3.client, model_config: dict) -> None:
        self.model = WaiterModel(model_config)
        self.client = client

    def waiter(self, waiter_name: str) -> Waiter:
        return create_waiter_with_client(waiter_name=waiter_name, waiter_model=self.model, client=self.client)
