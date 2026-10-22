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

from airflow.providers.common.compat.sdk import BaseHook


class InboundWebhookHook(BaseHook):
    """
    Registers the "Inbound Webhook" connection type.

    Webhook connections are read by the API server's webhooks app, so the hook has no client to create.
    """

    conn_name_attr = "webhook_conn_id"
    default_conn_name = "webhook_default"
    conn_type = "inbound_webhook"
    hook_name = "Inbound Webhook"

    def __init__(self, webhook_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.webhook_conn_id = webhook_conn_id
