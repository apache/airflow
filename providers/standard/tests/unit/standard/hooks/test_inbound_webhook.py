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

from airflow.providers.standard.hooks.inbound_webhook import InboundWebhookHook


def test_registers_the_inbound_webhook_connection_type():
    assert (
        InboundWebhookHook.conn_type,
        InboundWebhookHook.hook_name,
        InboundWebhookHook.default_conn_name,
    ) == ("inbound_webhook", "Inbound Webhook", "webhook_default")


def test_accepts_the_connection_id_that_get_hook_passes():
    assert InboundWebhookHook(webhook_conn_id="webhook_x").webhook_conn_id == "webhook_x"
