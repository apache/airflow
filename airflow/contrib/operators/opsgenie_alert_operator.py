# -*- coding: utf-8 -*-
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
#
from airflow.contrib.hooks.opsgenie_alert_hook import OpsgenieAlertHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class OpsgenieAlertOperator(BaseOperator):
    """
    This operator allows you to post alerts to Opsgenie.
    Accepts a connection that has an Opsgenie API key as the connection's password.
    This operator sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this operator.

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :type opsgenie_conn_id: str
    :param message: The Message of the Opsgenie alert (templated)
    :type message: str
    :param alias: Client-defined identifier of the alert (templated)
    :type alias: str
    :param description: Description field of the alert (templated)
    :type description: str
    :param responders: Teams, users, escalations and schedules that
        the alert will be routed to send notifications.
    :type responders: list[dict]
    :param visibleTo: Teams and users that the alert will become visible
        to without sending any notification.
    :type visibleTo: list[dict]
    :param actions: Custom actions that will be available for the alert.
    :type actions: list[str]
    :param tags: Tags of the alert.
    :type tags: list[str]
    :param details: Map of key-value pairs to use as custom properties of the alert.
    :type details: dict
    :param entity: Entity field of the alert that is
        generally used to specify which domain alert is related to. (templated)
    :type entity: str
    :param source: Source field of the alert. Default value is
        IP address of the incoming request.
    :type source: str
    :param priority: Priority level of the alert. Default value is P3. (templated)
    :type priority: str
    :param user: Display name of the request owner.
    :type user: str
    :param note: Additional note that will be added while creating the alert. (templated)
    :type note: str
    """
    template_fields = ('message', 'alias', 'description', 'entity', 'priority', 'note')

    @apply_defaults
    def __init__(self,
                 message,
                 opsgenie_conn_id='opsgenie_default',
                 alias=None,
                 description=None,
                 responders=None,
                 visibleTo=None,
                 actions=None,
                 tags=None,
                 details=None,
                 entity=None,
                 source=None,
                 priority=None,
                 user=None,
                 note=None,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)

        self.message = message
        self.opsgenie_conn_id = opsgenie_conn_id
        self.alias = alias
        self.description = description
        self.responders = responders
        self.visibleTo = visibleTo
        self.actions = actions
        self.tags = tags
        self.details = details
        self.entity = entity
        self.source = source
        self.priority = priority
        self.user = user
        self.note = note
        self.hook = None

    def _build_opsgenie_payload(self):
        """
        Construct the Opsgenie JSON payload. All relevant parameters are combined here
        to a valid Opsgenie JSON payload.

        :return: Opsgenie payload (dict) to send
        """
        payload = {}

        for key in [
            "message", "alias", "description", "responders",
            "visibleTo", "actions", "tags", "details", "entity",
            "source", "priority", "user", "note"
        ]:
            val = getattr(self, key)
            if val:
                payload[key] = val
        return payload

    def execute(self, context):
        """
        Call the OpsgenieAlertHook to post message
        """
        self.hook = OpsgenieAlertHook(self.opsgenie_conn_id)
        self.hook.execute(self._build_opsgenie_payload())
