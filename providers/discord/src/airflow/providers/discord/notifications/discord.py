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

from functools import cached_property
from typing import Any
from urllib.parse import urlencode

from airflow.providers.common.compat.notifier import BaseNotifier  # type: ignore
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.utils.timezone import utcnow  # type: ignore

ICON_URL: str = (
    "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/src/airflow/ui/public/pin_100.png"
)


class DiscordNotifier(BaseNotifier):
    """
    Discord BaseNotifier.

    :param discord_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
    :param text: The content of the message
    :param username: The username to send the message as. Optional
    :param avatar_url: The URL of the avatar to use for the message. Optional
    :param tts: Text to speech.
    """

    # A property that specifies the attributes that can be templated.
    template_fields = ("discord_conn_id", "text", "username", "avatar_url", "tts")

    def __init__(
        self,
        discord_conn_id: str = "discord_webhook_default",
        text: str | None = None,
        username: str | None = "Airflow",
        avatar_url: str | None = ICON_URL,
        tts: bool = False,
        success: bool | None = False,
        failure: bool | None = False,
        success_color: int = 0x00FF00,
        failure_color: int = 0xFF0000,
        mentions: list[dict[str, Any]] | None = None,
        mention_everyone: bool = False,
        mention_roles: list[str] | None = None,
        mention_channels: list[dict[str, Any]] | None = None,
        reactions: list[dict[str, Any]] | None = None,
        application_id: str = "airflow",
        description: str = "",
    ):
        super().__init__()
        self.discord_conn_id = discord_conn_id
        self.text = text
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.success = success or not failure
        self.success_color = success_color
        self.failure_color = failure_color
        self.mentions = mentions
        self.mention_everyone = mention_everyone
        self.mention_roles = mention_roles
        self.mention_channels = mention_channels
        self.application_id = application_id
        self.dag_name = None
        self.task_name = None
        self.duration = None
        self.failure_link = None
        self.success_link = None
        self.reactions = None
        self.proxy = None
        self.embeds: list[dict[str, Any]] | None = None
        self.description = description

    @cached_property
    def hook(self) -> DiscordWebhookHook:
        """Discord Webhook Hook."""
        hook = DiscordWebhookHook(
            http_conn_id=self.discord_conn_id,
            webhook_endpoint=None,
            message=self.text or "",
            username=self.username,
            avatar_url=self.avatar_url,
            tts=self.tts,
            proxy=None,
        )
        if not hook.message:
            self.embeds = self._build_embed()
        hook.embeds = self.embeds
        hook.mentions = self.mentions
        hook.mention_everyone = self.mention_everyone
        hook.mention_roles = self.mention_roles
        hook.mention_channels = self.mention_channels
        hook.reactions = self.reactions
        hook.application_id = self.application_id
        return hook

    def _build_embed(self) -> list[dict[str, Any]]:
        """Build the embed structure for the Discord message."""
        color = self.success_color if self.success else self.failure_color
        title = f"DAG run: {self.dag_name}" if self.success else f"DAG failure: {self.dag_name}"
        description = self.description

        embed = {
            "title": title,
            "description": description,
            "color": color,
            "fields": [
                {
                    "name": "Status",
                    "value": "✅ Success" if self.success else "❌ Failed",
                    "inline": True,
                },
                {"name": "Duration", "value": self.duration, "inline": True},
                {"name": "DAG", "value": self.dag_name, "inline": True},
            ],
            "footer": {"text": f"Task: {self.task_name}"},
            "timestamp": utcnow().isoformat(),
        }

        if self.success_link:
            embed["fields"].append(
                {
                    "name": "Link",
                    "value": f"[View DAG]({self.success_link})",
                    "inline": False,
                }
            )
        if self.failure_link:
            embed["fields"].append(
                {
                    "name": "Link",
                    "value": f"[View Logs]({self.failure_link})",
                    "inline": False,
                }
            )

        return [embed]

    def notify(self, context):
        """Send a message to a Discord channel."""
        task_instance = context.get("task_instance")
        if task_instance:
            self.dag_name = task_instance.dag_id.replace("_", "\\_")
            self.task_name = task_instance.task_id
            self.duration = str(task_instance.duration)
            self.failure_link = f"https://airflow.data.retize.io/dags/{task_instance.dag_id}/grid?task_id={task_instance.task_id}&tab=logs&dag_run_id={urlencode({'dag_run_id': task_instance.run_id.replace('+', '%2B')})}"
            self.success_link = f"https://airflow.data.retize.io/dags/{task_instance.dag_id}/grid?task_id={task_instance.task_id}&tab=logs&dag_run_id={urlencode({'dag_run_id': task_instance.run_id.replace('+', '%2B')})}"

        self.hook.username = self.username
        self.hook.avatar_url = self.avatar_url
        self.hook.tts = self.tts

        if self.text:
            lines = self.text.split("\n")
            messages = []
            current_message = ""
            in_code_block = False

            for line in lines:
                if line.startswith("```"):
                    if in_code_block:
                        current_message += "\n```"
                        messages.append(current_message)
                        current_message = "```"
                    else:
                        current_message += "\n" + line
                    in_code_block = not in_code_block
                elif len(current_message) + len(line) + 1 > 2000:
                    if in_code_block:
                        current_message += "\n```"
                        messages.append(current_message)
                        current_message = "```" + line
                    else:
                        messages.append(current_message)
                        current_message = line
                else:
                    if current_message:
                        current_message += "\n" + line
                    else:
                        current_message = line

            if current_message:
                if in_code_block:
                    current_message += "\n```"
                messages.append(current_message)

            self.embeds = []
            for message in messages:
                self.hook.message = message
                self.hook.execute()
        else:
            self.hook.embeds = self._build_embed()
            self.hook.execute()
