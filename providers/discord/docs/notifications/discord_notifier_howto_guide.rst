 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

How-to Guide for Discord notifications
========================================

Introduction
------------
Discord notifier (:class:`airflow.providers.discord.notifications.discord.DiscordNotifier`) allows users to send
messages to a discord channel using the various ``on_*_callbacks`` at both the Dag level and Task level


Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.discord.notifications.embed import Embed, EmbedAuthor, EmbedFooter, EmbedField
    from airflow.providers.discord.notifications.discord import DiscordNotifier

    with DAG(
        start_date=datetime(2025, 1, 1),
        on_success_callback=[
            DiscordNotifier(
                text="The Dag {{ dag.dag_id }} succeeded",
            )
        ],
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                DiscordNotifier(
                    text="The task {{ ti.task_id }} failed",
                    embed=Embed(
                        title="Hello ~~people~~ world :wave: The task {{ ti.task_id }} failed",
                        description="You can use [links](https://discord.com) or emojis :smile: 😎\n```\nAnd also code blocks\n```",
                        color=4321431,
                        timestamp="2025-11-23T19:05:15.292Z",
                        url="https://discord.com",
                        author=EmbedAuthor(
                            name="Author",
                            url="https://discord.com",
                            icon_url="https://cdn.discordapp.com/embed/avatars/0.png",
                        ),
                        footer=EmbedFooter(
                            text="Footer text", icon_url="https://cdn.discordapp.com/embed/avatars/0.png"
                        ),
                        fields=[EmbedField(name="Field 1, *lorem* **ipsum**, ~~dolor~~", value="Field value")],
                    ),
                )
            ],
            bash_command="fail",
        )
