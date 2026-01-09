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

Creating a Notifier
===================

The :class:`~airflow.sdk.definitions.notifier.BaseNotifier` is an abstract class that provides a basic
structure for sending notifications in Airflow using the various ``on_*__callback``.
It is intended for providers to extend and customize for their specific needs.

To extend the BaseNotifier class, you will need to create a new class that inherits from it. In this new class,
you should override the ``notify`` method with your own implementation that sends the notification. The ``notify``
method takes in a single parameter, the Airflow context, which contains information about the current task and execution.

You can also set the ``template_fields`` attribute to specify which attributes should be rendered as templates.

Here's an example of how you can create a Notifier class:

.. code-block:: python

    from airflow.sdk import BaseNotifier
    from my_provider import async_send_message, send_message


    class MyNotifier(BaseNotifier):
        template_fields = ("message",)

        def __init__(self, message: str):
            self.message = message

        def notify(self, context: Context) -> None:
            # Send notification here. For example:
            title = f"Task {context['task_instance'].task_id} failed"
            send_message(title, self.message)

        async def async_notify(self, context: Context) -> None:
            # Only required if your Notifier is going to support asynchronous code. For example:
            title = f"Task {context['task_instance'].task_id} failed"
            await async_send_message(title, self.message)


For a list of community-managed notifiers, see :doc:`apache-airflow-providers:core-extensions/notifications`.

Using Notifiers
===============

For using Notifiers in event-based Dag callbacks, see :doc:`../administration-and-deployment/logging-monitoring/callbacks`.
