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

Creating a notifier
===================
The BaseNotifier (:class:`airflow.notifications.basenotifier.BaseNotifier`) is an abstract class that provides a basic
structure for sending notifications in Airflow using the various ``on_*__callback``.
It is intended for providers to extend and customize for their specific needs.

To extend the BaseNotifier class, you will need to create a new class that inherits from it. In this new class,
you should override the ``notify`` method with your own implementation that sends the notification. The ``notify``
method takes in a single parameter, the Airflow context, which contains information about the current task and execution.

You can also set the ``template_fields`` attribute to specify which attributes should be rendered as templates.

Once you have a notifier implementation, you can use it in your ``DAG`` definition by passing it as an argument to
the ``on_*_callbacks``. For example, you can use it with ``on_success_callback`` or ``on_failure_callback`` to send
notifications based on the status of a task or DAG.

Here's an example of extending the BaseNotifier: SlackNotifier (:class:`airflow.providers.slack.notifications.slack_notifier.SlackNotifier`)
