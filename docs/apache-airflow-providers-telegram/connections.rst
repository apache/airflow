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

.. _howto/connection:telegram:

Connecting to Telegram
======================

The Telegram connection type enable connection to Telegram that allows you to post messages to Telegram using the telegram python-telegram-bot library.

After installing the Telegram provider in your Airflow environment, the corresponding connection type of ``telegram`` will be made available.

Default Connection IDs
----------------------

Telegram Hook uses the parameter ``telegram_conn_id`` for Connection IDs and the value of the parameter as ``telegram_default`` by default. You can create multiple connections in case you want to switch between environments.

Configuring the Connection
--------------------------

Password (optional)
    The Telegram API token

Host (optional)
    The Chat ID of the telegram chat/channel/group
