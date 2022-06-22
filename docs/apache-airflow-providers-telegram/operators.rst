
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



.. _howto/operator:TelegramOperator:

TelegramOperator
================

Use the :class:`~airflow.providers.telegram.operators.telegram.TelegramOperator`
to send message to a `Telegram <https://telegram.org/>`__ group or a specific chat.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``telegram_conn_id`` argument to connect to Telegram client where
the connection metadata is structured as follows:

.. list-table:: Telegram Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Password: string
     - Telegram Bot API Token
   * - Host: string
     - Chat ID of the Telegram group/chat
   * - Connection Type: string
     - http as connection type

An example usage of the TelegramOperator is as follows:

.. exampleinclude:: /../../airflow/providers/telegram/example_dags/example_telegram.py
    :language: python
    :start-after: [START howto_operator_telegram]
    :end-before: [END howto_operator_telegram]

.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``Host``, ``Password`` and so forth).
