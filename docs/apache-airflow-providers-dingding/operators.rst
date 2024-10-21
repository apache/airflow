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



Dingding Operators
==================


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this operators, you must do a few things:
  * Add custom robot to chat group group which you want to send a message.
  * Get `DingTalk Custom Robot webhook token <https://open.dingtalk.com/document/robots/custom-robot-access>`__.
  * Put the access token in the password field of the ``dingding_default`` Connection.
    Note: You need only token value rather than the whole webhook string.

.. _howto/operator:DingdingOperator:

Basic Usage
^^^^^^^^^^^

Use the :class:`~airflow.providers.dingding.operators.dingding.DingdingOperator`
to send message through `DingTalk Custom Robot <https://open.dingtalk.com/document/robots/custom-robot-access>`__:

.. exampleinclude:: /../../providers/tests/system/dingding/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding]
    :end-before: [END howto_operator_dingding]


Remind users in message
^^^^^^^^^^^^^^^^^^^^^^^

Use parameters ``at_mobiles`` and ``at_all`` to remind specific users when you send message,
``at_mobiles`` will be ignored When ``at_all`` is set to ``True``:

.. exampleinclude:: /../../providers/tests/system/dingding/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding_remind_users]
    :end-before: [END howto_operator_dingding_remind_users]


Send rich text message
^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.dingding.operators.dingding.DingdingOperator`
can send rich text messages including link, markdown, actionCard and feedCard
through `DingTalk Custom Robot <https://open.dingtalk.com/document/robots/custom-robot-access#title-72m-8ag-pqw>`__.
A rich text message can not remind specific users except by using markdown type message:

.. exampleinclude:: /../../providers/tests/system/dingding/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding_rich_text]
    :end-before: [END howto_operator_dingding_rich_text]


Sending messages from a Task callback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.dingding.hooks.dingding.DingdingHook` could handle task callback by writing a callback function
and then pass the function to ``sla_miss_callback``, ``on_success_callback``, ``on_failure_callback``,
or ``on_retry_callback``. Here we use ``on_failure_callback`` as an example:

.. exampleinclude:: /../../providers/tests/system/dingding/example_dingding.py
    :language: python
    :start-after: [START howto_operator_dingding_failure_callback]
    :end-before: [END howto_operator_dingding_failure_callback]


Changing connection host if you need
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.dingding.operators.dingding.DingdingOperator` operator
post http requests using default host ``https://oapi.dingtalk.com``,
if you need to change the host used you can set the host field of the connection.
