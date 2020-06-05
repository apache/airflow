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

  * Add custom robot to Dingding group which you want to send Dingding message.
  * Get the webhook token from Dingding custom robot.
  * Put the Dingding custom robot token in the password field of the ``dingding_default``
    Connection. Notice that you just need token rather than the whole webhook string.

Basic Usage
^^^^^^^^^^^

Use the :class:`~airflow.providers.dingding.operators.dingding.DingdingOperator`
to send Dingding message:

.. exampleinclude:: ../../../airflow/providers/dingding/example_dags/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding]
    :end-before: [END howto_operator_dingding]


Remind users in message
^^^^^^^^^^^^^^^^^^^^^^^

Use parameters ``at_mobiles`` and ``at_all`` to remind specific users when you send message,
``at_mobiles`` will be ignored When ``at_all`` is set to ``True``:

.. exampleinclude:: ../../../airflow/providers/dingding/example_dags/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding_remind_users]
    :end-before: [END howto_operator_dingding_remind_users]


Send rich text message
^^^^^^^^^^^^^^^^^^^^^^

The Dingding operator can send rich text messages including link, markdown, actionCard and feedCard.
A rich text message can not remind specific users except by using markdown type message:

.. exampleinclude:: ../../../airflow/providers/dingding/example_dags/example_dingding.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dingding_rich_text]
    :end-before: [END howto_operator_dingding_rich_text]


Sending messages from a Task callback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dingding operator could handle task callback by writing a function wrapper dingding operators
and then pass the function to ``sla_miss_callback``, ``on_success_callback``, ``on_failure_callback``,
or ``on_retry_callback``. Here we use ``on_failure_callback`` as an example:

.. exampleinclude:: ../../../airflow/providers/dingding/example_dags/example_dingding.py
    :language: python
    :start-after: [START howto_operator_dingding_failure_callback]
    :end-before: [END howto_operator_dingding_failure_callback]


Changing connection host if you need
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Dingding operator post http requests using default host ``https://oapi.dingtalk.com``,
if you need to change the host used you can set the host field of the connection.


More information
^^^^^^^^^^^^^^^^

See Dingding documentation on how to `custom robot
<https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq>`_.
