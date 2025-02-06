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

:py:mod:`airflow.providers.amazon.aws.notifications.chime`
==========================================================

.. py:module:: airflow.providers.amazon.aws.notifications.chime


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.chime.ChimeNotifier




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.chime.send_chime_notification


.. py:class:: ChimeNotifier(*, chime_conn_id, message = 'This is the default chime notifier message')


   Bases: :py:obj:`airflow.notifications.basenotifier.BaseNotifier`

   Chime notifier to send messages to a chime room via callbacks.

   :param: chime_conn_id: The chime connection to use with Endpoint as "https://hooks.chime.aws" and
                          the webhook token in the form of ```{webhook.id}?token{webhook.token}```
   :param: message: The message to send to the chime room associated with the webhook.


   .. py:attribute:: template_fields
      :value: ('message',)



   .. py:method:: hook()

      To reduce overhead cache the hook for the notifier.


   .. py:method:: notify(context)

      Send a message to a Chime Chat Room.



.. py:data:: send_chime_notification
