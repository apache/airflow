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

:py:mod:`airflow.providers.amazon.aws.hooks.chime`
==================================================

.. py:module:: airflow.providers.amazon.aws.hooks.chime

.. autoapi-nested-parse::

   This module contains a web hook for Chime.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.chime.ChimeWebhookHook




.. py:class:: ChimeWebhookHook(chime_conn_id, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.http.hooks.http.HttpHook`

   Interact with Amazon Chime Webhooks to create notifications.

   .. warning:: This hook is only designed to work with web hooks and not chat bots.

   :param chime_conn_id: :ref:`Amazon Chime Connection ID <howto/connection:chime>`
       with Endpoint as `https://hooks.chime.aws` and the webhook token
       in the form of ``{webhook.id}?token{webhook.token}``

   .. py:attribute:: conn_name_attr
      :value: 'chime_conn_id'



   .. py:attribute:: default_conn_name
      :value: 'chime_default'



   .. py:attribute:: conn_type
      :value: 'chime'



   .. py:attribute:: hook_name
      :value: 'Amazon Chime Webhook'



   .. py:method:: webhook_endpoint()


   .. py:method:: send_message(message)

      Execute calling the Chime webhook endpoint.

      :param message: The message you want to send to your Chime room.(max 4096 characters)


   .. py:method:: get_ui_field_behaviour()
      :classmethod:

      Return custom field behaviour to only get what is needed for Chime webhooks to function.
