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

Google Cloud Messaging Queues
==============================

Google Cloud Pub/Sub Queue Provider
------------------------------------

Implemented by :class:`~airflow.providers.google.cloud.queues.pubsub.PubsubMessageQueueProvider`

The Google Cloud Pub/Sub Queue Provider is a message queue provider that uses Google Cloud Pub/Sub as the underlying message queue system.

The queue should match this regex:

.. exampleinclude:: /../src/airflow/providers/google/cloud/queues/pubsub.py
    :language: python
    :dedent: 0
    :start-after: [START queue_regexp]
    :end-before: [END queue_regexp]

Queue URI Format:

.. code-block:: text

    projects/{project_id}/subscriptions/{subscription_name}

Where:

- ``project_id``: Google Cloud project ID where the Pub/Sub subscription exists
- ``subscription_name``: Name of the Pub/Sub subscription to consume messages from

.. note::
    The subscription name must follow Google Cloud naming conventions:
    - Must start with a letter
    - Can contain letters, numbers, hyphens, underscores, tildes, plus signs, and percent signs
    - Must be between 3 and 255 characters long
    - Cannot start with `goog`

The queue parameter is used to configure the underlying
:class:`~airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger` class.
The provider extracts the ``project_id`` and ``subscription`` from the queue URI format
and passes them along with additional configuration to the trigger constructor.

.. warning::
    Do not provide ``project_id`` or ``subscription`` in kwargs as they are extracted
    from the queue URI format and will raise a ValueError if provided.
