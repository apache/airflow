
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

Messaging Triggers
==================

Operators, sensors, and triggers in the Common Messaging provider serve as wrappers around those
from other providers, specifically for message queues.
They offer an abstraction layer to simplify usage and make it easier to switch between
different queue providers.

Add support for a provider
~~~~~~~~~~~~~~~~~~~~~~~~~~

To support a new provider please follow the steps below:

1. Create a new class in the provider extending
:class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider`.
Make sure it implements all abstract methods

2. Expose it via "queues" property in the ``provider.yaml`` file of the provider where you add the new class.
The ``queues`` property should be a list of fully-qualified class names of the queues.


The list of supported message queues is available in :doc:`apache-airflow-providers:core-extensions/message-queues`.
