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

Webhook Sources
---------------

|experimental|

This is a summary of all Apache Airflow Community provided implementations of webhook source types
exposed via community-managed providers.

Airflow can be extended by providers with webhook source types. A webhook source type verifies and
parses the inbound webhook deliveries of one kind of sender, and is implemented by a subclass of
``airflow.webhooks.base.WebhookSource``. Each provider can declare its own source types under
``webhook-sources`` in its ``provider.yaml``.

A source type declared by more than one installed provider is served by none of them.
``airflow providers webhook-sources`` lists every declared source type and shows such conflicts.

These are the webhook source types provided by the community-managed providers:

.. airflow-webhook-sources::
   :tags: None
   :header-separator: "
