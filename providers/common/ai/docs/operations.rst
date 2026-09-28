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

.. _howto/operations:

Reliability and operations
==========================

An AI task fails in more ways than a SQL task, and it costs money each time it runs.

- :doc:`durable_execution` replays the model and tool calls an agent already completed, so a
  retry pays only for the steps that did not finish.
- :doc:`retry_policies` lets a model classify a failure and decide whether a retry is worth
  it at all, with a plain rule table as the floor.
- :doc:`observability` exports model and tool calls as OpenTelemetry traces and metrics.
- :doc:`agent_security` is the defense-layer guide for agents that hold tools: what an agent
  can reach, how ``allowed_tables`` is enforced, and the production checklist.
- :doc:`provider_fallback` fails over to another vendor inside one task attempt when a
  model provider is down or rate limiting. It is configured on the connection, so it also
  appears under :doc:`model_providers`.
- :doc:`troubleshooting` lists the errors a first Dag most often hits, with the fix for each.

.. toctree::
    :hidden:
    :titlesonly:

    Durable execution <durable_execution>
    Retry policies <retry_policies>
    Observability <observability>
    Securing agent tools <agent_security>
    Troubleshooting <troubleshooting>
