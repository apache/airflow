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



.. _howto/connection:pagerduty:

PagerDuty Connection
====================

The PagerDuty connection type enables connection to PagerDuty API.

Default Connection IDs
----------------------

PagerDuty Hook uses parameter ``pagerduty_conn_id`` for Connection IDs and the value of the
parameter as ``pagerduty_default`` by default.

Configuring the Connection
--------------------------

Pagerduty API token
    The API token that will be used for authentication against the PagerDuty API.

Pagerduty Routing key (Integration key)
    The routing key (also known as Integration key) that will be used to route an event to the corresponding
    PagerDuty service or rule.

.. note::
    The Pagerduty Routing key is deprecated.
    Please use the :ref:`PagerDutyEvents connection <howto/connection:pagerduty-events>` instead.
