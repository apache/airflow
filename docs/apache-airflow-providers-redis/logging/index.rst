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

.. _write-logs-redis:

Writing logs to Redis
---------------------

Airflow can be configured to store log lines in Redis up to a configured maximum log lines, always keeping the most recent, up to a configured TTL. This deviates from other existing task handlers in that it accepts a connection ID.
This allows it to be used in addition to other handlers, and so allows a graceful/reversible transition from one logging system to another. This is particularly useful in situations that use Redis as a message broker, where additional infrastructure isn't desired.
