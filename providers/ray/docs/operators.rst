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

Ray operators and decorator
===========================

The provider supplies ``SetupRayCluster``, ``SubmitRayJob``, and ``DeleteRayCluster`` for explicit
cluster and job lifecycle management. It also supplies ``@ray.task`` for defining a Python callable
that is packaged and submitted as a Ray job.

See the system-test Dags in ``providers/ray/tests/system/ray`` for complete Airflow 3 examples.
