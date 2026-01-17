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

Human in the Loop (HITL) Operators
==================================

Human-in-the-Loop (HITL) operators enable workflows where task execution
can pause and wait for human input, approval, or intervention before
continuing.

These operators are part of the ``apache-airflow-providers-standard``
package and are intended for modeling approval steps or manual decision
points within automated pipelines.

Available HITL operators
------------------------

The Standard provider includes Human-in-the-Loop (HITL) operators.
For the complete and up-to-date list of available operators and their
parameters, refer to the Python API documentation:

`Python API reference <https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html>`_

Usage guide
-----------

For an end-to-end guide on using HITL operators, including examples and
recommended patterns, see the HITL tutorial:

* `HITL tutorial <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html>`_
