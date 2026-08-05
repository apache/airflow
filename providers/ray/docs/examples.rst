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

Examples
========

Submit to an existing Ray cluster
---------------------------------

The following Airflow 3 Dag uses ``@ray.task`` with a Ray dashboard URL supplied by the connection:

.. exampleinclude:: /../../ray/tests/system/ray/ray_taskflow_example_existing_cluster.py
   :language: python

Manage the full cluster lifecycle with a decorator
--------------------------------------------------

.. exampleinclude:: /../../ray/tests/system/ray/ray_taskflow_example.py
   :language: python

Submit a job with an operator
-----------------------------

.. exampleinclude:: /../../ray/tests/system/ray/ray_single_operator.py
   :language: python

Use explicit setup and teardown operators
-----------------------------------------

.. exampleinclude:: /../../ray/tests/system/ray/setup-teardown.py
   :language: python

KubeRay cluster specification
-----------------------------

The head service must be reachable from the Airflow worker. The included example uses a
``LoadBalancer`` service so the Ray dashboard can be submitted through XCom:

.. literalinclude:: ../../tests/system/ray/scripts/ray.yaml
   :language: yaml
