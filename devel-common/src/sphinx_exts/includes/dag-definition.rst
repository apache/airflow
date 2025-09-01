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

.. dag-definition-start
A Dag is a model that encapsulates everything needed to execute a workflow. Some Dag attributes include the following:

* **Schedule**: When the workflow should run.
* **Tasks**: :doc:`tasks </core-concepts/tasks>` are discrete units of work that are run on workers.
* **Task Dependencies**: The order and conditions under which :doc:`tasks </core-concepts/tasks>` execute.
* **Callbacks**: Actions to take when the entire workflow completes.
* **Additional Parameters**: And many other operational details.
.. dag-definition-end

.. dag-etymology-start
.. note::

    The term "DAG" comes from the mathematical concept "directed acyclic graph", but the meaning in Airflow has evolved well beyond just the literal data structure associated with the mathematical DAG concept. Therefore it was decided to use the term Dag in Airflow.
.. dag-etymology-end
