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

Decorators
----------
DAG authors can use decorators to author DAGs using the :doc:`TaskFlow <core-concepts/taskflow>` concept.
All Decorators derive from :class:`~airflow.providers.standard.decorators.base.TaskDecorator`.

Airflow has a set of Decorators that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :maxdepth: 1

  _api/airflow/decorators/index

You can read more about creating custom Decorators in :doc:`howto/create-custom-decorator`.
