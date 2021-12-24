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



.. _howto/operator:GithubOperator:

GithubOperator
=================

Use the :class:`~airflow.providers.github.operators.GithubOperator` to execute
SQL commands in a `Github <https://www.github.com/>`__ database.

An example of running the query using the operator:

.. exampleinclude:: /../../airflow/providers/github/example_dags/example_github_query.py
    :language: python
    :start-after: [START howto_operator_github]
    :end-before: [END howto_operator_github]
