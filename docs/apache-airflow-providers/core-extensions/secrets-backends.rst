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

Secret backends
---------------

This is a summary of all Apache Airflow Community provided implementations of secret backends
exposed via community-managed providers.

Airflow has the capability of reading connections, variables and configuration from Secret Backends rather
than from its own Database. While storing such information in Airflow's database is possible, many of the
enterprise customers already have some secret managers storing secrets, and Airflow can tap into those
via providers that implement secrets backends for services Airflow integrates with.

.. note::

  Secret Backend integration do not allow writes to the secret backend.
  This is a design choice as normally secret stores require elevated permissions to write as it is a protected resource.
  That means ``Variable.set(...)`` will write to the Airflow metastore even if you use secret backend.
  If you need to update a value of a secret stored in the secret backend you must do it explicitly. That can be done
  by using operator that writes to the secret backend of your choice.

.. warning::

  If you have key ``foo`` in secret backend and you will do ``Variable.set(key='foo',...)`` it will create
  Airflow Variable with key ``foo`` in the Airflow metastore. It means you will have 2 secrets with key ``foo``.
  While this is possible, Airflow detects that this situation is likely wrong and output to the task log a warning that
  explains while the write request is honored it will be ignored with the next read. The reason for this is when
  executing ``Variable.get('foo')``, it will read the value from the secret backend. The value stored in Airflow
  metastore will be ignored due to priority given to the secret backend.


You can also take a
look at Secret backends available in the core Airflow in
:doc:`apache-airflow:security/secrets/secrets-backend/index` and here you can see the ones
provided by the community-managed providers:

.. airflow-secrets-backends::
   :tags: None
   :header-separator: "
