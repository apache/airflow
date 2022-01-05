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


Changelog
---------

2.4.0
.....

Features
~~~~~~~~

* ``19489 - Pass client_encoding for postgres connections (#19827)``
* ``Amazon provider remove deprecation, second try (#19815)``


Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)``
   * ``Revert 'Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)' (#19791)``
   * ``Misc. documentation typos and language improvements (#19599)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``

2.3.0
.....

Features
~~~~~~~~

* ``Added upsert method on S3ToRedshift operator (#18027)``

Bug Fixes
~~~~~~~~~

* ``Fix example dag of PostgresOperator (#18236)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.2.0
.....

Features
~~~~~~~~

* ``Make schema in DBApiHook private (#17423)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``refactor: fixed type annotation for 'sql' param in PostgresOperator (#17331)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Improve postgres provider logging (#17214)``

2.1.0
.....

Features
~~~~~~~~

* ``Add schema as DbApiHook instance attribute (#16521)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``PostgresHook: deepcopy connection to avoid mutating connection obj (#15412)``
* ``postgres_hook_aws_conn_id (#16100)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix spelling (#15699)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

* ``Do not forward cluster-identifier to psycopg2 (#15360)``


1.0.1
.....

Updated documentation and readme files. Added HowTo guide for Postgres Operator.

1.0.0
.....

Initial version of the provider.
