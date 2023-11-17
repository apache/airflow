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

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-openlineage``


Changelog
---------

1.2.1
.....

Misc
~~~~

* ``Make schema filter uppercase in 'create_filter_clauses' (#35428)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix bad regexp in mypy-providers specification in pre-commits (#35465)``
   * ``Switch from Black to Ruff formatter (#35287)``

1.2.0
.....

Features
~~~~~~~~

* ``Send column lineage from SQL operators. (#34843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

.. Review and move the new changes to one of the sections above:
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

1.1.1
.....

Misc
~~~~

* ``Adjust log levels in OpenLineage provider (#34801)``

1.1.0
.....

Features
~~~~~~~~

* ``Allow to disable openlineage at operator level (#33685)``


Bug Fixes
~~~~~~~~~

* ``Fix import in 'get_custom_facets'. (#34122)``

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Add OpenLineage support for DBT Cloud. (#33959)``
* ``Refactor unneeded  jumps in providers (#33833)``
* ``Refactor: Replace lambdas with comprehensions in providers (#33771)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``openlineage: don't run task instance listener in executor (#33366)``
* ``openlineage: do not try to redact Proxy objects from deprecated config (#33393)``
* ``openlineage: defensively check for provided datetimes in listener (#33343)``

Misc
~~~~

* ``Add OpenLineage support for Trino. (#32910)``
* ``Simplify conditions on len() in other providers (#33569)``
* ``Replace repr() with proper formatting (#33520)``

1.0.1
.....

Bug Fixes
~~~~~~~~~

* ``openlineage: disable running listener if not configured (#33120)``
* ``Don't use database as fallback when no schema parsed. (#32959)``

Misc
~~~~

* ``openlineage, bigquery: add openlineage method support for BigQueryExecuteQueryOperator (#31293)``
* ``Move openlineage configuration to provider (#33124)``

1.0.0
.....

Initial version of the provider.
