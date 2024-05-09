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

main
.....

In Airflow 2.10.0, we fix the way try_number works, so that it no longer returns different values depending on task instance state.  Importantly, after the task is done, it no longer shows current_try + 1. Thus in 1.7.2 we patch this provider to fix try_number references so they no longer adjust for the old, bad behavior.

1.7.1
.....

Misc
~~~~

* ``openlineage, snowflake: do not run external queries for Snowflake (#39113)``

1.7.0
.....

Features
~~~~~~~~

* ``Add lineage_job_namespace and lineage_job_name OpenLineage macros (#38829)``
* ``openlineage: add 'opt-in' option (#37725)``

Bug Fixes
~~~~~~~~~

* ``fix: Remove redundant operator information from facets (#38264)``
* ``fix: disabled_for_operators now stops whole event emission (#38033)``
* ``fix: Add fallbacks when retrieving Airflow configuration to avoid errors being raised (#37994)``
* ``fix: Fix parent id macro and remove unused utils (#37877)``

Misc
~~~~

* ``Avoid use of 'assert' outside of the tests (#37718)``
* ``Add default for 'task' on TaskInstance / fix attrs on TaskInstancePydantic (#37854)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Brings back mypy-checks (#38597)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``fix: try002 for provider openlineage (#38806)``

1.6.0
.....

Features
~~~~~~~~

* ``feat: Add OpenLineage metrics for event_size and extraction time (#37797)``
* ``feat: Add OpenLineage support for File and User Airflow's lineage entities (#37744)``
* ``[OpenLineage] Add support for JobTypeJobFacet properties. (#37255)``
* ``chore: Update comments and logging in OpenLineage ExtractorManager (#37622)``

Bug Fixes
~~~~~~~~~

* ``fix: Check if operator is disabled in DefaultExtractor.extract_on_complete (#37392)``

Misc
~~~~

* ``docs: Update whole OpenLineage Provider docs. (#37620)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``tests: Add OpenLineage test cases for File to Dataset conversion (#37791)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Follow D401 style in openlineage, slack, and tableau providers (#37312)``

1.5.0
.....

Features
~~~~~~~~

* ``feat: Add dag_id when generating OpenLineage run_id for task instance. (#36659)``

.. Review and move the new changes to one of the sections above:
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

1.4.0
.....

Features
~~~~~~~~

* ``Add OpenLineage support for Redshift SQL. (#35794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix typo. (#36362)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.3.0
.....

Features
~~~~~~~~

* ``feat: Add parent_run_id for COMPLETE and FAIL events (#36067)``
* ``Add basic metrics to stats collector. (#35368)``

Bug Fixes
~~~~~~~~~

* ``fix: Repair run_id for OpenLineage FAIL events (#36051)``
* ``Fix and reapply templates for provider documentation (#35686)``

Misc
~~~~

* ``Remove ClassVar annotations. (#36084)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for provider packages (#35693)``

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
