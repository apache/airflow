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

1.12.1
......

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage dag start event not being emitted (#42448)``
* ``fix: typo in error stack trace formatting for clearer output (#42017)``

1.12.0
......

Features
~~~~~~~~

* ``feat: notify about potential serialization failures when sending DagRun, don't serialize unnecessary params, guard listener for exceptions (#41690)``

Bug Fixes
~~~~~~~~~

* ``fix: cast list to flattened string in openlineage InfoJsonEncodable (#41786)``

Misc
~~~~

* ``chore: bump OL provider dependencies versions (#42059)``
* ``move to dag_run.logical_date from execution date in OpenLineage provider (#41889)``
* ``Unify DAG schedule args and change default to None (#41453)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.11.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``feat: add debug facet to all OpenLineage events (#41217)``
* ``feat: add fileloc to DAG info in AirflowRunFacet (#41311)``
* ``feat: remove openlineage client deprecated from_environment() method (#41310)``
* ``feat: openlineage listener captures hook-level lineage (#41482)``

Bug Fixes
~~~~~~~~~

* ``fix: get task dependencies without serializing task tree to string (#41494)``
* ``fix: return empty data instead of None when OpenLineage on_start method is missing (#41268)``
* ``fix: replace dagTree with downstream_task_ids (#41587)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``chore: remove openlineage deprecation warnings (#41284)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 2nd wave of providers (#41559)``

1.10.0
......

Features
~~~~~~~~

* ``Add AirflowRun on COMPLETE/FAIL events (#40996)``
* ``openlineage: extend custom_run_facets to also be executed on complete and fail (#40953)``
* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``openlineage: Add AirflowRunFacet for dag runEvents (#40854)``
* ``[AIP-62] Translate AIP-60 URI to OpenLineage (#40173)``
* ``Ability to add custom facet in OpenLineage events (#38982)``
* ``openlineage: add method to common.compat to not force hooks to try/except every 2.10 hook lineage call (#40812)``
* ``openlineage: use airflow provided getters from conf (#40790)``
* ``openlineage: add config to include 'full' task info based on conf setting (#40589)``
* ``Add TaskInstance log_url to OpenLineage facet (#40797)``
* ``openlineage: add deferrable information to task info in airflow run facet (#40682)``

Bug Fixes
~~~~~~~~~

* ``Adjust default extractor's on_failure detection for airflow 2.10 fix (#41094)``
* ``openlineage: make value of slots in attrs.define consistent across all OL usages (#40992)``
* ``Set 'slots' to True for facets used in DagRun (#40972)``
* ``openlineage: fix / add some task attributes in AirflowRunFacet (#40725)``

Misc
~~~~

* ``openlineage: replace dt.now with airflow.utils.timezone.utcnow (#40887)``
* ``openlineage: remove deprecated parentRun facet key (#40681)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.9.1
.....

Bug Fixes
~~~~~~~~~

* ``fix openlineage parsing dag tree with MappedOperator (#40621)``

1.9.0
.....

Features
~~~~~~~~

* ``local task job: add timeout, to not kill on_task_instance_success listener prematurely (#39890)``
* ``openlineage: add some debug logging around sql parser call sites (#40200)``
* ``Add task SLA and queued datetime information to AirflowRunFacet (#40091)``
* ``Add error stacktrace to OpenLineage task event (#39813)``
* ``Introduce AirflowJobFacet and AirflowStateRunFacet (#39520)``
* ``Use UUIDv7 for OpenLineage runIds (#39889)``
* ``openlineage: execute extraction and message sending in separate process (#40078)``
* ``Add few removed Task properties in AirflowRunFacet (#40371)``

Bug Fixes
~~~~~~~~~

* ``openlineage, redshift: do not call DB for schemas below Airflow 2.10 (#40197)``
* ``fix: scheduler crashing with OL provider on airflow standalone (#40459)``
* ``nit: fix logging level (#40461)``
* ``fix: provide stack trace under proper key in OL facet (#40372)``

Misc
~~~~

* ``fix: sqa deprecations for airflow providers (#39293)``
* ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave June 2024 (#40273)``
   * ``fix: scheduler crashing with OL provider on airflow standalone (#40353)``
   * ``Revert "fix: scheduler crashing with OL provider on airflow standalone (#40353)" (#40402)``

1.8.0
.....

.. warning::
  In Airflow 2.10.0, we fix the way try_number works.
  For Airflow >= 2.10.0, use ``apache-airflow-providers-openlineage >= 1.8.0``.
  Older versions of Airflow are not affected, In case you run an incompatible version
  an exception will be raised asking you to upgrade provider version.

Features
~~~~~~~~

* ``Scheduler to handle incrementing of try_number (#39336)``

Bug Fixes
~~~~~~~~~

* ``fix: Prevent error when extractor can't be imported (#39736)``
* ``Re-configure ORM in spawned OpenLineage process in scheduler. (#39735)``

Misc
~~~~

* ``chore: Update conf retrieval docstring and adjust pool_size (#39721)``
* ``Remove 'openlineage.common' dependencies in Google and Snowflake providers. (#39614)``
* ``Use 'ProcessPoolExecutor' over 'ThreadPoolExecutor'. (#39235)``
* ``misc: Add custom provider runtime checks (#39609)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``openlineage: notify that logged exception was caught (#39493)``
* ``chore: Add more OpenLineage logs to facilitate debugging (#39136)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add missing 'dag_state_change_process_pool_size' in 'provider.yaml'. (#39674)``
   * ``Run unit tests for Providers with airflow installed as package. (#39513)``
   * ``Reapply templates for all providers (#39554)``


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
