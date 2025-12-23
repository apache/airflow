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

``apache-airflow-providers-common-compat``

Changelog
---------

1.10.1
......

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``
* ``Move the traces and metrics code under a common observability package (#56187)``
* ``Bump minimum prek version to 0.2.0 (#58952)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.10.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Adjust compat HookLevelLineage for new add_extra method (#58057)``

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

1.9.0
.....

Features
~~~~~~~~

* ``feat: backwards comp get async conn (#57143)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 19 files in providers (cncf,common) (#57995)``
   * ``Synchronize default versions in all split .pre-commit-config.yaml (#57851)``
   * ``Fix mypy errors in 'common/compat' (#57759)``
   * ``Extract prek hooks for Common.Compat provider (#57183)``

1.8.0
.....

Features
~~~~~~~~

* ``Simplify version-specific imports in the Standard provider (#56867)``
* ``Add SQLA's 'mapped_column' to common-compat (#56880)``
* ``Add comprehensive compatibility imports for Airflow 2 to 3 migration (#56790)``

Misc
~~~~

* ``Common.Compat: Extract reusable compat utilities and rename to sdk (#56884)``
* ``Simplify version-specific imports in the Google provider (#56793)``
* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56495)``

1.7.4
.....


Misc
~~~~

* ``Bump mypy to 1.18.1 (#55596)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Switch pre-commit to prek (#54258)``

1.7.3
.....

Misc
~~~~

* ``fix unreachable mypy warnings (#53575)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.7.2
.....

Misc
~~~~

* ``Replace models.BaseOperator to Task SDK one for Common Providers (#52443)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.7.1
.....

Misc
~~~~

* ``nit: Remove unreachable code (#51110)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.7.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix Breeze unit test (#50395)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

1.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Move bases classes to 'airflow.sdk.bases' (#48487)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add backwards compatibility provider tests for Airflow 3.0.0 (#49562)``
   * ``Prepare docs for Apr 3rd wave of providers (#49338)``
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Fix common-io and common-compat provider description format (#48864)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare documentation to release common.compat 1.6.1 (#49624)``

1.6.0
.....

Features
~~~~~~~~

* ``feat: Add helper for any provider version check (#47909)``
* ``feat: Add helper for OpenLineage version check (#47897)``

Misc
~~~~

* ``Move BaseNotifier to Task SDK (#48008)``
* ``AIP-84 Add Auth for DAG Versioning (#47553)``
* ``AIP-84 Add Auth for backfill (#47482)``
* ``AIP 84 Add auth for asset alias (#47241)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``serialize http transports contained in composite transport (#47444)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``fix: compat test test_provider_not_installed failing on main (#48012)``
   * ``Remove links to x/twitter.com (#47801)``

1.5.1
.....

Misc
~~~~

* ``Relocate airflow.auth to airflow.api_fastapi.auth (#47492)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

1.5.0
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

1.4.0
.....

Features
~~~~~~~~

* ``feat: automatically inject OL transport info into spark jobs (#45326)``
* ``feat: Add OpenLineage support for some SQL to GCS operators (#45242)``

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage sql parsing add try-except for sqlalchemy engine (#46366)``

Misc
~~~~

* ``Remove old lineage stuff (#45260)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``moving common-compat provider (#46063)``

1.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix(providers/common/compat): add back add_input_dataset and add_output_dataset to NoOpCollector (#44681)``
* ``Fix name of private function in compat provider (#44680)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Remove unnecessary compatibility code in S3 asset import (#44714)``
* ``Move Asset user facing components to task_sdk (#43773)``
* ``Make AssetAliasEvent a class context.py (#44709)``
* ``Move triggers to standard provider (#43608)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bumping common compat to 1.3.0 (#44728)``
   * ``Prevent __init__.py in providers from being modified (#44713)``
   * ``Fix accidental db tests in Task SDK (#44690)``
   * ``feat: automatically inject OL info into spark job in DataprocSubmitJobOperator (#44477)``

1.2.2
.....

Bug Fixes
~~~~~~~~~

* ``serialize asset/dataset timetable conditions in OpenLineage info also for Airflow 2 (#43434)``
* ``Move python operator to Standard provider (#42081)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Fix provider title in documentation (#43157)``

1.2.1
.....

Misc
~~~~

* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.1.0
.....

Features
~~~~~~~~

* ``Add method to common.compat to not force hooks to try/except every 2.10 hook lineage call (#40812)``

Misc
~~~~

* ``Migrate OpenLineage provider to V2 facets. (#39530)``
* ``Add support for hook lineage for S3Hook (#40819)``

1.0.0
.....

* ``Initial version of the provider. (#40374)``
