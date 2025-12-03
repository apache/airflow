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

``apache-airflow-providers-common-io``

Changelog
---------

1.7.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

1.6.5
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 19 files in providers (cncf,common) (#57995)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

1.6.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix XCom object storage backend path validation (#56244)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56495)``
   * ``Remove useless Airflow version compatibility checks (#55852)``

1.6.3
.....


Bug Fixes
~~~~~~~~~

* ``Fix circular import when using ''XComObjectStorageBackend'' (#55805)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Remove Legacy Query Object Usage from airflow-models (#52325)``
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Switch pre-commit to prek (#54258)``

1.6.2
.....

Bug Fixes
~~~~~~~~~

* ``Prevent FileNotFoundError in deserialize_value of XCom io provider to crash the scheduler (#52702)``

Misc
~~~~

* ``Deprecate decorators from Core (#53629)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores (#53298)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Move remaining 'airflow.io' code to Task SDK (#53211)``
* ``Replace usages of XCOM_RETURN_KEY in providers to not be from utils (#53170)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.6.1
.....

Misc
~~~~

* ``Replace models.BaseOperator to Task SDK one for Common Providers (#52443)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Switch the Supervisor/task process from line-based to length-prefixed (#51699)``

1.6.0
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
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

1.5.4
.....

Bug Fixes
~~~~~~~~~

* ``Use BaseXCom serialize_value when objectstorage_threshold is less than given input (#49173)``

Misc
~~~~

* ``Use contextlib.suppress(exception) instead of try-except-pass and add SIM105 ruff rule (#49251)``
* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.5.3
.....

Misc
~~~~

* ``Move ObjectStoragePath and attach to Task SDK (#48906)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Fix common-io and common-compat provider description format (#48864)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Move bases classes to 'airflow.sdk.bases' (#48487)``

1.5.2
.....

Bug Fixes
~~~~~~~~~

* ``fix PosixPath not working with file create_asset (#47880)``
* ``convert non-absolute file path to prevent namespace explosion (#47818)``

Misc
~~~~

* ``AIP-72: Handle Custom XCom Backend on Task SDK (#47339)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Remove links to x/twitter.com (#47801)``

1.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Add local scheme as alternative to file for using the ObjectStoragePath (#46670)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Moving EmptyOperator to standard provider (#46231)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``refactor(providers/common/io): move common io provider to new structure (#46111)``

1.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``feat: add OpenLineage support for transfer operators between gcs and local (#44417)``
* ``Move Asset user facing components to task_sdk (#43773)``
* ``Migrate pickled data & change XCom value type to JSON (#44166)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Prepare docs for Nov 1st wave of providers (#44011)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Update path of example dags in docs (#45069)``

1.4.2
.....

Misc
~~~~

* ``Drop python3.8 support core and providers (#42766)``
* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Protect against None components of universal pathlib xcom backend (#41921)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.4.0
.....

Features
~~~~~~~~

* ``[AIP-62] Translate AIP-60 URI to OpenLineage (#40173)``
* ``openlineage: add file dataset type support into common.io provider (#40817)``

Misc
~~~~

* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``openlineage: add support for hook lineage for S3Hook (#40819)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

1.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage in FileTransferOperator for Airflow 2.8 (#39755)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix missing reverse quote in docs (#38275)``
* ``Fix remaining D401 checks (#37434)``

Misc
~~~~

* ``Improve XComObjectStorageBackend implementation (#38608)``
* ``Rename to XComObjectStorageBackend (#38607)``
* ``Turn common.io xcom exception into OptionalProviderFeatureException (#38543)``
* ``Update ObjectStoragePath for universal_pathlib>=v0.2.2 (#37930)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix XComObjectStoreBackend config var in docs (#38142)``
   * ``Revert ObjectStorage config variables name (#38415)``
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Revert "Update ObjectStoragePath for universal_pathlib>=v0.2.1 (#37524)" (#37567)``
   * ``Update ObjectStoragePath for universal_pathlib>=v0.2.1 (#37524)``
   * ``Add comment about versions updated by release manager (#37488)``

1.3.0
.....

Features
~~~~~~~~

* ``AIP-58: Add object storage backend for xcom (#37058)``

1.2.0
.....

Features
~~~~~~~~

* ``Add support for openlineage to AFS and common.io (#36410)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``
   * ``Prepare 2nd wave of providers in December (#36373)``
   * ``Prepare docs 1st wave of Providers December 2023 (#36112)``
   * ``Add documentation for 3rd wave of providers in Deember (#36464)``

1.1.0
.....

Features
~~~~~~~~

* ``Refactor ObjectStorage into a Path (#35612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for providers (#35693)``
   * ``Fix and reapply templates for provider documentation (#35686)``

1.0.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Used older interface from 2.8.0.dev0 versions``

Bug Fixes
~~~~~~~~~

* ``fix changelog of common-io (#35241)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improvements to airflow.io (#35478)``

1.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Used older interface from 2.8.0.dev0 versions``

Initial version of the provider.
