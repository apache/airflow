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

``apache-airflow-providers-airbyte``

Changelog
---------


5.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Bug Fixes
~~~~~~~~~

* ``Make Airbyte connection fields properly labeled in Airflow 3 (#58342)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

5.2.5
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 19 files in providers (airbyte, alibaba, atlassian, papermill, presto, redis, singularity, sqlite, tableau, vertica, weaviate, elasticsearch, exasol) (#57986)``

5.2.4
.....

Misc
~~~~

* ``Migrate airbyte provider to ''common.compat'' (#56996)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Make term Dag consistent in providers docs (#55101)``
   * ``Switch pre-commit to prek (#54258)``

5.2.3
.....

Misc
~~~~

* ``Improve debug logging in Airbyte provider (#51503)``

Doc-only
~~~~~~~~

* ``Improve connection docs (#53942)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.2.2
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages in providers to version_compat in airbyte (#52776)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.2.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Provider Migration: Update airbyte provider for Airflow 3.0 compatibility (#52418)``
* ``Replace 'models.BaseOperator' to Task SDK one for Alibaba & Airbyte (#52335)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``

Doc-only
~~~~~~~~

* ``Clean some leftovers of Python 3.9 removal - All the rest (#52432)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Airbyte test fixes, make mock JobResponse response id as int (#52134)``
   * ``Remove pytest.mark.db_test: airbyte and amazon providers where possible (#52017)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

5.2.0
.....

Features
~~~~~~~~

* ``Add option to create connections using proxies (#49729)``

Misc
~~~~

* ``Bump some provider dependencies for faster resolution (#51727)``

5.1.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``update airbyte changelog (#49934)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

5.0.2
.....

.. note::

   In this version of the provider, the ``provider_info`` entrypoint returned json has been cleaned up to
   not contain any extra values that have not been described in the
   `Provider Info Schema <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/provider_info.schema.json>`_
   This is generally backwards-compatible change, as those values appearing there (such as ``dependencies`` had
   never been described in the schema nor documentation (and the schema allows for optional, unsolicited components).
   If you depended on some values like ``dependencies`` there, the data exposed there is available in the metadata
   of the package (for example ``dependencies`` are available in ``requires`` metadata field of the package) and
   you should retrieve them from there instead.

   Also the ``Provider Info Schema`` for Airflow 3.0 has been updated to reflect the latest functionality
   that can be exposed by the provider. The schema is backwards-compatible, it only contains new possible
   entries that can appear there, reflecting new functionality added in Airflow 2 and 3.

Misc
~~~~

* ``remove superfluous else block (#49199)``
* ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``

Doc-only
~~~~~~~~

* ``Fix some mistakes in AirbyteJobSensor docs. (#49196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

5.0.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: api_version on on_kill method (#46833)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

Doc-only
~~~~~~~~

* ``Remove extra whitespace in provider readme template (#46975)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``
   * ``Move new provider tests to "provider_tests" submodule (#45955)``
   * ``Add script to move providers to the new directory structure (#45945)``
   * ``Move apache.iceberg provider to new providers structure (#45809)``
   * ``move Celery provider to new provider code structure (#45786)``
   * ``Move first provider (airbyte) to a separate project (#45259)``

5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters, and features have been removed from the Airbyte provider package.
  The following breaking changes were introduced:

  * Removed ``polling_interval`` parameter from ``AirbyteJobSensor``. Use the ``poke_interval`` parameter instead.


* ``Remove deprecated code from Airbyte provider (#44577)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update DAG example links in multiple providers documents (#44034)``
   * ``Prepare docs for Nov 1st wave of providers (#44011)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Update path of example dags in docs (#45069)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This version introduce a new way to handle the connection to Airbyte using ``client_id`` and ``client_secret`` instead of ``login`` and ``password``.
  You can get them accessing the Airbyte UI and creating a new Application in the Settings page.

  There is a large refactor to create a connection.
  You must specify the Full Qualified Domain Name in the ``host`` parameter, eg: ``https://my.company:8000/airbyte/v1/``.
  The ``token_url`` parameter is optional and it is used to create the access token, the default value is ``v1/applications/token`` used by Airbyte Cloud.
  You must remove the ``api_type`` parameter from your DAG it isn't required anymore.

* ``Update provider to use Airbyte API Python SDK (#41122)``

Misc
~~~~

* ``Fix wrong casing in airbyte hook. (#42170)``
* ``Pin airbyte-api to 0.51.0 (#42155)``
* ``remove deprecated soft_fail from providers (#41710)``

3.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.8.1
.....

Misc
~~~~

* ``Simplify 'airflow_version' imports (#39497)``
* ``Faster 'airflow_version' imports (#39552)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

3.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix(airbyte/hooks): add schema and port to prevent InvalidURL error (#38860)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

3.7.0
.....

Features
~~~~~~~~

* ``Ensure Airbyte Provider is Compatible with Cloud and Config APIs (#37943)``

Bug Fixes
~~~~~~~~~

* ``fix: try002 for provider airbyte (#38786)``
* ``fix(airbyte): add return statement to yield within a while loop in triggers (#38390)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Applied D401 to airbyte files. (#37370)``

3.6.0
.....

Features
~~~~~~~~

* ``Add deferrable functionality to the AirbyteJobSensor and AirbyteTriggerSyncOperator (#36780)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

3.5.1
.....

Bug Fixes
~~~~~~~~~

``Cancel airbyte job when timeout exceeded to prevent inconsistency among airflow and airbyte (#36241)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Update information about links into the provider.yaml files (#35837)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

3.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

3.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fix(providers/airbyte): respect soft_fail argument when exception is raised (#34156)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
   * ``Prepare docs for Aug 2023 2nd wave of Providers (#33291)``
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Airbyte and Alibaba (#32214)``
   * ``Improve provider documentation and README structure (#32125)``

3.3.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``

3.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

3.2.1
.....

Misc
~~~~

* ``Clarify optional parameters in Airbyte docstrings (#30031)``

3.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``
   * ``AIP-47 - Migrate Airbyte DAGs to new design (#25135)``

3.1.0
.....

Features
~~~~~~~~

* ``'AirbyteHook' add cancel job option (#24593)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.1.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.3
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.2
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix mypy airbyte provider errors (#20271)``
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix messed-up changelog in 3 providers (#17380)``
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Bug Fixes
~~~~~~~~~

* ``AirbyteHook - Consider incomplete status (#16965)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Updating Airbyte example DAG to use XComArgs (#16867)``
   * ``Removes pylint from our toolchain (#16682)``

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

* ``Add test_connection method to Airbyte hook (#16236)``

Bug Fixes
~~~~~~~~~

* ``Fix hooks extended from http hook (#16109)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add missing docstring params (#15741)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.0
.....

Initial version of the provider.
