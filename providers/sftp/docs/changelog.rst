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

``apache-airflow-providers-sftp``


Changelog
---------

5.5.1
.....

Bug Fixes
~~~~~~~~~

* ``fix(sftp): add default port to SFTPHookAsync (#58945)``

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.5.0
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

5.4.2
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate sftp provider to 'common.compat' (#57111)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to sftp Provider test (#57924)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

5.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Pass required remote_host arg to SSHHook (#55664)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT001 rule to prvoider tests (#55935)``

5.4.0
.....


Features
~~~~~~~~

* ``Feature: add optional managed connection (#52700)``
* ``Add file_pattern to template fields (#54562)``

Bug Fixes
~~~~~~~~~

* ``Fix sftp async hoook (#54763)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

5.3.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix missing prefetch arg to SFTPOperator (#53906)``

Misc
~~~~

* ``Limit paramiko to '< 4.0.0' till we remove DSS support (#54078)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Updating SSH dependency for SFTP provider (#53100)``
* ``Fix BlobWriter (GCS)  support for SFTP Streaming (#52850)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in sftp provider where possible (#53266)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages to version_compat in SFTP (#52894)``

Doc-only
~~~~~~~~

* ``docs: Correct TaskFlow capitalization in documentation (#51794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Deprecate decorators from Core (#53629)``

5.3.2
.....

Bug Fixes
~~~~~~~~~

* ``bugfix: removed cache for proxycommand in SFTPHook (#52641)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Replace 'models.BaseOperator' to Task SDK one for SFTP (#52435)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Separate out creation of default Connections for tests and non-tests (#52129)``

5.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Updating SFTPSensor to properly handle scenario where file is missing (#51167)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.3.0
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

5.2.1
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.2.0
.....

Features
~~~~~~~~

* ``Implement concurrent directory transfer in SFTPOperator (#47533)``

Misc
~~~~

* ``Make '@task' import from airflow.sdk (#48896)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``

5.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Checking modification timestamps only when newer_than parameter is present (#48063)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

5.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Re-added close_conn connection on SFTPHook and get_conn should return SFTPClient instead of context managed connection (#47217)``

Misc
~~~~

* ``Change get_conn to get_managed_conn in direcotry transfer (#47248)``
* ``Improve SFTP hook's directory transfer to use a single connection for multiple files (#46582)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

5.1.0
.....

Features
~~~~~~~~

* ``Add DELETE operation in SFTPOperator (#46233)``
* ``Also allow passing buffer instead of path for retrieve_file and store_file methods in SFTPHook (#44247)``
* ``Add directory transfer support for SFTPOperator (#44126)``

Bug Fixes
~~~~~~~~~

* ``Make sure the SSHClient is also closed when using connection in context manager from SFTPHook (#46716)``

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Move SFTP Provider to the New Structure and fix codespell checks (#46155)``

5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the sftp provider package.
  The following breaking changes were introduced:

  * Removed deprecated ``ssh_hook`` parameter from ``SFTPOperator``. Use ``sftp_hook`` instead.
  * Removed deprecated ``ssh_hook`` parameter from ``SFTPHook``.
  * Removed deprecated ``ftp_conn_id`` parameter from ``SFTPHook``. Use ``ssh_conn_id`` instead.

* ``Remove deprecations from SFTP Provider (#44740)``

Features
~~~~~~~~

* ``feat: retrieve sftp file attrs once instead multiple time (#44625)``
* ``Add host_proxy_cmd parameter to SSHHook and SFTPHook (#44565)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Prepare docs for Nov 1st wave of providers (#44011)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

4.11.1
......

Misc
~~~~

* ``remove deprecated soft_fail from providers (#41710)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.11.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.10.3
......

Misc
~~~~

* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

4.10.2
......

Bug Fixes
~~~~~~~~~

* ``Fix resource management in SFTPSensor (#40022)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``


4.10.1
......

Bug Fixes
~~~~~~~~~

* ``Fix SFTPSensor.newer_than not working with jinja logical ds/ts expression (#39056)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

4.10.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

4.9.1
.....

Bug Fixes
~~~~~~~~~

* ``fix(sftp): add return statement to yield within a while loop in triggers (#38391)``
* ``Close open connections for deferrable SFTPSensor (#38881)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Resolve 'PT012' in 'SFTP' provider tests (#38518)``
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``

4.9.0
.....

Features
~~~~~~~~

* ``Add deferrable param in SFTPSensor (#37117)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``
   * ``Added D401 support to http, smtp and sftp provider (#37303)``

4.8.1
.....

Bug Fixes
~~~~~~~~~

* ``change warning message (#36148)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``

Misc
~~~~

* ``Add code snippet formatting in docstrings via Ruff (#36262)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.8.0
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
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Switch from Black to Ruff formatter (#35287)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

4.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: Think positively in providers (#34279)``

4.6.1
.....

Bug Fixes
~~~~~~~~~

* ``fix(providers/sftp): respect soft_fail argument when exception is raised (#34169)``

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

4.6.0
.....


Features
~~~~~~~~

* ``Add parameter sftp_prefetch to SFTPToGCSOperator (#33274)``

Misc
~~~~

* ``Refactor: Remove useless str() calls (#33629)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers - Final Pass (#33303)``

4.5.0
.....

Features
~~~~~~~~

* ``openlineage, sftp: add OpenLineage support for sftp provider (#31360)``

4.4.0
.....

Features
~~~~~~~~

* ``Adds sftp_sensor decorator (#32457)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove redundant Operator suffix from sensor name (#32475)``

4.3.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Providers (#31427)``

4.3.0
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
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

4.2.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix SFTPSensor when using newer_than and there are multiple matched files (#29794)``

4.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix sftp sensor with pattern (#29467)``

4.2.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix SFTP operator's template fields processing (#29068)``
* ``FTP operator has logic in __init__ (#29073)``

4.2.1
.....

Misc
~~~~

* ``Update codespell and fix typos (#28568)``
* ``[misc] Get rid of 'pass' statement in conditions (#27775)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Bug Fixes
~~~~~~~~~

* ``SFTP Provider: Fix default folder permissions  (#26593)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

4.1.0
.....

Features
~~~~~~~~

* ``SFTPOperator - add support for list of file paths (#26666)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Convert sftp hook to use paramiko instead of pysftp (#24512)``

Features
~~~~~~~~

* ``Update 'actual_file_to_check' with rendered 'path' (#24451)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Automatically detect if non-lazy logging interpolation is used (#24910)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``
   * ``Add documentation for July 2022 Provider's release (#25030)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Adding fnmatch type regex to SFTPSensor (#24084)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.6.0
.....

Features
~~~~~~~~

* ``add newer_than parameter to SFTP sensor (#21655) (#22377)``

2.5.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.5.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.5.0
.....

Features
~~~~~~~~

* ``Updates FTPHook provider to have test_connection (#21997)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Add optional features in providers. (#21074)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``

2.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Bugfix: ''SFTPHook'' does not respect ''ssh_conn_id'' arg (#20756)``
* ``fix deprecation messages for SFTPHook (#20692)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Breaking change found with ssh_conn_id``

Features
~~~~~~~~

* ``Making SFTPHook's constructor consistent with its superclass SSHHook (#20164)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for SFTP provider (#20242)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.3.0
.....

Features
~~~~~~~~

* ``Add test_connection method for sftp hook (#19609)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

Features
~~~~~~~~

* ``SFTP hook to prefer the SSH paramiko key over the key file path (#18988)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``More f-strings (#18855)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Add support for non-RSA type key for SFTP hook (#16314)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove redundant logging in SFTP Hook (#16704)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``

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

* ``Depreciate private_key_pass in SFTPHook conn extra and rename to private_key_passphrase (#14028)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.2.0
.....

Features
~~~~~~~~

* ``Undeprecate private_key option in SFTPHook (#15348)``
* ``Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)``

1.1.1
.....

Features
~~~~~~~~

* ``SFTPHook private_key_pass extra param is deprecated and renamed to private_key_passphrase, for consistency with
  arguments' naming in SSHHook``

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Add retryer to SFTP hook connection (#13065)``


1.0.0
.....

Initial version of the provider.
