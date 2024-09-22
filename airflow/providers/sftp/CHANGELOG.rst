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

* ``change warnning message (#36148)``
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
   * ``Use reproducible builds for provider packages (#35693)``
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
