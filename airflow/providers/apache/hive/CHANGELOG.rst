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

``apache-airflow-providers-apache-hive``


Changelog
---------

7.0.0
.....


Breaking changes
~~~~~~~~~~~~~~~~

Remove the ability of specify a proxy user as an ``owner`` or ``login`` or ``as_param`` in the connection.
Now, setting the user in ``Proxy User`` connection parameter or passing ``proxy_user`` to HiveHook will do the job.

* `` Simplify hive client connection (#37043)``

Misc
~~~~

* ``Fix pyhive hive_pure_sasl extra name (#37323)``

.. Review and move the new changes to one of the sections above:
   * ``D401 Support in Providers (simple) (#37258)``

6.4.2
.....


Bug Fixes
~~~~~~~~~

* ``Fix assignment of template field in '__init__' in 'hive-stats' (#36905)``

Misc
~~~~

* ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

6.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix assignment of template field in '__init__' in 'hive_to_samba.py' (#36486)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

6.4.0
.....

Features
~~~~~~~~

* ``Add param proxy user for hive (#36221)``

Misc
~~~~

* ``Add code snippet formatting in docstrings via Ruff (#36262)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.3.0
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
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``

6.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Consolidate hook management in HiveOperator (#34430)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: Simplify comparisons (#34181)``

6.1.6
.....

Misc
~~~~

* ``Refactor regex in providers (#33898)``
* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Replace single element slice by next() in hive provider (#33937)``
* ``Use a single  statement with multiple contexts instead of nested  statements in providers (#33768)``
* ``Use startswith once with a tuple in Hive hook (#33765)``
* ``Refactor: Simplify a few loops (#33736)``
* ``E731: replace lambda by a def method in Airflow providers (#33757)``
* ``Use f-string instead of  in Airflow providers (#33752)``

6.1.5
.....

.. note::
  The provider now uses pure-sasl, a pure-Python implementation of SASL,
  which is better maintained than previous sasl implementation, even
  if a bit slower for sasl interface. It also allows hive to be
  installed for Python 3.11.

Misc
~~~~

* ``Bring back hive support for Python 3.11 (#32607)``
* ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``
* ``Simplify 'X for X in Y' to 'Y' where applicable (#33453)``
* ``Replace OrderedDict with plain dict (#33508)``
* ``Simplify code around enumerate (#33476)``
* ``Use str.splitlines() to split lines in providers (#33593)``
* ``Simplify conditions on len() in providers/apache (#33564)``
* ``Replace repr() with proper formatting (#33520)``
* ``Avoid importing pandas and numpy in runtime and module level (#33483)``
* ``Consolidate import and usage of pandas (#33480)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``

6.1.4
.....

Misc
~~~~

* ``Bring back mysql-connector-python as required depednency (#32989)``

6.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix Pandas2 compatibility for Hive (#32752)``

Misc
~~~~

* ``Add more accurate typing for DbApiHook.run method (#31846)``
* ``Move Hive configuration to Apache Hive provider (#32777)``


6.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Add proxy_user template check (#32334)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``

6.1.1
.....

.. note::
  This release dropped support for Python 3.7

Bug Fixes
~~~~~~~~~

* ``Sanitize beeline principal parameter (#31983)``

Misc
~~~~

* ``Replace unicodecsv with standard csv library (#31693)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

   * ``Apache provider docstring improvements (#31730)``
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Apache providers only (#31424)``
   * ``Add Python 3.11 support (#27264)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

6.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Update return types of 'get_key' methods on 'S3Hook' (#30923)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Decouple "job runner" from BaseJob ORM model (#30255)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

6.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The auth option is moved from the extra field to the auth parameter in the Hook. If you have extra
parameters defined in your connections as auth, you should move them to the DAG where your HiveOperator
or other Hive related operators are used.

* ``Move auth parameter from extra to Hook parameter (#30212)``

5.1.3
.....

Bug Fixes
~~~~~~~~~
* ``Validate Hive Beeline parameters (#29502)``

5.1.2
.....

Misc
~~~~

* ``Fixed MyPy errors introduced by new mysql-connector-python (#28995)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Remove conn.close() ignores (#29005)" (#29010)``
   * ``Remove conn.close() ignores (#29005)``

5.1.1
.....

Bug Fixes
~~~~~~~~~
* ``Move local_infile option from extra to hook parameter (#28811)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.1.0
.....

Features
~~~~~~~~

The ``apache.hive`` provider provides now hive macros that used to be provided by Airflow. As of 5.1.0 version
of ``apache.hive`` the hive macros are provided by the Provider.

* ``Move Hive macros to the provider (#28538)``
* ``Make pandas dependency optional for Amazon Provider (#28505)``

.. Review and move the new changes to one of the sections above:

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The ``hive_cli_params`` from connection were moved to the Hook. If you have extra parameters defined in your
connections as ``hive_cli_params`` extra, you should move them to the DAG where your HiveOperator is used.

* ``Move hive_cli_params to hook parameters (#28101)``

Features
~~~~~~~~

* ``Improve filtering for invalid schemas in Hive hook (#27808)``


4.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

4.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Bug Fixes
~~~~~~~~~

* ``Filter out invalid schemas in Hive hook (#27647)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

4.0.1
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Review and move the new changes to one of the sections above:
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.0.0
.....

Breaking Changes
~~~~~~~~~~~~~~~~

* The ``hql`` parameter in ``get_records`` of ``HiveServer2Hook`` has been renamed to sql to match the
  ``get_records`` DbApiHook signature. If you used it as a positional parameter, this is no change for you,
  but if you used it as keyword one, you need to rename it.
* ``hive_conf`` parameter has been renamed to ``parameters`` and it is now second parameter, to match ``get_records``
  signature from the DbApiHook. You need to rename it if you used it.
* ``schema`` parameter in ``get_records`` is an optional kwargs extra parameter that you can add, to match
  the schema of ``get_records`` from DbApiHook.

* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
* ``Remove Smart Sensors (#25507)``


3.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``

Bug Fixes
~~~~~~~~~

* ``fix connection extra parameter 'auth_mechanism' in 'HiveMetastoreHook' and 'HiveServer2Hook' (#24713)``

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

Misc
~~~~

* ``chore: Refactoring and Cleaning Apache Providers (#24219)``
* ``AIP-47 - Migrate hive DAGs to new design #22439 (#24204)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add typing for airflow/configuration.py (#23716)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix HiveToMySqlOperator's wrong docstring (#23316)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pre-commit hook versions (#22887)``

2.3.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.3.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.3.0
.....

Features
~~~~~~~~

* ``Set larger limit get_partitions_by_filter in HiveMetastoreHook (#21504)``

Bug Fixes
~~~~~~~~~

* ``Fix Python 3.9 support in Hive (#21893)``
* ``Fix key typo in 'template_fields_renderers' for 'HiveOperator' (#21525)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Add how-to guide for hive operator (#21590)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy issues in 'example_twitter_dag' (#21571)``
   * ``Remove unnecessary/stale comments (#21572)``

2.2.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix MyPy errors in Apache Providers (#20422)``
   * ``Fix MyPy Errors for providers: Tableau, CNCF, Apache (#20654)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Add some type hints for Hive providers (#20210)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.1.0
.....

Features
~~~~~~~~

* ``hive provider: restore HA support for metastore (#19777)``

Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix typos in Hive transfer operator docstrings (#19474)``
   * ``Improve various docstrings in Apache Hive providers (#19866)``
   * ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``

2.0.3
.....

Bug Fixes
~~~~~~~~~

* ``fix get_connections deprecation warn in hivemetastore hook (#18854)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``More f-strings (#18855)``
   * ``Remove unnecessary string concatenations in AirflowException in s3_to_hive.py (#19026)``
   * ``Update documentation for September providers release (#18613)``
   * ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``

2.0.2
.....

Bug fixes
~~~~~~~~~

* ``HiveHook fix get_pandas_df() failure when it tries to read an empty table (#17777)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.0.1
.....

Features
~~~~~~~~

* ``Add Python 3.9 support (#15515)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Updating Apache example DAGs to use XComArgs (#16869)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Remove duplicate key from Python dictionary (#15735)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Make Airflow code Pylint 2.8 compatible (#15534)``
   * ``Use Pip 21.* to install airflow officially (#15513)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation for the Hive Provider (#15704)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.3
.....

Bug fixes
~~~~~~~~~

* ``Fix mistake and typos in doc/docstrings (#15180)``
* ``Fix grammar and remove duplicate words (#14647)``
* ``Resolve issue related to HiveCliHook kill (#14542)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Remove password if in LDAP or CUSTOM mode HiveServer2Hook (#11767)``

1.0.0
.....

Initial version of the provider.
