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

Changelog
---------

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

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
