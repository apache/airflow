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

The version of MySQL server has to be 5.6.4+. The exact version upper bound depends
on the version of ``mysqlclient`` package. For example, ``mysqlclient`` 1.3.12 can only be
used with MySQL server 5.6.4 through 5.7.

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

4.0.2
.....

Misc
~~~~

* ``Use MariaDB client binaries in arm64 image for support MySQL backend (#29519)``

4.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Fixed MyPy errors introduced by new mysql-connector-python (#28995)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Remove conn.close() ignores (#29005)" (#29010)``
   * ``Remove conn.close() ignores (#29005)``

4.0.0
.....

Breaking Changes
~~~~~~~~~~~~~~~~

You can no longer pass "local_infile" as extra in the connection. You should pass it instead as
hook's "local_infile" parameter when you create the MySqlHook (either directly or via hook_params).

* ``Move local_infile option from extra to hook parameter (#28811)``

3.4.0
.....

Features
~~~~~~~~

* ``Allow SSL mode in MySQL provider (#27717)``

Bug fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

3.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

3.2.1
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``D400 first line should end with period batch02 (#25268)``

3.2.0
.....

Features
~~~~~~~~

* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


3.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``

Bug Fixes
~~~~~~~~~

* ``Close the MySQL connections once operations are done. (#24508)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Migrate MySQL example DAGs to new design #22453 (#24142)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.2.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.2.1
.....

Misc
~~~~

* ``Support for Python 3.10``
* ``Make DbApiHook use get_uri from Connection (#21764)``
* ``Update MySqlOperator example dag (#21434)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``

Misc
~~~~

* ``Refactor vertica_to_mysql to make it more 'mypy' friendly (#20618)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``refactor: fixed type annotation for 'sql' in MySqlOperator (#17388)``
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Added template_fields_renderers for MySQL Operator (#16914)``
* ``Extended template_fields_renderers for MySQL provider (#16987)``
* ``Parse template parameters field for MySQL operator (#17080)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Make Airflow code Pylint 2.8 compatible (#15534)``
   * ``Update Docstrings of Modules with Missing Params (#15391)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation for Providers (#15499)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.1.0
.....

Features
~~~~~~~~

* ``Adds 'Trino' provider (with lower memory footprint for tests) (#15187)``
* ``A bunch of template_fields_renderers additions (#15130)``

Bug fixes
~~~~~~~~~

* ``Fix autocommit calls for mysql-connector-python (#14869)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``MySQL hook respects conn_name_attr (#14240)``

1.0.1
.....

Updated documentation and readme files.


1.0.0
.....

Initial version of the provider.
