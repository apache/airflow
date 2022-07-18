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

3.1.0
.....

Features
~~~~~~~~

* ``Added databricks_conn_id as templated field (#24945)``
* ``Add 'test_connection' method to Databricks hook (#24617)``
* ``Move all SQL classes to common-sql provider (#24836)``

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Automatically detect if non-lazy logging interpolation is used (#24910)``
   * ``Remove "bad characters" from our codebase (#24841)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Features
~~~~~~~~

* ``Add Deferrable Databricks operators (#19736)``
* ``Add git_source to DatabricksSubmitRunOperator (#23620)``

Bug Fixes
~~~~~~~~~

* ``fix: DatabricksSubmitRunOperator and DatabricksRunNowOperator cannot define .json as template_ext (#23622) (#23641)``
* ``Fix UnboundLocalError when sql is empty list in DatabricksSqlHook (#23815)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``AIP-47 - Migrate databricks DAGs to new design #22442 (#24203)``
   * ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.7.0
.....

Features
~~~~~~~~

* ``Update to the released version of DBSQL connector``
* ``DatabricksSqlOperator - switch to databricks-sql-connector 2.x``
* ``Further improvement of Databricks Jobs operators (#23199)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Address review comments``
   * ``Clean up in-line f-string concatenation (#23591)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

2.6.0
.....

Features
~~~~~~~~

* ``More operators for Databricks Repos (#22422)``
* ``Add a link to Databricks Job Run (#22541)``
* ``Databricks SQL operators are now Python 3.10 compatible (#22886)``

Bug Fixes
~~~~~~~~~

* ``Databricks: Correctly handle HTTP exception (#22885)``

Misc
~~~~

* ``Refactor 'DatabricksJobRunLink' to not create ad hoc TaskInstances (#22571)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update black precommit (#22521)``
   * ``Fix new MyPy errors in main (#22884)``
   * ``Prepare mid-April provider documentation. (#22819)``

   * ``Prepare for RC2 release of March Databricks provider (#22979)``

2.5.0
.....

Features
~~~~~~~~

* ``Operator for updating Databricks Repos (#22278)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.4.0
.....

Features
~~~~~~~~

* ``Add new options to DatabricksCopyIntoOperator (#22076)``
* ``Databricks hook - retry on HTTP Status 429 as well (#21852)``

Misc
~~~~

* ``Skip some tests for Databricks from running on Python 3.10 (#22221)``

2.3.0
.....

Features
~~~~~~~~

* ``Add-showing-runtime-error-feature-to-DatabricksSubmitRunOperator (#21709)``
* ``Databricks: add support for triggering jobs by name (#21663)``
* ``Added template_ext = ('.json') to databricks operators #18925 (#21530)``
* ``Databricks SQL operators (#21363)``

Bug Fixes
~~~~~~~~~

* ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Updated Databricks docs for correct jobs 2.1 API and links (#21494)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``

2.2.0
.....

Features
~~~~~~~~

* ``Add 'wait_for_termination' argument for Databricks Operators (#20536)``
* ``Update connection object to ''cached_property'' in ''DatabricksHook'' (#20526)``
* ``Remove 'host' as an instance attr in 'DatabricksHook' (#20540)``
* ``Databricks: fix verification of Managed Identity (#20550)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for Databricks provider. (#20265)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix mypy databricks operator (#20598)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.1.0
.....

Features
~~~~~~~~

* ``Databricks: add more methods to represent run state information (#19723)``
* ``Databricks - allow Azure SP authentication on other Azure clouds (#19722)``
* ``Databricks: allow to specify PAT in Password field (#19585)``
* ``Databricks jobs 2.1 (#19544)``
* ``Update Databricks API from 2.0 to 2.1 (#19412)``
* ``Authentication with AAD tokens in Databricks provider (#19335)``
* ``Update Databricks operators to match latest version of API 2.0 (#19443)``
* ``Remove db call from DatabricksHook.__init__() (#20180)``

Bug Fixes
~~~~~~~~~

* ``Fixup string concatenations (#19099)``
* ``Databricks hook: fix expiration time check (#20036)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Refactor DatabricksHook (#19835)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Unhide changelog entry for databricks (#20128)``
   * ``Update documentation for RC2 release of November Databricks Provider (#20086)``

2.0.2
.....

Bug Fixes
~~~~~~~~~
   * ``Move DB call out of DatabricksHook.__init__ (#18339)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``An initial rework of the 'Concepts' docs (#15444)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add documentation for Databricks connection (#15410)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
