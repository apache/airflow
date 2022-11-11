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

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.3+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers
* In SnowflakeHook, if both ``extra__snowflake__foo`` and ``foo`` existed in connection extra
  dict, the prefixed version would be used; now, the non-prefixed version will be preferred.

3.3.0
.....

Features
~~~~~~~~

* ``Add custom handler param in SnowflakeOperator (#25983)``

Bug Fixes
~~~~~~~~~

* ``Fix wrong deprecation warning for 'S3ToSnowflakeOperator' (#26047)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``copy into snowflake from external stage (#25541)``

3.2.0
.....

Features
~~~~~~~~

* ``Move all "old" SQL operators to common.sql providers (#25350)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


3.1.0
.....

Features
~~~~~~~~

* ``Adding generic 'SqlToSlackOperator' (#24663)``
* ``Move all SQL classes to common-sql provider (#24836)``
* ``Pattern parameter in S3ToSnowflakeOperator (#24571)``

Bug Fixes
~~~~~~~~~

* ``S3ToSnowflakeOperator: escape single quote in s3_keys (#24607)``

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

Bug Fixes
~~~~~~~~~

* ``Fix error when SnowflakeHook take empty list in 'sql' param (#23767)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Snowflake system tests to new design #22434 (#24151)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.7.0
.....

Features
~~~~~~~~

* ``Allow multiline text in private key field for Snowflake (#23066)``

2.6.0
.....

Features
~~~~~~~~

* ``Add support for private key in connection for Snowflake (#22266)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.5.2
.....

Misc
~~~~

* ``Remove Snowflake limits (#22181)``

2.5.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.5.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``

Bug Fixes
~~~~~~~~~

* ``Fix #21096: Support boolean in extra__snowflake__insecure_mode (#21155)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add optional features in providers. (#21074)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Snowflake Provider: Improve tests for Snowflake Hook (#20745)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.4.0
.....

Features
~~~~~~~~

* ``Support insecure mode in SnowflakeHook (#20106)``
* ``Remove unused code in SnowflakeHook (#20107)``
* ``Improvements for 'SnowflakeHook.get_sqlalchemy_engine'  (#20509)``
* ``Exclude snowflake-sqlalchemy v1.2.5 (#20245)``
* ``Limit Snowflake connector to <2.7.2 (#20395)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for Snowflake provider. (#20212)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Remove duplicate get_connection in SnowflakeHook (#19543)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.0
.....

Features
~~~~~~~~

* ``Add test_connection method for Snowflake Hook (#19041)``
* ``Add region to Snowflake URI. (#18650)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Moving the example tag a little bit up to include the part where you specify the snowflake_conn_id (#19180)``

2.2.0
.....

Features
~~~~~~~~

* ``Add Snowflake operators based on SQL Checks  (#17741)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Fix messed-up changelog in 3 providers (#17380)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Adding: Snowflake Role in snowflake provider hook (#16735)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Logging and returning info about query execution SnowflakeHook (#15736)``
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

Features
~~~~~~~~

* ``Add 'template_fields' to 'S3ToSnowflake' operator (#15926)``
* ``Allow S3ToSnowflakeOperator to omit schema (#15817)``
* ``Added ability for Snowflake to attribute usage to Airflow by adding an application parameter (#16420)``

Bug Fixes
~~~~~~~~~

* ``fix: restore parameters support when sql passed to SnowflakeHook as str (#16102)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix formatting and missing import (#16455)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.3.0
.....

Features
~~~~~~~~

* ``Expose snowflake query_id in snowflake hook and operator (#15533)``

1.2.0
.....

Features
~~~~~~~~

* ``Add dynamic fields to snowflake connection (#14724)``

1.1.1
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``
* ``Prepare to release the next wave of providers: (#14487)``

1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Fix S3ToSnowflakeOperator to support uploading all files in the specified stage (#12505)``
* ``Add connection arguments in S3ToSnowflakeOperator (#12564)``

1.0.0
.....

Initial version of the provider.
