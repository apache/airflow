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

5.4.0
.....

Features
~~~~~~~~
* ``Bring back psycopg2-binary as dependency instead of psycopg (#28316)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

5.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``PostgresHook: Added ON CONFLICT DO NOTHING statement when all target fields are primary keys (#26661)``
* ``Add SQLExecuteQueryOperator (#25717)``
* ``Rename schema to database in PostgresHook (#26744)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

5.2.2
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Review and move the new changes to one of the sections above:
   * ``Rename schema to database in 'PostgresHook' (#26436)``
   * ``Revert "Rename schema to database in 'PostgresHook' (#26436)" (#26734)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump dep on common-sql to fix issue with SQLTableCheckOperator (#26143)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``postgres provider: use non-binary psycopg2 (#25710)``

5.2.0
.....

Features
~~~~~~~~

* ``Use only public AwsHook's methods during IAM authorization (#25424)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


5.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Postgres example DAGs to new design #22458 (#24148)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

4.1.0
.....

Features
~~~~~~~~

* ``adds ability to pass config params to postgres operator (#21551)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

4.0.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

4.0.0
.....

The URIs returned by Postgres ``get_uri()`` returns ``postgresql://`` instead
of ``postgres://`` prefix which is the only supported prefix for the
SQLAlchemy 1.4.0+. Any usage of ``get_uri()`` where ``postgres://`` prefix
should be updated to reflect it.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Replaces the usage of postgres:// with postgresql:// (#21205)``

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Fix mypy errors in postgres/hooks and postgres/operators (#20600)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix mypy providers (#20190)``
   * ``Add documentation for January 2021 providers release (#21257)``


3.0.1
.....

Misc
~~~~

* ``Make DbApiHook use get_uri from Connection (#21764)``

2.4.0
.....

Features
~~~~~~~~

* ``19489 - Pass client_encoding for postgres connections (#19827)``
* ``Amazon provider remove deprecation, second try (#19815)``


Bug Fixes
~~~~~~~~~

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)``
   * ``Revert 'Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)' (#19791)``
   * ``Misc. documentation typos and language improvements (#19599)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``

2.3.0
.....

Features
~~~~~~~~

* ``Added upsert method on S3ToRedshift operator (#18027)``

Bug Fixes
~~~~~~~~~

* ``Fix example dag of PostgresOperator (#18236)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.2.0
.....

Features
~~~~~~~~

* ``Make schema in DBApiHook private (#17423)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``refactor: fixed type annotation for 'sql' param in PostgresOperator (#17331)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Improve postgres provider logging (#17214)``

2.1.0
.....

Features
~~~~~~~~

* ``Add schema as DbApiHook instance attribute (#16521)``

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

Features
~~~~~~~~

* ``PostgresHook: deepcopy connection to avoid mutating connection obj (#15412)``
* ``postgres_hook_aws_conn_id (#16100)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix spelling (#15699)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

* ``Do not forward cluster-identifier to psycopg2 (#15360)``


1.0.1
.....

Updated documentation and readme files. Added HowTo guide for Postgres Operator.

1.0.0
.....

Initial version of the provider.
