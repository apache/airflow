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

``apache-airflow-providers-ydb``


Changelog
---------

2.1.1
.....

Bug Fixes
~~~~~~~~~

* ``fIx deprecation warnings in common.sql (#47169)``

Misc
~~~~

* ``Replace ydb limitation with yandexcloud exclusion (#47142)``
* ``Limit ydb to < 3.19.0 (#47105)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``
   * ``Adding a task that validates format of dates in YDB example dag (#46807)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``init run on ydb (#46058)``

2.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

.. Review and move the new changes to one of the sections above:
   * ``Update Example URL in YDB docs (#45033)``

2.0.0
.....

.. note::
  This release removes YDBScanQueryOperator from this provider package.
  At this point, YDBExecuteQueryOperator could load unlimited amount of rows, so no specific operator is needed.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Migrate YDB Operator to new DBAPI (#43784)``

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

1.4.0
.....

Features
~~~~~~~~

* ``Add an ability to use scan queries via new YDB operator (#42311)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Add database to table name in bulk upsert, use bulk upsert in system test (#41303)``
* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.0
.....

Features
~~~~~~~~

* ``ydb provider: add bulk upsert support (#40631)``

Misc
~~~~

* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.1.0
.....

Features
~~~~~~~~

* ``support auth key from content and from file (#40390)``

1.0.0
.....

* ``Initial version of the provider (#39996)``
