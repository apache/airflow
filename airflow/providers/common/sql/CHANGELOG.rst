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

1.3.1
.....

This release fixes a few errors that were introduced in common.sql operator while refactoring common parts:

* ``_process_output`` method in ``SQLExecuteQueryOperator`` has now consistent semantics and typing, it
  can also modify the returned (and stored in XCom) values in the operators that derive from the
  ``SQLExecuteQueryOperator``).
* descriptions of all returned results are stored as descriptions property in the DBApiHook
* last description of the cursor whether to return single query results values are now exposed in
  DBApiHook via last_description property.

Lack of consistency in the operator caused ``1.3.0`` to be yanked - the ``1.3.0`` should not be used - if
you have ``1.3.0`` installed, upgrade to ``1.3.1``.

Bug Fixes
~~~~~~~~~

* ``Restore removed (but used) methods in common.sql (#27843)``
* ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
* ``Bump common.sql provider to 1.3.1 (#27888)``

1.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add SQLExecuteQueryOperator (#25717)``
* ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
* ``DbApiHook consistent insert_rows logging (#26758)``

Bug Fixes
~~~~~~~~~

* ``Common sql bugfixes and improvements (#26761)``
* ``Use unused SQLCheckOperator.parameters in SQLCheckOperator.execute. (#27599)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

1.2.0
.....

Features
~~~~~~~~

* ``Make placeholder style configurable (#25939)``
* ``Better error message for pre-common-sql providers (#26051)``

Bug Fixes
~~~~~~~~~

* ``Fix (and test) SQLTableCheckOperator on postgresql (#25821)``
* ``Don't use Pandas for SQLTableCheckOperator (#25822)``
* ``Discard semicolon stripping in SQL hook (#25855)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):


1.1.0
.....

Features
~~~~~~~~

* ``Improve taskflow type hints with ParamSpec (#25173)``
* ``Move all "old" SQL operators to common.sql providers (#25350)``
* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
* ``Unify DbApiHook.run() method with the methods which override it (#23971)``
* ``Common SQLCheckOperators Various Functionality Update (#25164)``

Bug Fixes
~~~~~~~~~

* ``Allow Legacy SqlSensor to use the common.sql providers (#25293)``
* ``Fix fetch_all_handler & db-api tests for it (#25430)``
* ``Align Common SQL provider logo location (#25538)``
* ``Fix SQL split string to include ';-less' statements (#25713)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix CHANGELOG for common.sql provider and add amazon commit (#25636)``

1.0.0
.....

Initial version of the provider.
Adds ``SQLColumnCheckOperator`` and ``SQLTableCheckOperator``.
Moves ``DBApiHook``, ``SQLSensor`` and ``ConnectorProtocol`` to the provider.
