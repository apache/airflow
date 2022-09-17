
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


Package apache-airflow-providers-common-sql
------------------------------------------------------

`Common SQL Provider <https://en.wikipedia.org/wiki/SQL>`__


This is detailed commit list of changes for versions provider package: ``common.sql``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.2.0
.....

Latest change: 2022-08-30

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`27e2101f6e <https://github.com/apache/airflow/commit/27e2101f6ee5567b2843cbccf1dca0b0e7c96186>`_  2022-08-30   ``Better error messsage for pre-common-sql providers (#26051)``
`a74d934991 <https://github.com/apache/airflow/commit/a74d9349919b340638f0db01bc3abb86f71c6093>`_  2022-08-27   ``Fix placeholders in 'TrinoHook', 'PrestoHook', 'SqliteHook' (#25939)``
`874a95cc17 <https://github.com/apache/airflow/commit/874a95cc17c3578a0d81c5e034cb6590a92ea310>`_  2022-08-22   ``Discard semicolon stripping in SQL hook (#25855)``
`dd72e67524 <https://github.com/apache/airflow/commit/dd72e67524c99e34ba4c62bfb554e4caf877d5ec>`_  2022-08-19   ``Fix (and test) SQLTableCheckOperator on postgresql (#25821)``
`5b3d579a42 <https://github.com/apache/airflow/commit/5b3d579a42bcf21c43fa648c473dad3228cb37e8>`_  2022-08-19   ``Don't use Pandas for SQLTableCheckOperator (#25822)``
=================================================================================================  ===========  ========================================================================

1.1.0
.....

Latest change: 2022-08-15

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`7d0525a55b <https://github.com/apache/airflow/commit/7d0525a55b93e5c8de8a9ef0c8dde0f9c93bb80c>`_  2022-08-15   ``Prepare documentation for RC4 release of providers (#25720)``
`7a19651369 <https://github.com/apache/airflow/commit/7a19651369790e2abb563d96a42f41ec31ebfb85>`_  2022-08-15   ``Fix SQL split string to include ';-less' statements (#25713)``
`5923788143 <https://github.com/apache/airflow/commit/5923788143e7871b56de5164b96a407b2fba75b8>`_  2022-08-10   ``Fix CHANGELOG for common.sql provider and add amazon commit (#25636)``
`d82436b382 <https://github.com/apache/airflow/commit/d82436b382c41643a7385af8a58c50c106b0d01a>`_  2022-08-05   ``Fix fetch_all_handler & db-api tests for it (#25430)``
`348a28957a <https://github.com/apache/airflow/commit/348a28957ae9c4601d69be4f312dae07a6a521a7>`_  2022-08-04   ``Align Common SQL provider logo location (#25538)``
`acab8f52dd <https://github.com/apache/airflow/commit/acab8f52dd8d90fd6583779127895dd343780f79>`_  2022-07-29   ``Move all "old" SQL operators to common.sql providers (#25350)``
`b0fd105f4a <https://github.com/apache/airflow/commit/b0fd105f4ade9933476470f6e247dd5fa518ffc9>`_  2022-07-28   ``Allow Legacy SqlSensor to use the common.sql providers (#25293)``
`5d4abbd58c <https://github.com/apache/airflow/commit/5d4abbd58c33e7dfa8505e307d43420459d3df55>`_  2022-07-27   ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
`df00436569 <https://github.com/apache/airflow/commit/df00436569bb6fb79ce8c0b7ca71dddf02b854ef>`_  2022-07-22   ``Unify DbApiHook.run() method with the methods which override it (#23971)``
`be7cb1e837 <https://github.com/apache/airflow/commit/be7cb1e837b875f44fcf7903329755245dd02dc3>`_  2022-07-22   ``Common SQLCheckOperators Various Functionality Update (#25164)``
=================================================================================================  ===========  ============================================================================

1.0.0
.....

Latest change: 2022-07-07

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
=================================================================================================  ===========  ========================================================
