
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



1.3.3
.....

Latest change: 2023-01-09

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`9a7f07491e <https://github.com/apache/airflow/commit/9a7f07491e603123182adfd5706fbae524e33c0d>`_  2023-01-09   ``Handle non-compliant behaviour of Exasol cursor (#28744)``
=================================================================================================  ===========  ============================================================

1.3.2
.....

Latest change: 2023-01-02

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`2e7b9f5504 <https://github.com/apache/airflow/commit/2e7b9f550403cc6937b3210aaaf9e80e3e944445>`_  2022-12-29   ``Defer to hook setting for split_statements in SQLExecuteQueryOperator (#28635)``
`f115b207bc <https://github.com/apache/airflow/commit/f115b207bc844c10569b2df6fc9acfa32a3c7f41>`_  2022-12-18   ``fIx isort problems introduced by recent isort release (#28434)``
`a6cda7cd23 <https://github.com/apache/airflow/commit/a6cda7cd230ef22f7fe042d6d5e9f78c660c4a75>`_  2022-12-10   ``Fix template rendering for Common SQL operators (#28202)``
`6852f3fbea <https://github.com/apache/airflow/commit/6852f3fbea5dd0fa6b8a289d2f9f11dd2159053d>`_  2022-12-05   ``Add pre-commits preventing accidental API changes in common.sql (#27962)``
`a158fbb6bd <https://github.com/apache/airflow/commit/a158fbb6bde07cd20003680a4cf5e7811b9eda98>`_  2022-11-28   ``Clarify docstrings for updated DbApiHook (#27966)``
=================================================================================================  ===========  ==================================================================================

1.3.1
.....

Latest change: 2022-11-26

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`25bdbc8e67 <https://github.com/apache/airflow/commit/25bdbc8e6768712bad6043618242eec9c6632618>`_  2022-11-26   ``Updated docs for RC3 wave of providers (#27937)``
`db5375bea7 <https://github.com/apache/airflow/commit/db5375bea7a0564c12f56c91e1c8c7b6c049698c>`_  2022-11-26   ``Fixing the behaviours of SQL Hooks and Operators finally (#27912)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
`80c327bd3b <https://github.com/apache/airflow/commit/80c327bd3b45807ff2e38d532325bccd6fe0ede0>`_  2022-11-24   ``Bump common.sql provider to 1.3.1 (#27888)``
`ea306c9462 <https://github.com/apache/airflow/commit/ea306c9462615d6b215d43f7f17d68f4c62951b1>`_  2022-11-24   ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
`dbb4b59dcb <https://github.com/apache/airflow/commit/dbb4b59dcbc8b57243d1588d45a4d2717c3e7758>`_  2022-11-23   ``Restore removed (but used) methods in common.sql (#27843)``
=================================================================================================  ===========  ==============================================================================

1.3.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`3ae98b824d <https://github.com/apache/airflow/commit/3ae98b824db437b2db928a73ac8b50c0a2f80124>`_  2022-11-14   ``Use unused SQLCheckOperator.parameters in SQLCheckOperator.execute. (#27599)``
`5c37b503f1 <https://github.com/apache/airflow/commit/5c37b503f118b8ad2585dff9949dd8fdb96689ed>`_  2022-10-31   ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`87eb46bbc6 <https://github.com/apache/airflow/commit/87eb46bbc69c20148773d72e990fbd5d20076342>`_  2022-10-26   ``Common sql bugfixes and improvements (#26761)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`ecd4d6654f <https://github.com/apache/airflow/commit/ecd4d6654ff8e0da4a7b8f29fd23c37c9c219076>`_  2022-10-18   ``Add SQLExecuteQueryOperator (#25717)``
`76014609c0 <https://github.com/apache/airflow/commit/76014609c07bfa307ef7598794d1c0404c5279bd>`_  2022-10-09   ``DbApiHook consistent insert_rows logging (#26758)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
=================================================================================================  ===========  ====================================================================================

1.2.0
.....

Latest change: 2022-09-05

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`25d0baa4ee <https://github.com/apache/airflow/commit/25d0baa4ee69769ff339931f76ebace28c4315f2>`_  2022-09-05   ``Prepare bug-fix release of providers out of band (#26109)``
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
