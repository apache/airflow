
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



1.8.0
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`128f6b9e40 <https://github.com/apache/airflow/commit/128f6b9e40c4cf96f900629294175f9c5babd703>`_  2023-10-13   ``Add missing header into 'common.sql' changelog (#34910)``
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
`7ebf4220c9 <https://github.com/apache/airflow/commit/7ebf4220c9abd001f1fa23c95f882efddd5afbac>`_  2023-09-28   ``Refactor usage of str() in providers (#34320)``
`659d94f0ae <https://github.com/apache/airflow/commit/659d94f0ae89f47a7d4b95d6c19ab7f87bd3a60f>`_  2023-09-21   ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``
`f5c2748c33 <https://github.com/apache/airflow/commit/f5c2748c3346bdebf445afd615657af8849345dd>`_  2023-09-08   ``fix(providers/sql): respect soft_fail argument when exception is raised (#34199)``
=================================================================================================  ===========  ====================================================================================

1.7.2
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`a7310f9c91 <https://github.com/apache/airflow/commit/a7310f9c9127cf87a71e0bfa141c066d6a0bc82b>`_  2023-09-05   ``Refactor regex in providers (#33898)``
`d757f6a3af <https://github.com/apache/airflow/commit/d757f6a3af24c3ec0d48c8c983d6ba5d6ed2202e>`_  2023-09-03   ``Fix BigQueryValueCheckOperator deferrable mode optimisation (#34018)``
=================================================================================================  ===========  ========================================================================

1.7.1
.....

Latest change: 2023-08-26

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`92474db6a5 <https://github.com/apache/airflow/commit/92474db6a5321a0c0cd0dc21695f95d51c3aad16>`_  2023-08-23   ``Refactor: Better percentage formatting (#33595)``
`a54c2424df <https://github.com/apache/airflow/commit/a54c2424df51bf1acec420f4792a237dabcfa12b>`_  2023-08-23   ``Fix typos (double words and it's/its) (#33623)``
`a91ee7ac2f <https://github.com/apache/airflow/commit/a91ee7ac2fe29f460a4e4b0d8c1346f40672be43>`_  2023-08-20   ``Refactor: Simplify code in smaller providers (#33234)``
=================================================================================================  ===========  ============================================================

1.7.0
.....

Latest change: 2023-08-11

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`b5a4d36383 <https://github.com/apache/airflow/commit/b5a4d36383c4143f46e168b8b7a4ba2dc7c54076>`_  2023-08-11   ``Prepare docs for Aug 2023 2nd wave of Providers (#33291)``
`9736143468 <https://github.com/apache/airflow/commit/9736143468cfe034e65afb3df3031ab3626f0f6d>`_  2023-08-07   ``Add a new parameter to SQL operators to specify conn id field (#30784)``
=================================================================================================  ===========  ==========================================================================

1.6.2
.....

Latest change: 2023-08-05

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`60677b0ba3 <https://github.com/apache/airflow/commit/60677b0ba3c9e81595ec2aa3d4be2737e5b32054>`_  2023-08-05   ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
`cfac7d379f <https://github.com/apache/airflow/commit/cfac7d379f43d8d15da65cae8620322dfd0043d6>`_  2023-08-04   ``Make SQLExecute Query signature consistent with other SQL operators (#32974)``
`e3d82c6be0 <https://github.com/apache/airflow/commit/e3d82c6be0e0e1468ade053c37690aa1e0e4882d>`_  2023-08-04   ``Get rid of Python2 numeric relics (#33050)``
=================================================================================================  ===========  ================================================================================

1.6.1
.....

Latest change: 2023-07-29

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`d06b7af69a <https://github.com/apache/airflow/commit/d06b7af69a65c50321ba2a9904551f3b8affc7f1>`_  2023-07-29   ``Prepare docs for July 2023 3rd wave of Providers (#32875)``
`ce2841bf6a <https://github.com/apache/airflow/commit/ce2841bf6ab609f31cb04aea9a39473de281bf24>`_  2023-07-25   ``Add default port to Openlineage authority method. (#32828)``
`60c49ab2df <https://github.com/apache/airflow/commit/60c49ab2dfabaf450b80a5c7569743dd383500a6>`_  2023-07-19   ``Add more accurate typing for DbApiHook.run method (#31846)``
`ef0ed1aacc <https://github.com/apache/airflow/commit/ef0ed1aacc208be9e52a35211d2beaefb735173a>`_  2023-07-06   ``Fix local OpenLineage import in 'SQLExecuteQueryOperator'. (#32400)``
=================================================================================================  ===========  =======================================================================

1.6.0
.....

Latest change: 2023-07-06

=================================================================================================  ===========  =================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================================
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`ee4a838d49 <https://github.com/apache/airflow/commit/ee4a838d49461b3b053a9cbe660dbff06a17fff5>`_  2023-07-05   ``Pass SQLAlchemy engine to construct information schema query. (#32371)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`f2e2125b07 <https://github.com/apache/airflow/commit/f2e2125b070794b6a66fb3e2840ca14d07054cf2>`_  2023-06-29   ``openlineage, common.sql:  provide OL SQL parser as internal OpenLineage provider API (#31398)``
`8c37b74a20 <https://github.com/apache/airflow/commit/8c37b74a208a808d905c1b86d081d69d7a1aa900>`_  2023-06-28   ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  =================================================================================================

1.5.2
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`9276310a43 <https://github.com/apache/airflow/commit/9276310a43d17a9e9e38c2cb83686a15656896b2>`_  2023-06-05   ``Improve docstrings in providers (#31681)``
`a59076eaee <https://github.com/apache/airflow/commit/a59076eaeed03dd46e749ad58160193b4ef3660c>`_  2023-06-02   ``Add D400 pydocstyle check - Providers (#31427)``
`9fa75aaf7a <https://github.com/apache/airflow/commit/9fa75aaf7a391ebf0e6b6949445c060f6de2ceb9>`_  2023-05-29   ``Remove Python 3.7 support (#30963)``
=================================================================================================  ===========  =============================================================

1.5.1
.....

Latest change: 2023-05-24

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`d745cee3db <https://github.com/apache/airflow/commit/d745cee3dbde6b437a817aa64e385a1a948389d5>`_  2023-05-24   ``Prepare adhoc wave of Providers (#31478)``
`547e352578 <https://github.com/apache/airflow/commit/547e352578fac92f072b269dc257d21cdc279d97>`_  2023-05-23   ``Bring back min-airflow-version for preinstalled providers (#31469)``
=================================================================================================  ===========  ======================================================================

1.5.0
.....

Latest change: 2023-05-19

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`45548b9451 <https://github.com/apache/airflow/commit/45548b9451fba4e48c6f0c0ba6050482c2ea2956>`_  2023-05-19   ``Prepare RC2 docs for May 2023 wave of Providers (#31416)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`edd7133a13 <https://github.com/apache/airflow/commit/edd7133a1336c9553d77ba13c83bc7f48d4c63f0>`_  2023-05-09   ``Add conditional output processing in SQL operators (#31136)``
`00a527f671 <https://github.com/apache/airflow/commit/00a527f67111cc4f2bb03ff374f21b9f4930727c>`_  2023-05-08   ``Remove noisy log from SQL table check (#31037)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  ======================================================================================

1.4.0
.....

Latest change: 2023-04-02

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`55dbf1ff1f <https://github.com/apache/airflow/commit/55dbf1ff1fb0b22714f695a66f6108b3249d1199>`_  2023-04-02   ``Prepare docs for April 2023 wave of Providers (#30378)``
`a9b79a27b2 <https://github.com/apache/airflow/commit/a9b79a27b25a47c7e0390c139b517f229fdacd12>`_  2023-03-08   ``Add option to show output of 'SQLExecuteQueryOperator' in the log (#29954)``
`95710e0cdd <https://github.com/apache/airflow/commit/95710e0cdd54d3ac37d0148466705a81b31bcb7f>`_  2023-03-03   ``Fix Python API docs formatting for Common SQL provider (#29863)``
=================================================================================================  ===========  ==============================================================================

1.3.4
.....

Latest change: 2023-03-03

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`fcd3c0149f <https://github.com/apache/airflow/commit/fcd3c0149f17b364dfb94c0523d23e3145976bbe>`_  2023-03-03   ``Prepare docs for 03/2023 wave of Providers (#29878)``
`19f1e7c27b <https://github.com/apache/airflow/commit/19f1e7c27b85e297497842c73f13533767ebd6ba>`_  2023-02-22   ``Do not process output when do_xcom_push=False  (#29599)``
`0af6f20c5f <https://github.com/apache/airflow/commit/0af6f20c5f36c6cac3fc1b23ff47763ea2c24ba2>`_  2023-01-30   ``Make the S3-to-SQL system test self-contained (#29204)``
`129f0820cd <https://github.com/apache/airflow/commit/129f0820cd03c721ebebe3461489f255bb9e752c>`_  2023-01-23   ``Make static checks generated file  more stable accross the board (#29080)``
=================================================================================================  ===========  =============================================================================

1.3.3
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`9a7f07491e <https://github.com/apache/airflow/commit/9a7f07491e603123182adfd5706fbae524e33c0d>`_  2023-01-09   ``Handle non-compliant behaviour of Exasol cursor (#28744)``
=================================================================================================  ===========  ==================================================================

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
