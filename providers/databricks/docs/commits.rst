
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


Package apache-airflow-providers-databricks
------------------------------------------------------

`Databricks <https://databricks.com/>`__


This is detailed commit list of changes for versions provider package: ``databricks``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.0.0
.....

Latest change: 2022-11-26

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`db5375bea7 <https://github.com/apache/airflow/commit/db5375bea7a0564c12f56c91e1c8c7b6c049698c>`_  2022-11-26   ``Fixing the behaviours of SQL Hooks and Operators finally (#27912)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
`80c327bd3b <https://github.com/apache/airflow/commit/80c327bd3b45807ff2e38d532325bccd6fe0ede0>`_  2022-11-24   ``Bump common.sql provider to 1.3.1 (#27888)``
`ea306c9462 <https://github.com/apache/airflow/commit/ea306c9462615d6b215d43f7f17d68f4c62951b1>`_  2022-11-24   ``Fix errors in Databricks SQL operator introduced when refactoring (#27854)``
`a343bba1e3 <https://github.com/apache/airflow/commit/a343bba1e39a1b28c469974fc87eb106c9f67db8>`_  2022-11-23   ``Fix templating fields and do_xcom_push in DatabricksSQLOperator (#27868)``
=================================================================================================  ===========  ==============================================================================

3.4.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`00af5c007e <https://github.com/apache/airflow/commit/00af5c007ef2200401b53c40236e664758e47f27>`_  2022-11-14   ``Replace urlparse with urlsplit (#27389)``
`eb06c65556 <https://github.com/apache/airflow/commit/eb06c655561737a82d6f99b233c28bbc7f32a28d>`_  2022-11-11   ``Use new job search API for triggering Databricks job by name (#27446)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`ecd4d6654f <https://github.com/apache/airflow/commit/ecd4d6654ff8e0da4a7b8f29fd23c37c9c219076>`_  2022-10-18   ``Add SQLExecuteQueryOperator (#25717)``
=================================================================================================  ===========  =========================================================================

3.3.0
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`89e44c46ad <https://github.com/apache/airflow/commit/89e44c46add19b37e82d0769ce08d57885732856>`_  2022-09-27   ``Remove duplicated connection-type within the provider (#26628)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`5066844513 <https://github.com/apache/airflow/commit/50668445137e4037bb4a3b652bec22e53d1eddd7>`_  2022-09-09   ``D400 first line should end with period batch02 (#25268)``
`25a9c6a905 <https://github.com/apache/airflow/commit/25a9c6a9058b829fc038fdd3fc789890e563bd1d>`_  2022-08-26   ``DatabricksSubmitRunOperator dbt task support (#25623)``
`9535ec0bba <https://github.com/apache/airflow/commit/9535ec0bbae112f78f0e8ccde6b5aff39f3fa75b>`_  2022-08-22   ``Databricks: fix provider name in the User-Agent string (#25873)``
`ca9229b6fe <https://github.com/apache/airflow/commit/ca9229b6fe7eda198c7ce32da13afb97ab9f3e28>`_  2022-08-18   ``Add common-sql lower bound for common-sql (#25789)``
=================================================================================================  ===========  ====================================================================================

3.2.0
.....

Latest change: 2022-08-15

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`7d0525a55b <https://github.com/apache/airflow/commit/7d0525a55b93e5c8de8a9ef0c8dde0f9c93bb80c>`_  2022-08-15   ``Prepare documentation for RC4 release of providers (#25720)``
`4d32f61fd0 <https://github.com/apache/airflow/commit/4d32f61fd049889b49b4ce8b664d8e134aecb053>`_  2022-08-12   ``Databricks: Fix provider for Airflow 2.2.x (#25674)``
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`52f2f5bfa8 <https://github.com/apache/airflow/commit/52f2f5bfa8ac83b5514f82ba22c710d659dc0b2f>`_  2022-08-07   ``Databricks: update user-agent string (#25578)``
`0255a0a5e7 <https://github.com/apache/airflow/commit/0255a0a5e7b93f2daa3a51792cd38d19d6a373c0>`_  2022-08-04   ``Do not convert boolean values to string in deep_string_coerce function (#25394)``
`679a85325a <https://github.com/apache/airflow/commit/679a85325a73fac814c805c8c34d752ae7a94312>`_  2022-08-03   ``Correctly handle output of the failed tasks (#25427)``
`82f842ffc5 <https://github.com/apache/airflow/commit/82f842ffc56817eb039f1c4f1e2c090e6941c6af>`_  2022-07-28   ``updated documentation for databricks operator (#24599)``
`54a8c4fd2a <https://github.com/apache/airflow/commit/54a8c4fd2a1d1af6166f43d588dca8ce24bd058b>`_  2022-07-27   ``More improvements in the Databricks operators (#25260)``
`7438707747 <https://github.com/apache/airflow/commit/7438707747db20ace6afa38900d111df8611c558>`_  2022-07-26   ``Improved telemetry for Databricks provider (#25115)``
`df00436569 <https://github.com/apache/airflow/commit/df00436569bb6fb79ce8c0b7ca71dddf02b854ef>`_  2022-07-22   ``Unify DbApiHook.run() method with the methods which override it (#23971)``
`2f70daf5ac <https://github.com/apache/airflow/commit/2f70daf5ac36100ff0bbd4ac66ce921a2bc6dea0>`_  2022-07-18   ``Databricks: fix test_connection implementation (#25114)``
=================================================================================================  ===========  ===================================================================================

3.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`8dfe7bf5ff <https://github.com/apache/airflow/commit/8dfe7bf5ff090a675353a49da21407dffe2fc15e>`_  2022-07-11   ``Added databricks_conn_id as templated field (#24945)``
`acaa0635c8 <https://github.com/apache/airflow/commit/acaa0635c8477c98ab78da9f6d86e6f1bad2737d>`_  2022-07-08   ``Automatically detect if non-lazy logging interpolation is used (#24910)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`96b01a8012 <https://github.com/apache/airflow/commit/96b01a8012d164df7c24c460149d3b79ecad3901>`_  2022-07-05   ``Remove "bad characters" from our codebase (#24841)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`ed37c3a0e8 <https://github.com/apache/airflow/commit/ed37c3a0e87f64e6942497c5d4c15078a5e02d16>`_  2022-06-28   ``Add 'test_connection' method to Databricks hook (#24617)``
`9c59831ee7 <https://github.com/apache/airflow/commit/9c59831ee78f14de96421c74986933c494407afa>`_  2022-06-21   ``Update providers to use functools compat for ''cached_property'' (#24582)``
=================================================================================================  ===========  =============================================================================

3.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  =======================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`ddf9013098 <https://github.com/apache/airflow/commit/ddf9013098b09176d7b34861b2357ded50b9fe26>`_  2022-06-05   ``AIP-47 - Migrate databricks DAGs to new design #22442 (#24203)``
`acf89510cd <https://github.com/apache/airflow/commit/acf89510cd5a18d15c1a45e674ba0bcae9293097>`_  2022-06-04   ``fix: DatabricksSubmitRunOperator and DatabricksRunNowOperator cannot define .json as template_ext (#23622) (#23641)``
`92ddcf4ac6 <https://github.com/apache/airflow/commit/92ddcf4ac6fa452c5056b1f7cad1fca4d5759802>`_  2022-05-27   ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
`6150d28323 <https://github.com/apache/airflow/commit/6150d283234b48f86362fd4da856e282dd91ebb4>`_  2022-05-22   ``Add Deferrable Databricks operators (#19736)``
`cf5a78e91c <https://github.com/apache/airflow/commit/cf5a78e91cb920e7014b76914956681aeb44b29f>`_  2022-05-22   ``Fix UnboundLocalError when sql is empty list in DatabricksSqlHook (#23815)``
`d0a5b3a4f2 <https://github.com/apache/airflow/commit/d0a5b3a4f25b736661693c73ea4df0e7d445a778>`_  2022-05-13   ``Add git_source to DatabricksSubmitRunOperator (#23620)``
=================================================================================================  ===========  =======================================================================================================================

2.7.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`428a439953 <https://github.com/apache/airflow/commit/428a43995390b3623a51aa7bac7e21da69a8db22>`_  2022-05-09   ``Clean up in-line f-string concatenation (#23591)``
`a58506b2a6 <https://github.com/apache/airflow/commit/a58506b2a68f0d4533b41feb67efb0caf34e14d8>`_  2022-04-26   ``Address review comments``
`6a3d6cc32b <https://github.com/apache/airflow/commit/6a3d6cc32b4e3922d259c889460fe82e0ebf3663>`_  2022-04-26   ``Update to the released version of DBSQL connector``
`7b3bf4e435 <https://github.com/apache/airflow/commit/7b3bf4e43558999af29a4ce7f60f2f9ef55f2ebf>`_  2022-04-26   ``DatabricksSqlOperator - switch to databricks-sql-connector 2.x``
`f02b0b6b40 <https://github.com/apache/airflow/commit/f02b0b6b4054bd3038fc3fec85adef7502ea0c3c>`_  2022-04-25   ``Further improvement of Databricks Jobs operators (#23199)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
=================================================================================================  ===========  ===========================================================================

2.6.0
.....

Latest change: 2022-04-13

=================================================================================================  ===========  ===============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================
`40831144be <https://github.com/apache/airflow/commit/40831144bedd3e652d8856b918a26d2e0a8e8e02>`_  2022-04-13   ``Prepare for RC2 release of March Databricks provider (#22979)``
`7be57eb256 <https://github.com/apache/airflow/commit/7be57eb2566651de89048798766f0ad5f267cdc2>`_  2022-04-10   ``Databricks SQL operators are now Python 3.10 compatible (#22886)``
`aa8c08db38 <https://github.com/apache/airflow/commit/aa8c08db383ebfabf30a7c2b2debb64c0968df48>`_  2022-04-10   ``Databricks: Correctly handle HTTP exception (#22885)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`1b12c93ed3 <https://github.com/apache/airflow/commit/1b12c93ed3efa6a7d42e4f1bfa28376e23739ba1>`_  2022-03-31   ``Refactor 'DatabricksJobRunLink' to not create ad hoc TaskInstances (#22571)``
`95169d1d07 <https://github.com/apache/airflow/commit/95169d1d07e66a8c7647e5b0f6a14cea57d515fc>`_  2022-03-27   ``Add a link to Databricks Job Run (#22541)``
`352d7f72dd <https://github.com/apache/airflow/commit/352d7f72dd1e21f1522d69b71917142430548d66>`_  2022-03-27   ``More operators for Databricks Repos (#22422)``
`c063fc688c <https://github.com/apache/airflow/commit/c063fc688cf20c37ed830de5e3dac4a664fd8241>`_  2022-03-25   ``Update black precommit (#22521)``
=================================================================================================  ===========  ===============================================================================

2.5.0
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`cc920963a6 <https://github.com/apache/airflow/commit/cc920963a69aca840394c3c9e60e0c53235a6fe6>`_  2022-03-15   ``Operator for updating Databricks Repos (#22278)``
=================================================================================================  ===========  ==============================================================

2.4.0
.....

Latest change: 2022-03-14

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`12e9e2c695 <https://github.com/apache/airflow/commit/12e9e2c695f9ebb9d3dde9c0f7dfaa112654f0d6>`_  2022-03-14   ``Databricks hook - retry on HTTP Status 429 as well (#21852)``
`af9d85ccd8 <https://github.com/apache/airflow/commit/af9d85ccd8abdc3c252c19764d3ea16970ae0f20>`_  2022-03-13   ``Skip some tests for Databricks from running on Python 3.10 (#22221)``
`4014194320 <https://github.com/apache/airflow/commit/401419432082d222b823e4f2a66f21e5cc3ab28d>`_  2022-03-08   ``Add new options to DatabricksCopyIntoOperator (#22076)``
=================================================================================================  ===========  =======================================================================

2.3.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`62bf1276f6 <https://github.com/apache/airflow/commit/62bf1276f6b6de00779e13749ab92a67890d23f4>`_  2022-03-01   ``Add-showing-runtime-error-feature-to-DatabricksSubmitRunOperator (#21709)``
`27d19e7626 <https://github.com/apache/airflow/commit/27d19e7626ef80687997a6799762fa00162c1328>`_  2022-02-27   ``Databricks SQL operators (#21363)``
`a1845c68f9 <https://github.com/apache/airflow/commit/a1845c68f9a04e61dd99ccc0a23d17a277babf57>`_  2022-02-26   ``Databricks: add support for triggering jobs by name (#21663)``
`7cca82495b <https://github.com/apache/airflow/commit/7cca82495b38d9e3c52a086958f07719981eb1cd>`_  2022-02-15   ``Updated Databricks docs for correct jobs 2.1 API and links (#21494)``
`0a2d0d1ecb <https://github.com/apache/airflow/commit/0a2d0d1ecbb7a72677f96bc17117799ab40853e0>`_  2022-02-12   ``Added template_ext = ('.json') to databricks operators #18925 (#21530)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
=================================================================================================  ===========  =============================================================================

2.2.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`0bf424f37f <https://github.com/apache/airflow/commit/0bf424f37fc2786e7a74e7f1df88dc92538abbd4>`_  2021-12-30   ``Fix mypy databricks operator (#20598)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`c5c18c54fa <https://github.com/apache/airflow/commit/c5c18c54fa83463bc953249dc28edcbf7179da17>`_  2021-12-29   ``Databricks: fix verification of Managed Identity (#20550)``
`d3b3161f0d <https://github.com/apache/airflow/commit/d3b3161f0da47975e779255806a0fb0019cd38df>`_  2021-12-28   ``Remove 'host' as an instance attr in 'DatabricksHook' (#20540)``
`58afc19377 <https://github.com/apache/airflow/commit/58afc193776a8e811e9a210a18f93dabebc904d4>`_  2021-12-28   ``Add 'wait_for_termination' argument for Databricks Operators (#20536)``
`e7659d08b0 <https://github.com/apache/airflow/commit/e7659d08b0ca83913bc958f54658385ac77e366a>`_  2021-12-27   ``Update connection object to ''cached_property'' in ''DatabricksHook'' (#20526)``
`cad39274d9 <https://github.com/apache/airflow/commit/cad39274d9a8eceba2845dc39e8c870959746478>`_  2021-12-14   ``Fix MyPy Errors for Databricks provider. (#20265)``
=================================================================================================  ===========  ==================================================================================

2.1.0
.....

Latest change: 2021-12-10

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`820bfed515 <https://github.com/apache/airflow/commit/820bfed515bd7d6b2fb7aaa31b2e23f98454f870>`_  2021-12-10   ``Prepare docs for provider's RC2 release (#20205)``
`66f94f95c2 <https://github.com/apache/airflow/commit/66f94f95c2e92baad2761b5a1fa405e36c17808a>`_  2021-12-10   ``Remove db call from 'DatabricksHook.__init__()' (#20180)``
`545ca59ba9 <https://github.com/apache/airflow/commit/545ca59ba9a0b346cbbf28cc6958f9575e5e6b0b>`_  2021-12-08   ``Unhide changelog entry for databricks (#20128)``
`637db1a0ba <https://github.com/apache/airflow/commit/637db1a0ba9c8173372f1f5d6f60ec4c4f3699d8>`_  2021-12-07   ``Update documentation for RC2 release of November Databricks Provider (#20086)``
`728e94a47e <https://github.com/apache/airflow/commit/728e94a47e0048829ce67096235d34019be9fac7>`_  2021-12-05   ``Refactor DatabricksHook (#19835)``
`4925b37b66 <https://github.com/apache/airflow/commit/4925b37b661a1117dc9f1a10be11f03e67e1a413>`_  2021-12-04   ``Databricks hook: fix expiration time check (#20036)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`11998848a4 <https://github.com/apache/airflow/commit/11998848a4b07f255ae8fcd78d6ad549dabea7e6>`_  2021-11-24   ``Databricks: add more methods to represent run state information (#19723)``
`56bdfe7a84 <https://github.com/apache/airflow/commit/56bdfe7a840c25360d596ca94fd11d2ccfadb4ba>`_  2021-11-22   ``Databricks - allow Azure SP authentication on other Azure clouds (#19722)``
`244627e3da <https://github.com/apache/airflow/commit/244627e3daa3e416696e5ddb20a2d4ea5e16b96e>`_  2021-11-14   ``Databricks: allow to specify PAT in Password field (#19585)``
`0a4a8bdb94 <https://github.com/apache/airflow/commit/0a4a8bdb943979820fa7067797764e47f3e0b0c3>`_  2021-11-14   ``Databricks jobs 2.1 (#19544)``
`8ae878953b <https://github.com/apache/airflow/commit/8ae878953b183b2689481f5e5806bc2ccca4c509>`_  2021-11-09   ``Update Databricks API from 2.0 to 2.1 (#19412)``
`28b51fb7bd <https://github.com/apache/airflow/commit/28b51fb7bd886e6a2de216d877cc69147441818e>`_  2021-11-08   ``Authentication with AAD tokens in Databricks provider (#19335)``
`3a0c455855 <https://github.com/apache/airflow/commit/3a0c4558558689d7498fe2fc171ad9a8e132119e>`_  2021-11-07   ``Update Databricks operators to match latest version of API 2.0 (#19443)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`f5ad26dcdd <https://github.com/apache/airflow/commit/f5ad26dcdd7bcb724992528dce71056965b94d26>`_  2021-10-21   ``Fixup string concatenations (#19099)``
=================================================================================================  ===========  =================================================================================

2.0.2
.....

Latest change: 2021-09-30

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`0b7b13372f <https://github.com/apache/airflow/commit/0b7b13372f6dbf18a35d5346d3955f65b31dd00d>`_  2021-09-18   ``Move DB call out of ''DatabricksHook.__init__'' (#18339)``
=================================================================================================  ===========  ======================================================================================

2.0.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
=================================================================================================  ===========  ============================================================================

2.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`df143aee8d <https://github.com/apache/airflow/commit/df143aee8d9e7e0089b747bdd27addf63bb4962f>`_  2021-04-29   ``An initial rework of the "Concepts" docs (#15444)``
`49cae1f052 <https://github.com/apache/airflow/commit/49cae1f052ab86369bbc28eb8aba5166b7be7711>`_  2021-04-17   ``Add documentation for Databricks connection (#15410)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
=================================================================================================  ===========  =================================================================

1.0.1
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  ========================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f121 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`b027223132 <https://github.com/apache/airflow/commit/b0272231320a4975cc39968dec8f0abf7a5cca11>`_  2020-11-13   ``Add install/uninstall api to databricks hook (#12316)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`7e0d08e1f0 <https://github.com/apache/airflow/commit/7e0d08e1f074871307f0eb9e9ae7a66f7ce67626>`_  2020-11-09   ``Add how-to Guide for Databricks operators (#12175)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`54353f8745 <https://github.com/apache/airflow/commit/54353f874589f9be236458995147d13e0e763ffc>`_  2020-09-27   ``Increase type coverage for five different providers (#11170)``
`966a06d96b <https://github.com/apache/airflow/commit/966a06d96bbfe330f1d2825f7b7eaa16d43b7a00>`_  2020-09-18   ``Fetching databricks host from connection if not supplied in extras. (#10762)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`bfefcce0c9 <https://github.com/apache/airflow/commit/bfefcce0c9f273042dd79ff50eb9af032ecacf59>`_  2020-08-25   ``Updated REST API call so GET requests pass payload in query string instead of request body (#10462)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`2f2d8dbfaf <https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148>`_  2020-08-25   ``Remove all "noinspection" comments native to IntelliJ (#10525)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`e13a14c873 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`f1073381ed <https://github.com/apache/airflow/commit/f1073381ed764a218b2502d15ca28a5b326f9f2d>`_  2020-05-22   ``Add support for spark python and submit tasks in Databricks operator(#8846)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`649935e8ce <https://github.com/apache/airflow/commit/649935e8ce906759fdd08884ab1e3db0a03f6953>`_  2020-04-27   ``[AIRFLOW-8472]: 'PATCH' for Databricks hook '_do_api_call' (#8473)``
`16903ba3a6 <https://github.com/apache/airflow/commit/16903ba3a6ee5e61f1c6b5d17a8c6cf3c3a9a7f6>`_  2020-04-24   ``[AIRFLOW-8474]: Adding possibility to get job_id from Databricks run (#8475)``
`5648dfbc30 <https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06>`_  2020-03-23   ``Add missing call to Super class in 'amazon', 'cloudant & 'databricks' providers (#7827)``
`3320e432a1 <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`4d03e33c11 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`83c037873f <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`c42a375e79 <https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8>`_  2020-01-27   ``[AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)``
=================================================================================================  ===========  ======================================================================================================================================================================
