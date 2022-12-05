
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


Package apache-airflow-providers-snowflake
------------------------------------------------------

`Snowflake <https://www.snowflake.com/>`__


This is detailed commit list of changes for versions provider package: ``snowflake``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.0.2
.....

Latest change: 2022-12-01

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`d9cefcd0c5 <https://github.com/apache/airflow/commit/d9cefcd0c50a1cce1c3c8e9ecb99cfacde5eafbf>`_  2022-12-01   ``Make Snowflake Hook conform to semantics of DBApi (#28006)``
`2e7a4bcb55 <https://github.com/apache/airflow/commit/2e7a4bcb550538283f28550208b01515d348fb51>`_  2022-11-30   ``Fix wrapping of run() method result of exasol and snoflake DB hooks (#27997)``
=================================================================================================  ===========  ================================================================================

4.0.1
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
=================================================================================================  ===========  ==============================================================================

4.0.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`3ae98b824d <https://github.com/apache/airflow/commit/3ae98b824db437b2db928a73ac8b50c0a2f80124>`_  2022-11-14   ``Use unused SQLCheckOperator.parameters in SQLCheckOperator.execute. (#27599)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`82e9ed7aca <https://github.com/apache/airflow/commit/82e9ed7aca371247f9f7fe882d7ad157cb4859d8>`_  2022-10-22   ``Update snowflake hook to not use extra prefix (#26764)``
`ecd4d6654f <https://github.com/apache/airflow/commit/ecd4d6654ff8e0da4a7b8f29fd23c37c9c219076>`_  2022-10-18   ``Add SQLExecuteQueryOperator (#25717)``
=================================================================================================  ===========  ================================================================================

3.3.0
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`6a615ee477 <https://github.com/apache/airflow/commit/6a615ee47755e851854970d7042ee00d0040c8dc>`_  2022-08-30   ``Fix wrong deprecation warning for 'S3ToSnowflakeOperator' (#26047)``
`9e12d483bc <https://github.com/apache/airflow/commit/9e12d483bcde714ca4225c94df182c4eacd36f7c>`_  2022-08-27   ``Add custom handler param in SnowflakeOperator (#25983)``
`5c52bbf32d <https://github.com/apache/airflow/commit/5c52bbf32d81291b57d051ccbd1a2479ff706efc>`_  2022-08-27   ``copy into snowflake from external stage (#25541)``
=================================================================================================  ===========  ====================================================================================

3.2.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`acab8f52dd <https://github.com/apache/airflow/commit/acab8f52dd8d90fd6583779127895dd343780f79>`_  2022-07-29   ``Move all "old" SQL operators to common.sql providers (#25350)``
`df00436569 <https://github.com/apache/airflow/commit/df00436569bb6fb79ce8c0b7ca71dddf02b854ef>`_  2022-07-22   ``Unify DbApiHook.run() method with the methods which override it (#23971)``
=================================================================================================  ===========  ============================================================================

3.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`13908c2c91 <https://github.com/apache/airflow/commit/13908c2c914cf08f9d962a4d3b6ae54fbdf1d223>`_  2022-06-29   ``Adding generic 'SqlToSlackOperator' (#24663)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`8a34d25049 <https://github.com/apache/airflow/commit/8a34d25049a060a035d4db4a49cd4a0d0b07fb0b>`_  2022-06-26   ``S3ToSnowflakeOperator: escape single quote in s3_keys (#24607)``
`66e84001df <https://github.com/apache/airflow/commit/66e84001df069c76ba8bfefe15956c4018844b92>`_  2022-06-22   ``Pattern parameter in S3ToSnowflakeOperator (#24571)``
=================================================================================================  ===========  ==================================================================

3.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`c2f10a4ee9 <https://github.com/apache/airflow/commit/c2f10a4ee9c2404e545d78281bf742a199895817>`_  2022-06-03   ``Migrate Snowflake system tests to new design #22434 (#24151)``
`86cfd1244a <https://github.com/apache/airflow/commit/86cfd1244a641a8f17c9b33a34399d9be264f556>`_  2022-05-20   ``Fix error when SnowflakeHook take empty list in 'sql' param (#23767)``
=================================================================================================  ===========  ==================================================================================

2.7.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`b6aaf9e2fc <https://github.com/apache/airflow/commit/b6aaf9e2fc40724c9904504e121633baab2396e1>`_  2022-05-01   ``Allow multiline text in private key field for Snowflake (#23066)``
=================================================================================================  ===========  ====================================================================

2.6.0
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`d6ed9cb950 <https://github.com/apache/airflow/commit/d6ed9cb95041285b1250039377e968329d9ca1f1>`_  2022-03-15   ``Add support for private key in connection for Snowflake (#22266)``
=================================================================================================  ===========  ====================================================================

2.5.2
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`5d9b088dfa <https://github.com/apache/airflow/commit/5d9b088dfa3267953fb7698358069861bdb2abf1>`_  2022-03-11   ``Remove Snowflake limits (#22181)``
=================================================================================================  ===========  ====================================================================

2.5.1
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
=================================================================================================  ===========  ========================================================

2.5.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`39e395f981 <https://github.com/apache/airflow/commit/39e395f9816c04ef2f033eb0b4f635fc3018d803>`_  2022-02-04   ``Add more SQL template fields renderers (#21237)``
`534e9ae117 <https://github.com/apache/airflow/commit/534e9ae117641b4147542f2deec2a077f0a42e2f>`_  2022-01-28   ``Fix #21096: Support boolean in extra__snowflake__insecure_mode (#21155)``
`cb73053211 <https://github.com/apache/airflow/commit/cb73053211367e2c2dd76d5279cdc7dc7b190124>`_  2022-01-27   ``Add optional features in providers. (#21074)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`9ea459a6bd <https://github.com/apache/airflow/commit/9ea459a6bd8073f16dc197b1147f220293557dc8>`_  2022-01-08   ``Snowflake Provider: Improve tests for Snowflake Hook (#20745)``
=================================================================================================  ===========  ===========================================================================

2.4.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`a632b74846 <https://github.com/apache/airflow/commit/a632b74846bae28408fb4c1b38671fae23ca005c>`_  2021-12-28   ``Improvements for 'SnowflakeHook.get_sqlalchemy_engine'  (#20509)``
`fcc3b92fb6 <https://github.com/apache/airflow/commit/fcc3b92fb6770597c4058c547a49f391de4dba44>`_  2021-12-13   ``Fix MyPy Errors for Snowflake provider. (#20212)``
`89a66ae023 <https://github.com/apache/airflow/commit/89a66ae02319a20d6170187527d4535a26078378>`_  2021-12-13   ``Support insecure mode in SnowflakeHook (#20106)``
`7fb301b0b4 <https://github.com/apache/airflow/commit/7fb301b0b466f470c737ded99b670b3f0605f1a4>`_  2021-12-08   ``Remove unused code in SnowflakeHook (#20107)``
=================================================================================================  ===========  =========================================================================

2.3.1
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`de9900539c <https://github.com/apache/airflow/commit/de9900539c9731325e29fd1bbac37c4bc1363bc4>`_  2021-11-12   ``Remove duplicate get_connection in SnowflakeHook (#19543)``
=================================================================================================  ===========  ======================================================================

2.3.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ===============================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`d53d4f9c7c <https://github.com/apache/airflow/commit/d53d4f9c7c1280970fc4b9ee3240c8d1db5f2c57>`_  2021-10-25   ``Moving the example tag a little bit up to include the part where you specify the snowflake_conn_id (#19180)``
`acfb7b5acf <https://github.com/apache/airflow/commit/acfb7b5acf887d38aa8751c18d17dbfe85e78b7c>`_  2021-10-25   ``Add test_connection method for Snowflake Hook (#19041)``
`0a37be3e3c <https://github.com/apache/airflow/commit/0a37be3e3cf9289f63f1506bc31db409c2b46738>`_  2021-09-30   ``Add region to Snowflake URI. (#18650)``
=================================================================================================  ===========  ===============================================================================================================

2.2.0
.....

Latest change: 2021-09-30

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`a8970764d9 <https://github.com/apache/airflow/commit/a8970764d98f33a54be0e880df27f86b311038ac>`_  2021-09-10   ``Add Snowflake operators based on SQL Checks  (#17741)``
=================================================================================================  ===========  ======================================================================================

2.1.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`97428efc41 <https://github.com/apache/airflow/commit/97428efc41e5902183827fb9e4e56d067ca771df>`_  2021-08-02   ``Fix messed-up changelog in 3 providers (#17380)``
=================================================================================================  ===========  ============================================================================

2.1.0
.....

Latest change: 2021-07-26

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`0dbd0f420c <https://github.com/apache/airflow/commit/0dbd0f420cc08e011317e2a9f21f92fff4a64c1b>`_  2021-07-26   ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`5999cb9a66 <https://github.com/apache/airflow/commit/5999cb9a660fcf54e68d8b331b0d912f71f4836d>`_  2021-07-07   ``Adding: Snowflake Role in snowflake provider hook (#16735)``
`8b41c2e0b9 <https://github.com/apache/airflow/commit/8b41c2e0b982335ee380f732452d133ad2dd7ce9>`_  2021-07-01   ``Logging and returning info about query execution SnowflakeHook (#15736)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
=================================================================================================  ===========  =============================================================================

2.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  =========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`608dd0ddf6 <https://github.com/apache/airflow/commit/608dd0ddf65dac7f7eee2cb54628a93805b7ad66>`_  2021-06-15   ``Fix formatting and missing import (#16455)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`643e46ca7a <https://github.com/apache/airflow/commit/643e46ca7ad0b86ddcdae37ffe5b77d31c46b52f>`_  2021-06-15   ``Added ability for Snowflake to attribute usage to Airflow by adding an application parameter (#16420)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`aeb93f8e5b <https://github.com/apache/airflow/commit/aeb93f8e5bb4a9175e8834d476a6b679beff4a7e>`_  2021-05-27   ``fix: restore parameters support when sql passed to SnowflakeHook as str (#16102)``
`20f3639403 <https://github.com/apache/airflow/commit/20f363940316126e706923ee9caf7172dd4caeb6>`_  2021-05-19   ``Add 'template_fields' to 'S3ToSnowflake' operator (#15926)``
`6f956dc99b <https://github.com/apache/airflow/commit/6f956dc99b6c6393f7b50e9da9f778b5cf0bef88>`_  2021-05-13   ``Allow S3ToSnowflakeOperator to omit schema (#15817)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
=================================================================================================  ===========  =========================================================================================================

1.3.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  ================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`c6be8b113d <https://github.com/apache/airflow/commit/c6be8b113db4c8da65d526e50a249ce5311f5341>`_  2021-04-30   ``Expose snowflake query_id in snowflake hook and operator, support multiple statements in sql string (#15533)``
`814e471d13 <https://github.com/apache/airflow/commit/814e471d137aad68bd64a21d20736e7b88403f97>`_  2021-04-29   ``Update pre-commit checks (#15583)``
`7a0d412245 <https://github.com/apache/airflow/commit/7a0d4122459289e0f2db78ad2849d5ba42df4468>`_  2021-04-25   ``Add Connection Documentation to more Providers (#15408)``
=================================================================================================  ===========  ================================================================================================================

1.2.0
.....

Latest change: 2021-04-06

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`042be2e4e0 <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`9b76b94c94 <https://github.com/apache/airflow/commit/9b76b94c940d472290861930a1d5860b43b3b2b2>`_  2021-04-02   ``A bunch of template_fields_renderers additions (#15130)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`e4bf8f3491 <https://github.com/apache/airflow/commit/e4bf8f34911940937f1e80007adeb47e9a5d4c9c>`_  2021-03-16   ``Add dynamic fields to snowflake connection (#14724)``
=================================================================================================  ===========  =============================================================================

1.1.1
.....

Latest change: 2021-03-08

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`b753c7fa60 <https://github.com/apache/airflow/commit/b753c7fa60e8d92bbaab68b557a1fbbdc1ec5dd0>`_  2021-03-08   ``Prepare ad-hoc release of the four previously excluded providers (#14655)``
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  =============================================================================

1.1.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`85a3ce1a47 <https://github.com/apache/airflow/commit/85a3ce1a47e0b84bac518e87481e92d266edea31>`_  2021-01-18   ``Fix S3ToSnowflakeOperator to support uploading all files in the specified stage (#12505)``
`dbf751112f <https://github.com/apache/airflow/commit/dbf751112f3f978b1e21ffb91d696035c5e0109c>`_  2021-01-16   ``Add connection arguments in S3ToSnowflakeOperator (#12564)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  ============================================================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ==================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`2037303eef <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`ef4af21351 <https://github.com/apache/airflow/commit/ef4af2135171c6e451f1407ea1a280ea875f2175>`_  2020-11-22   ``Move providers docs to separate package + Spell-check in a common job with docs-build (#12527)``
`234d689387 <https://github.com/apache/airflow/commit/234d689387ef89222bfdee481987c37d1e79b5af>`_  2020-11-21   ``Fix S3ToSnowflakeOperator docstring (#12504)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f121 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`9276607b58 <https://github.com/apache/airflow/commit/9276607b58bedfb2128c63fabec85d77e7dba07f>`_  2020-11-12   ``Add session_parameters option to snowflake_hook (#12071)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`d363adb618 <https://github.com/apache/airflow/commit/d363adb6187e9cba1d965f424c95058fa933df1f>`_  2020-10-31   ``Adding SnowflakeOperator howto-documentation and example DAG (#11975)``
`ecc3a4df0d <https://github.com/apache/airflow/commit/ecc3a4df0da67f258c3ad04733d6e561d8266c93>`_  2020-10-30   ``Add autocommit property for snowflake connection (#10838)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`4830687453 <https://github.com/apache/airflow/commit/48306874538eea7cfd42358d5ebb59705204bfc4>`_  2020-10-24   ``Use Python 3 style super classes (#11806)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`d305876bee <https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731>`_  2020-10-12   ``Remove redundant None provided as default to dict.get() (#11448)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`0161b5ea2b <https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1>`_  2020-09-26   ``Increasing type coverage for multiple provider (#11159)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d1bce91bb2 <https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513>`_  2020-08-25   ``PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`f6734b3b85 <https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb>`_  2020-08-12   ``Enable Sphinx spellcheck for doc generation (#10280)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`1c9374d257 <https://github.com/apache/airflow/commit/1c9374d2573483dd66f5c35032e24140864e72c0>`_  2020-06-03   ``Add snowflake to slack operator (#9023)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`1d36b0303b <https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea>`_  2020-05-23   ``Fix references in docs (#8984)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`a546a10b13 <https://github.com/apache/airflow/commit/a546a10b13b1f7a119071d8d2001cb17ccdcbbf7>`_  2020-05-16   ``Add Snowflake system test (#8422)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`cd635dd7d5 <https://github.com/apache/airflow/commit/cd635dd7d57cab2f41efac2d3d94e8f80a6c96d6>`_  2020-05-10   ``[AIRFLOW-5906] Add authenticator parameter to snowflake_hook (#8642)``
`297ad30885 <https://github.com/apache/airflow/commit/297ad30885eeb77c062f37df78a78f381e7d140e>`_  2020-04-20   ``Fix Snowflake hook conn id (#8423)``
`cf1109d661 <https://github.com/apache/airflow/commit/cf1109d661991943bb4861a0468ba4bc8946376d>`_  2020-02-07   ``[AIRFLOW-6755] Fix snowflake hook bug and tests (#7380)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`eee34ee808 <https://github.com/apache/airflow/commit/eee34ee8080bb7bc81294c3fbd8be93bbf795367>`_  2020-01-24   ``[AIRFLOW-4204] Update super() calls (#7248)``
`17af3beea5 <https://github.com/apache/airflow/commit/17af3beea5095d9aec81c06404614ca6d1057a45>`_  2020-01-21   ``[AIRFLOW-5816] Add S3 to snowflake operator (#6469)``
=================================================================================================  ===========  ==================================================================================================
