
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


Package apache-airflow-providers-apache-hive
------------------------------------------------------

`Apache Hive <https://hive.apache.org/>`__


This is detailed commit list of changes for versions provider package: ``apache.hive``.
For high-level changelog, see :doc:`package information including changelog <index>`.



5.1.2
.....

Latest change: 2023-01-18

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`23da4daaa0 <https://github.com/apache/airflow/commit/23da4daaa018e72b39b977afcde85deaf2224f1e>`_  2023-01-18   ``Revert "Remove conn.close() ignores (#29005)" (#29010)``
`85f8df7b8a <https://github.com/apache/airflow/commit/85f8df7b8a18e1147c7e014a7af7fc4e66aaa8be>`_  2023-01-18   ``Remove conn.close() ignores (#29005)``
`aa97474020 <https://github.com/apache/airflow/commit/aa97474020712d3f450ab169a5a054580e7b7d28>`_  2023-01-18   ``Fixed MyPy errors introduced by new mysql-connector-python (#28995)``
=================================================================================================  ===========  =======================================================================

5.1.1
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`45dd0c484e <https://github.com/apache/airflow/commit/45dd0c484e16ff56800cc9c047f56b4a909d2d0d>`_  2023-01-11   ``Move local_infile option from extra to hook parameter (#28811)``
=================================================================================================  ===========  ==================================================================

5.1.0
.....

Latest change: 2023-01-02

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`4e545c6e54 <https://github.com/apache/airflow/commit/4e545c6e54712eedb6ca9cbb8333393ae3f6cba2>`_  2022-12-27   ``Move Hive macros to the provider (#28538)``
`d9ae90fc64 <https://github.com/apache/airflow/commit/d9ae90fc6478133767e29774920ed797175146bc>`_  2022-12-21   ``Make pandas dependency optional for Amazon Provider (#28505)``
=================================================================================================  ===========  ================================================================

5.0.0
.....

Latest change: 2022-12-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`443df3b9c8 <https://github.com/apache/airflow/commit/443df3b9c8ef698e0204490c535f78c6c70276f3>`_  2022-12-13   ``Prepare ad hoc providers release (#28327)``
`5f8481c799 <https://github.com/apache/airflow/commit/5f8481c799ea6bd742a5ccc194b2ff8dbe01eab5>`_  2022-12-06   ``Move hive_cli_params to hook parameters (#28101)``
`2d45f9d6c3 <https://github.com/apache/airflow/commit/2d45f9d6c30aabebce3449eae9f152ba6d2306e2>`_  2022-11-27   ``Improve filtering for invalid schemas in Hive hook (#27808)``
=================================================================================================  ===========  ===============================================================

4.1.1
.....

Latest change: 2022-11-26

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`25bdbc8e67 <https://github.com/apache/airflow/commit/25bdbc8e6768712bad6043618242eec9c6632618>`_  2022-11-26   ``Updated docs for RC3 wave of providers (#27937)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
`80c327bd3b <https://github.com/apache/airflow/commit/80c327bd3b45807ff2e38d532325bccd6fe0ede0>`_  2022-11-24   ``Bump common.sql provider to 1.3.1 (#27888)``
=================================================================================================  ===========  ================================================================

4.1.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`150dd927c3 <https://github.com/apache/airflow/commit/150dd927c3297daccab507bc5a5f2c3a32f24d5f>`_  2022-11-14   ``Filter out invalid schemas in Hive hook (#27647)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
=================================================================================================  ===========  =========================================================================

4.0.1
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`ca9229b6fe <https://github.com/apache/airflow/commit/ca9229b6fe7eda198c7ce32da13afb97ab9f3e28>`_  2022-08-18   ``Add common-sql lower bound for common-sql (#25789)``
=================================================================================================  ===========  ====================================================================================

4.0.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`7e3d2350db <https://github.com/apache/airflow/commit/7e3d2350dbb23b9c98bbadf73296425648e1e42d>`_  2022-08-04   ``Remove Smart Sensors (#25507)``
`5d4abbd58c <https://github.com/apache/airflow/commit/5d4abbd58c33e7dfa8505e307d43420459d3df55>`_  2022-07-27   ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
=================================================================================================  ===========  ===========================================================================

3.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`cef97fccd5 <https://github.com/apache/airflow/commit/cef97fccd511c8e5485df24f27b82fa3e46929d7>`_  2022-06-29   ``fix connection extra parameter 'auth_mechanism' in 'HiveMetastoreHook' and 'HiveServer2Hook' (#24713)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
=================================================================================================  ===========  =========================================================================================================

3.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`b4a5783a2a <https://github.com/apache/airflow/commit/b4a5783a2a90d9a0dc8abe5f2a47e639bfb61646>`_  2022-06-06   ``chore: Refactoring and Cleaning Apache Providers (#24219)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`100ea9d1fc <https://github.com/apache/airflow/commit/100ea9d1fc6831b1ea6e7d33f38c0da5ec9c5fc4>`_  2022-06-05   ``AIP-47 - Migrate hive DAGs to new design #22439 (#24204)``
`71e4deb1b0 <https://github.com/apache/airflow/commit/71e4deb1b093b7ad9320eb5eb34eca8ea440a238>`_  2022-05-16   ``Add typing for airflow/configuration.py (#23716)``
=================================================================================================  ===========  ==================================================================================

2.3.3
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ======================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`2d109401b3 <https://github.com/apache/airflow/commit/2d109401b3566aef613501691d18cf7e4c776cd2>`_  2022-05-04   ``Bump pre-commit hook versions (#22887)``
`0c9c1cf94a <https://github.com/apache/airflow/commit/0c9c1cf94acc6fb315a9bc6f5bf1fbd4e4b4c923>`_  2022-04-28   ``Fix HiveToMySqlOperator's wrong docstring (#23316)``
=================================================================================================  ===========  ======================================================

2.3.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

2.3.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

2.3.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`563ecfa053 <https://github.com/apache/airflow/commit/563ecfa0539f5cbd42a715de0e25e563bd62c179>`_  2022-03-01   ``Add Python 3.9 support to Hive (#21893)``
`f6e0ed0dcc <https://github.com/apache/airflow/commit/f6e0ed0dcc492636f6d1a3a413d5df2f9758f80d>`_  2022-02-15   ``Add how-to guide for hive operator (#21590)``
`041babb060 <https://github.com/apache/airflow/commit/041babb0608a102fd9d83e77b7fab5d9831ec2b4>`_  2022-02-15   ``Fix mypy issues in 'example_twitter_dag' (#21571)``
`2d6282d6b7 <https://github.com/apache/airflow/commit/2d6282d6b7d8c7603e96f0f28ebe0180855687f3>`_  2022-02-15   ``Remove unnecessary/stale comments (#21572)``
`06010fa12a <https://github.com/apache/airflow/commit/06010fa12abe4c6b195e72d2f99f30d8ed1ba9c6>`_  2022-02-11   ``Fix key typo in 'template_fields_renderers' for 'HiveOperator' (#21525)``
`d927507899 <https://github.com/apache/airflow/commit/d927507899c423b81bba212544e1fe07b4ca69b7>`_  2022-02-11   ``Set larger limit get_partitions_by_filter in HiveMetastoreHook (#21504)``
=================================================================================================  ===========  ===========================================================================

2.2.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`8f81b9a01c <https://github.com/apache/airflow/commit/8f81b9a01c7708a282271f9afd6b16a91011f105>`_  2022-02-08   ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`39e395f981 <https://github.com/apache/airflow/commit/39e395f9816c04ef2f033eb0b4f635fc3018d803>`_  2022-02-04   ``Add more SQL template fields renderers (#21237)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`5569b868a9 <https://github.com/apache/airflow/commit/5569b868a990c97dfc63a0e014a814ec1cc0f953>`_  2022-01-09   ``Fix MyPy Errors for providers: Tableau, CNCF, Apache (#20654)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`485ff6cc64 <https://github.com/apache/airflow/commit/485ff6cc64d8f6a15d8d6a0be50661fe6d04b2d9>`_  2021-12-29   ``Fix MyPy errors in Apache Providers (#20422)``
`f760823b4a <https://github.com/apache/airflow/commit/f760823b4af3f0fdfacf63dae199ec4d88028f71>`_  2021-12-11   ``Add some type hints for Hive providers (#20210)``
=================================================================================================  ===========  =================================================================================

2.1.0
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`16b3ab5860 <https://github.com/apache/airflow/commit/16b3ab5860bc766fa31bbeccfb08ea710ca4bae7>`_  2021-11-29   ``Improve various docstrings in Apache Hive providers (#19866)``
`ac752e777b <https://github.com/apache/airflow/commit/ac752e777bad330d05c6aebbea940616831aa6f2>`_  2021-11-24   ``hive provider: restore HA support for metastore (#19777)``
`f50f677514 <https://github.com/apache/airflow/commit/f50f677514b562ac83a00cde2bfd0efdfbe171e4>`_  2021-11-08   ``Fix typos in Hive transfer operator docstrings (#19474)``
`ae044884d1 <https://github.com/apache/airflow/commit/ae044884d1dacce8dbf47c618f543b58f9ff101f>`_  2021-11-03   ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``
=================================================================================================  ===========  ==============================================================================

2.0.3
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ==========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`86a2a19ad2 <https://github.com/apache/airflow/commit/86a2a19ad2bdc87a9ad14bb7fde9313b2d7489bb>`_  2021-10-17   ``More f-strings (#18855)``
`80b5e65a6a <https://github.com/apache/airflow/commit/80b5e65a6abf0a136c5690548c5039f90dda01ab>`_  2021-10-17   ``Remove unnecessary string concatenations in AirflowException in s3_to_hive.py (#19026)``
`232f7d1587 <https://github.com/apache/airflow/commit/232f7d158741405f959e8b09b1687238920306a0>`_  2021-10-10   ``fix get_connections deprecation warn in hivemetastore hook (#18854)``
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`a458fcc573 <https://github.com/apache/airflow/commit/a458fcc573845ff65244a2dafd204ed70129f3e8>`_  2021-09-27   ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
=================================================================================================  ===========  ==========================================================================================

2.0.2
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`da99c3fa6c <https://github.com/apache/airflow/commit/da99c3fa6c366d762bba9fbf3118cc3b3d55f6b4>`_  2021-08-30   ``HiveHook fix get_pandas_df() failure when it tries to read an empty table (#17777)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
=================================================================================================  ===========  ======================================================================================

2.0.1
.....

Latest change: 2021-07-26

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`91f4d80ff0 <https://github.com/apache/airflow/commit/91f4d80ff09093de49478214c5bd027e02c92a0e>`_  2021-07-23   ``Updating Apache example DAGs to use XComArgs (#16869)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`ce44b62890 <https://github.com/apache/airflow/commit/ce44b628904e4f7480a2c208b5d5e087526408b6>`_  2021-06-25   ``Add Python 3.9 support (#15515)``
=================================================================================================  ===========  ===================================================================

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
`476d0f6e3d <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
`736a62f824 <https://github.com/apache/airflow/commit/736a62f824d9062b52983633528e58c445d8cc56>`_  2021-05-08   ``Remove duplicate key from Python dictionary (#15735)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`9953a047c4 <https://github.com/apache/airflow/commit/9953a047c4b0471ceb6effc669dce8d03c2f935b>`_  2021-05-07   ``Add Connection Documentation for the Hive Provider (#15704)``
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`4b031d39e1 <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`e229f3541d <https://github.com/apache/airflow/commit/e229f3541dd764db54785625875a7c5e94225736>`_  2021-04-27   ``Use Pip 21.* to install airflow officially (#15513)``
=================================================================================================  ===========  =================================================================

1.0.3
.....

Latest change: 2021-04-06

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`042be2e4e0 <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`53dafa593f <https://github.com/apache/airflow/commit/53dafa593fd7ce0be2a48dc9a9e993bb42b6abc5>`_  2021-04-04   ``Fix mistake and typos in doc/docstrings (#15180)``
`85e0e76074 <https://github.com/apache/airflow/commit/85e0e76074ced7af258fa476a321865208c33375>`_  2021-03-29   ``Pin flynt to fix failing PRs (#15076)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`6dc24c95e3 <https://github.com/apache/airflow/commit/6dc24c95e3bb46ac42fc80b1948aa79ae6c6fbd1>`_  2021-03-07   ``Fix grammar and remove duplicate words (#14647)``
`b0d6069d25 <https://github.com/apache/airflow/commit/b0d6069d25cf482309af40eec068bcccb2b62552>`_  2021-03-05   ``Fix broken static check on Master  (#14633)``
`d9e4454c66 <https://github.com/apache/airflow/commit/d9e4454c66051a9e8bb5b2f3814d46f29332b89d>`_  2021-03-01   ``Resolve issue related to HiveCliHook kill (#14542)``
=================================================================================================  ===========  =============================================================================

1.0.2
.....

Latest change: 2021-02-27

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  =======================================================================

1.0.1
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`5f81fc73c8 <https://github.com/apache/airflow/commit/5f81fc73c8ea3fc1c3b08080f439fa123926f250>`_  2021-01-03   ``Fix: Remove password if in LDAP or CUSTOM mode HiveServer2Hook (#11767)``
`4f494d4d92 <https://github.com/apache/airflow/commit/4f494d4d92d15f088c54019fbb8a594024bfcf2c>`_  2021-01-03   ``Fix few typos (#13450)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c89 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
=================================================================================================  ===========  ===========================================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`a075b6df99 <https://github.com/apache/airflow/commit/a075b6df99a4f5e21d198f7be56b577432e6f9db>`_  2020-12-09   ``Rename remaining Sensors to match AIP-21 (#12927)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`2037303eef <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`250436d962 <https://github.com/apache/airflow/commit/250436d962c8c950d38c1eb5e54a998891648cc9>`_  2020-11-10   ``Fix spelling in Python files (#12230)``
`502ba309ea <https://github.com/apache/airflow/commit/502ba309ea470943f0e99c634269e3d2d13ce6ca>`_  2020-11-10   ``Enable Markdownlint rule - MD022/blanks-around-headings (#12225)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`e3f96ce7a8 <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`ac943c9e18 <https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c>`_  2020-09-08   ``[AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d760265452 <https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2>`_  2020-08-25   ``PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`27339a5a0f <https://github.com/apache/airflow/commit/27339a5a0f9e382dbc7d32a128f0831a48ef9a12>`_  2020-08-22   ``Remove mentions of Airflow Gitter (#10460)``
`7c206a82a6 <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`8f8db8959e <https://github.com/apache/airflow/commit/8f8db8959e526be54d700845d36ee9f315bae2ea>`_  2020-08-12   ``DbApiHook: Support kwargs in get_pandas_df (#9730)``
`b43f90abf4 <https://github.com/apache/airflow/commit/b43f90abf4c7219d5d59cccb0514256bd3f2fdc7>`_  2020-08-09   ``Fix various typos in the repo (#10263)``
`3b3287d7ac <https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726>`_  2020-08-05   ``Enforce keyword only arguments on apache operators (#10170)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`c2db0dfeb1 <https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed>`_  2020-07-22   ``More strict rules in mypy (#9705) (#9906)``
`5013fda8f0 <https://github.com/apache/airflow/commit/5013fda8f072e633c114fb39fb59a22f60200b40>`_  2020-07-20   ``Add drop_partition functionality for HiveMetastoreHook (#9472)``
`4d74ac2111 <https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2>`_  2020-07-19   ``Increase typing for Apache and http provider package (#9729)``
`44d4ae809c <https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3>`_  2020-07-06   ``Upgrade to latest pre-commit checks (#9686)``
`e13a14c873 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`c78e2a5fea <https://github.com/apache/airflow/commit/c78e2a5feae15e84b05430cfc5935f0e289fb6b4>`_  2020-06-16   ``Make hive macros py3 compatible (#8598)``
`6350fd6ebb <https://github.com/apache/airflow/commit/6350fd6ebb9958982cb3fa1d466168fc31708035>`_  2020-06-08   ``Don't use the term "whitelist" - language matters (#9174)``
`10796cb7ce <https://github.com/apache/airflow/commit/10796cb7ce52c8ac2f68024e531fdda779547bdf>`_  2020-06-03   ``Remove Hive/Hadoop/Java dependency from unit tests (#9029)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`cdb3f25456 <https://github.com/apache/airflow/commit/cdb3f25456e49d0199cd7ccd680626dac01c9be6>`_  2020-05-26   ``All classes in backport providers are now importable in Airflow 1.10 (#8991)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`93ea058802 <https://github.com/apache/airflow/commit/93ea05880283a56e3d42ab07db7453977a3de8ec>`_  2020-04-21   ``[AIRFLOW-7059] pass hive_conf to get_pandas_df in HiveServer2Hook (#8380)``
`87969a350d <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`cb0bf4a142 <https://github.com/apache/airflow/commit/cb0bf4a142656ee40b43a01660b6f6b08a9840fa>`_  2020-03-30   ``Remove sql like function in base_hook (#7901)``
`4bde99f132 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`7e6372a681 <https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388>`_  2020-03-23   ``Add call to Super call in apache providers (#7820)``
`3320e432a1 <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`4d03e33c11 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`f3ad5cf618 <https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a>`_  2020-02-03   ``[AIRFLOW-4681] Make sensors module pylint compatible (#7309)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`83c037873f <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`057f3ae3a4 <https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135>`_  2020-01-29   ``[AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)``
`059eda05f8 <https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf>`_  2020-01-21   ``[AIRFLOW-6610] Move software classes to providers package (#7231)``
`0481b9a957 <https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b>`_  2020-01-12   ``[AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)``
=================================================================================================  ===========  ======================================================================================================================================================================
