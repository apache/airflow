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

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!

Package apache-airflow-providers-slack
------------------------------------------------------

`Slack <https://slack.com/>`__ services integration including:

  - `Slack API <https://api.slack.com/>`__
  - `Slack Incoming Webhook <https://api.slack.com/messaging/webhooks>`__


This is detailed commit list of changes for versions provider package: ``slack``.
For high-level changelog, see :doc:`package information including changelog <index>`.



8.4.0
.....

Latest change: 2023-11-07

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`3fbd9d6b18 <https://github.com/apache/airflow/commit/3fbd9d6b18021faa08550532241515d75fbf3b83>`_  2023-11-07   ``Add missing examples into Slack Provider (#35495)``
`11bdfe4c12 <https://github.com/apache/airflow/commit/11bdfe4c12efa2f5d256cc49916a20beaa5487eb>`_  2023-11-07   ``Work around typing issue in examples and providers (#35494)``
`850e1947a6 <https://github.com/apache/airflow/commit/850e1947a6691e3c0664e59bbb36debc3ab19f48>`_  2023-11-05   ``Reorganize SQL to Slack Operators (#35215)``
=================================================================================================  ===========  ===============================================================

8.3.0
.....

Latest change: 2023-10-28

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`d1c58d86de <https://github.com/apache/airflow/commit/d1c58d86de1267d9268a1efe0a0c102633c051a1>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
`3592ff4046 <https://github.com/apache/airflow/commit/3592ff40465032fa041600be740ee6bc25e7c242>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
`dd7ba3cae1 <https://github.com/apache/airflow/commit/dd7ba3cae139cb10d71c5ebc25fc496c67ee784e>`_  2023-10-19   ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
`e6f445129a <https://github.com/apache/airflow/commit/e6f445129a998eab62d71bd91b4a5f46cd77c1de>`_  2023-10-19   ``Pass additional arguments from Slack's Operators/Notifiers to Hooks (#35039)``
`b75f9e8806 <https://github.com/apache/airflow/commit/b75f9e880614fa0427e7d24a1817955f5de658b3>`_  2023-10-18   ``Upgrade pre-commits (#35033)``
=================================================================================================  ===========  ================================================================================

8.2.0
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`e9987d5059 <https://github.com/apache/airflow/commit/e9987d50598f70d84cbb2a5d964e21020e81c080>`_  2023-10-13   ``Prepare docs 1st wave of Providers in October 2023 (#34916)``
`6ba2c4485c <https://github.com/apache/airflow/commit/6ba2c4485cb8ff2cf3c2e4d8043e4c7fe5008b15>`_  2023-10-12   ``Docstring correction for 'SlackAPIOperator' (#34871)``
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
`faa32f23e8 <https://github.com/apache/airflow/commit/faa32f23e824ec8dd00b296ce9d8bd239ac0437f>`_  2023-09-23   ``Slack: use default_conn_name by default (#34548)``
=================================================================================================  ===========  ===============================================================

8.1.0
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ===================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`e357f7b531 <https://github.com/apache/airflow/commit/e357f7b531e50981faa1199595c0e92a23a714b5>`_  2023-09-01   ``Add Slack Incoming Webhook Notifier (#33966)``
`b4d4f55b47 <https://github.com/apache/airflow/commit/b4d4f55b479d07c13ab25bb2e80cb053378b56d7>`_  2023-08-31   ``Refactor: Replace lambdas with comprehensions in providers (#33771)``
`9d8c77e447 <https://github.com/apache/airflow/commit/9d8c77e447f5515b9a6aa85fa72511a86a128c28>`_  2023-08-27   ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
=================================================================================================  ===========  ===================================================================================================

8.0.0
.....

Latest change: 2023-08-26

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`ed6a4fd116 <https://github.com/apache/airflow/commit/ed6a4fd1162407a2007d949765eab5749af1c3ac>`_  2023-08-24   ``Remove deprecated parts from Slack provider (#33557)``
`0a7eae3fcb <https://github.com/apache/airflow/commit/0a7eae3fcbda0b2c4d2fb383e6b07157c2adc937>`_  2023-08-23   ``Replace deprecated slack notification in provider.yaml with new one (#33643)``
`ea8519c055 <https://github.com/apache/airflow/commit/ea8519c0554d16b13d330a686f8479fc10cc58f2>`_  2023-08-18   ``Avoid importing pandas and numpy in runtime and module level (#33483)``
`8e88eb8fa7 <https://github.com/apache/airflow/commit/8e88eb8fa7e1fc12918dcbfcfc8ed28381008d33>`_  2023-08-17   ``Consolidate import and usage of pandas (#33480)``
=================================================================================================  ===========  ================================================================================

7.3.2
.....

Latest change: 2023-07-29

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`d06b7af69a <https://github.com/apache/airflow/commit/d06b7af69a65c50321ba2a9904551f3b8affc7f1>`_  2023-07-29   ``Prepare docs for July 2023 3rd wave of Providers (#32875)``
`60c49ab2df <https://github.com/apache/airflow/commit/60c49ab2dfabaf450b80a5c7569743dd383500a6>`_  2023-07-19   ``Add more accurate typing for DbApiHook.run method (#31846)``
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`e45bee8840 <https://github.com/apache/airflow/commit/e45bee884068399e7265421511e17fed106ce5b4>`_  2023-07-05   ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  ====================================================================

7.3.1
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`8b146152d6 <https://github.com/apache/airflow/commit/8b146152d62118defb3004c997c89c99348ef948>`_  2023-06-20   ``Add note about dropping Python 3.7 for providers (#32015)``
`9276310a43 <https://github.com/apache/airflow/commit/9276310a43d17a9e9e38c2cb83686a15656896b2>`_  2023-06-05   ``Improve docstrings in providers (#31681)``
`a59076eaee <https://github.com/apache/airflow/commit/a59076eaeed03dd46e749ad58160193b4ef3660c>`_  2023-06-02   ``Add D400 pydocstyle check - Providers (#31427)``
`9fa75aaf7a <https://github.com/apache/airflow/commit/9fa75aaf7a391ebf0e6b6949445c060f6de2ceb9>`_  2023-05-29   ``Remove Python 3.7 support (#30963)``
=================================================================================================  ===========  =============================================================

7.3.0
.....

Latest change: 2023-05-19

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`45548b9451 <https://github.com/apache/airflow/commit/45548b9451fba4e48c6f0c0ba6050482c2ea2956>`_  2023-05-19   ``Prepare RC2 docs for May 2023 wave of Providers (#31416)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`24532312b6 <https://github.com/apache/airflow/commit/24532312b694242ba74644fdd43a487e93122235>`_  2023-05-12   ``Standardize Slack Notifier (#31244)``
`0a30706aa7 <https://github.com/apache/airflow/commit/0a30706aa7c581905ca99a8b6e2f05960d480729>`_  2023-05-03   ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`a7eb32a5b2 <https://github.com/apache/airflow/commit/a7eb32a5b222e236454d3e474eec478ded7c368d>`_  2023-04-30   ``Bump minimum Airflow version in providers (#30917)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
`ce6ae2457e <https://github.com/apache/airflow/commit/ce6ae2457ef3d9f44f0086b58026909170bbf22a>`_  2023-02-08   ``Prepare docs for Feb 2023 wave of Providers (#29379)``
`b5b1fae2df <https://github.com/apache/airflow/commit/b5b1fae2dfe92751d6aaaace00009ce29625095b>`_  2023-01-31   ``Add Documentation for notification feature extension (#29191)``
=================================================================================================  ===========  ======================================================================================

7.2.0
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`a7e1cb2fbf <https://github.com/apache/airflow/commit/a7e1cb2fbfc684508f4b832527ae2371f99ad37d>`_  2023-01-04   ``Add general-purpose "notifier" concept to DAGs (#28569)``
=================================================================================================  ===========  ==================================================================

7.1.1
.....

Latest change: 2023-01-02

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`527b948856 <https://github.com/apache/airflow/commit/527b948856584320f74d385f58477af79506834d>`_  2022-12-03   ``[misc] Replace XOR '^' conditions by 'exactly_one' helper in providers (#27858)``
=================================================================================================  ===========  ===================================================================================

7.1.0
.....

Latest change: 2022-11-26

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`25bdbc8e67 <https://github.com/apache/airflow/commit/25bdbc8e6768712bad6043618242eec9c6632618>`_  2022-11-26   ``Updated docs for RC3 wave of providers (#27937)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
`80c327bd3b <https://github.com/apache/airflow/commit/80c327bd3b45807ff2e38d532325bccd6fe0ede0>`_  2022-11-24   ``Bump common.sql provider to 1.3.1 (#27888)``
`c609477260 <https://github.com/apache/airflow/commit/c60947726082905f7b369a90e8d37fcdb873149f>`_  2022-11-16   ``Implements SqlToSlackApiFileOperator (#26374)``
=================================================================================================  ===========  ================================================================

7.0.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`00af5c007e <https://github.com/apache/airflow/commit/00af5c007ef2200401b53c40236e664758e47f27>`_  2022-11-14   ``Replace urlparse with urlsplit (#27389)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`cc44bae412 <https://github.com/apache/airflow/commit/cc44bae412412bec088ccee569dcc5f4aae810d1>`_  2022-10-22   ``Allow and prefer non-prefixed extra fields for slack hooks (#27070)``
=================================================================================================  ===========  =========================================================================

6.0.0
.....

Latest change: 2022-10-04

=================================================================================================  ===========  ==============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================================
`403ed7163f <https://github.com/apache/airflow/commit/403ed7163f3431deb7fc21108e1743385e139907>`_  2022-10-04   ``Add docs for Google/Slack RC providers (#26860)``
`7b183071a3 <https://github.com/apache/airflow/commit/7b183071a398cbe340853f357bc6c029d551b4d1>`_  2022-10-03   ``Fix Slack Connections created in the UI (#26845)``
`ec1615b589 <https://github.com/apache/airflow/commit/ec1615b589d60416cac449bea5fa777a5eda4757>`_  2022-09-28   ``Fix errors in CHANGELOGS for slack and amazon (#26746)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`fd27584b3d <https://github.com/apache/airflow/commit/fd27584b3dc355eaf0c0cd7a4cd65e0e580fcf6d>`_  2022-09-27   ``Refactor 'SlackWebhookOperator': Get rid of mandatory http-provider dependency (#26648)``
`95a5fc7ec9 <https://github.com/apache/airflow/commit/95a5fc7ec9a637337af9446f11d7f90a6e47e006>`_  2022-09-22   ``Refactor SlackWebhookHook in order to use 'slack_sdk' instead of HttpHook methods (#26452)``
`7d5e8cce6c <https://github.com/apache/airflow/commit/7d5e8cce6c3e27bc7b9bf28823e9adc6cdb458e2>`_  2022-09-19   ``Remove unsafe imports in Slack API Connection (#26459)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`675bb6c0e8 <https://github.com/apache/airflow/commit/675bb6c0e88c380e39d242e04543e11950ea1141>`_  2022-09-09   ``Move send_file method into SlackHook (#26118)``
`214873cc60 <https://github.com/apache/airflow/commit/214873cc60200ff6f070bacd31b1b21012703c3e>`_  2022-08-31   ``Refactor Slack API Hook and add Connection (#25852)``
`8acdc2a834 <https://github.com/apache/airflow/commit/8acdc2a834b9c4e287fe612ed56ab8908d777609>`_  2022-08-30   ``Replace SQL with Common SQL in pre commit (#26058)``
`ca9229b6fe <https://github.com/apache/airflow/commit/ca9229b6fe7eda198c7ce32da13afb97ab9f3e28>`_  2022-08-18   ``Add common-sql lower bound for common-sql (#25789)``
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`808035e00a <https://github.com/apache/airflow/commit/808035e00aaf59a8012c50903a09d3f50bd92ca4>`_  2022-07-18   ``AIP-47 - Migrate Slack DAG to new design (#25137)``
=================================================================================================  ===========  ==============================================================================================

5.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`69565ddfa0 <https://github.com/apache/airflow/commit/69565ddfa0371d83596b295e9c711074dd273f5f>`_  2022-07-01   ``Update docstring in 'SqlToSlackOperator' (#24759)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`13908c2c91 <https://github.com/apache/airflow/commit/13908c2c914cf08f9d962a4d3b6ae54fbdf1d223>`_  2022-06-29   ``Adding generic 'SqlToSlackOperator' (#24663)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
=================================================================================================  ===========  ==================================================================

5.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
=================================================================================================  ===========  ==================================================================================

4.2.3
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

4.2.2
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

4.2.1
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
=================================================================================================  ===========  ========================================================

4.2.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`0ac3b8c3dd <https://github.com/apache/airflow/commit/0ac3b8c3dd749c59e60cf0169580b9e7c5049d9e>`_  2022-01-27   ``Return slack api call response in slack_hook (#21107)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`a47c58a780 <https://github.com/apache/airflow/commit/a47c58a78003a1aba22fe3ac6fe7a120807bfaa9>`_  2021-12-30   ``Update SlackWebhookHook docstring (#20061)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`dad2f8103b <https://github.com/apache/airflow/commit/dad2f8103be954afaedf15e9d098ee417b0d5d02>`_  2021-12-15   ``Fix mypy providers (#20190)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`d937bebdad <https://github.com/apache/airflow/commit/d937bebdadeab6d159be10ea328e1947001ba73c>`_  2021-10-16   ``Doc: Restoring additional context in Slack operators how-to guide (#18985)``
=================================================================================================  ===========  ==============================================================================

4.1.0
.....

Latest change: 2021-09-30

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`29493d2d61 <https://github.com/apache/airflow/commit/29493d2d617198e34cded1a04ab6c039012eb068>`_  2021-09-26   ``Add Slack operators how-to guide (#18525)``
`9bf0ed2179 <https://github.com/apache/airflow/commit/9bf0ed2179b62f374cad74334a8976534cf1a53b>`_  2021-09-23   ``Restore 'filename' to template_fields (#18466)``
=================================================================================================  ===========  ======================================================================================

4.0.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`2935be1990 <https://github.com/apache/airflow/commit/2935be19901467c645bce9d134e28335f2aee7d8>`_  2021-08-16   ``Fixed SlackAPIFileOperator to upload file and file content. (#17400)``
`07c8ee0151 <https://github.com/apache/airflow/commit/07c8ee01512b0cc1c4602e356b7179cfb50a27f4>`_  2021-08-02   ``Fixed SlackAPIFileOperator to upload file and file content (#17247)``
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
=================================================================================================  ===========  ============================================================================

4.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`1e647029e4 <https://github.com/apache/airflow/commit/1e647029e469c1bb17e9ad051d0184f3357644c3>`_  2021-06-01   ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
`10ed42a837 <https://github.com/apache/airflow/commit/10ed42a837e11d8e954c1f885e289a4248edd2ca>`_  2021-05-27   ``Fix hooks extended from http hook (#16109)``
`6d9fc3ed98 <https://github.com/apache/airflow/commit/6d9fc3ed98f694a4f30bed29cb0599c5aa5b8ef1>`_  2021-05-14   ``Fix docstring formatting on ''SlackHook'' (#15840)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`40a2476a5d <https://github.com/apache/airflow/commit/40a2476a5db14ee26b5108d72635da116eab720b>`_  2021-04-28   ``Adds interactivity when generating provider documentation. (#15518)``
`bf2b48174a <https://github.com/apache/airflow/commit/bf2b48174a1ccfe398eefba7f04a5cacac421266>`_  2021-04-27   ``Add Connection Documentation for Providers (#15499)``
`a7ca1b3b0b <https://github.com/apache/airflow/commit/a7ca1b3b0bdf0b7677e53be1b11e833714dfbbb4>`_  2021-03-26   ``Fix Sphinx Issues with Docstrings (#14968)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
=================================================================================================  ===========  =======================================================================

3.0.0
.....

Latest change: 2021-02-27

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`8c060d55df <https://github.com/apache/airflow/commit/8c060d55dfb3ded21cb9d2305cffe14e1c610680>`_  2021-02-18   ``Don't allow SlackHook.call method accept *args (#14289)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  =======================================================================

2.0.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`2839450013 <https://github.com/apache/airflow/commit/283945001363d8f492fbd25f2765d39fa06d757a>`_  2021-01-25   ``Upgrade slack_sdk to v3 (#13745)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  ========================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`2947e09999 <https://github.com/apache/airflow/commit/2947e0999979fad1f2c98aeb4f1e46297e4c9864>`_  2020-12-02   ``SlackWebhookHook use password instead of extra (#12674)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`dd2095f4a8 <https://github.com/apache/airflow/commit/dd2095f4a8b07c9b1a4c279a3578cd1e23b71a1b>`_  2020-11-10   ``Simplify string expressions & Use f-string (#12216)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`4830687453 <https://github.com/apache/airflow/commit/48306874538eea7cfd42358d5ebb59705204bfc4>`_  2020-10-24   ``Use Python 3 style super classes (#11806)``
`4fb5c017fe <https://github.com/apache/airflow/commit/4fb5c017fe5ca41ed95547a857c9c39efc4f1476>`_  2020-10-21   ``Check response status in slack webhook hook. (#11620)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`720912f67b <https://github.com/apache/airflow/commit/720912f67b3af0bdcbac64d6b8bf6d51c6247e26>`_  2020-10-02   ``Strict type check for multiple providers (#11229)``
`0161b5ea2b <https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1>`_  2020-09-26   ``Increasing type coverage for multiple provider (#11159)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d1bce91bb2 <https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513>`_  2020-08-25   ``PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`2f2d8dbfaf <https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148>`_  2020-08-25   ``Remove all "noinspection" comments native to IntelliJ (#10525)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`7cc1c8bc00 <https://github.com/apache/airflow/commit/7cc1c8bc0031f1d9839baaa5a6c7a9bc7ec37ead>`_  2020-07-25   ``Updates the slack WebClient call to use the instance variable - token (#9995)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`df8efd04f3 <https://github.com/apache/airflow/commit/df8efd04f394afc4b5affb677bc78d8b7bd5275a>`_  2020-06-21   ``Enable & Fix "Docstring Content Issues" PyDocStyle Check (#9460)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`5cf46fad1e <https://github.com/apache/airflow/commit/5cf46fad1e0a9cdde213258b2064e16d30d3160e>`_  2020-05-29   ``Add SlackAPIFileOperator impementing files.upload from Slack API (#9004)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`427257c2e2 <https://github.com/apache/airflow/commit/427257c2e2ffc886ef9f516e9c4d015a4ede9bbd>`_  2020-05-24   ``Remove defunct code from setup.py (#8982)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`578fc514cd <https://github.com/apache/airflow/commit/578fc514cd325b7d190bdcfb749a384d101238fa>`_  2020-05-12   ``[AIRFLOW-4543] Update slack operator to support slackclient v2 (#5519)``
`4bde99f132 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`be2b2baa7c <https://github.com/apache/airflow/commit/be2b2baa7c5f53c2d73646e4623cdb6731551b70>`_  2020-03-23   ``Add missing call to Super class in 'http', 'grpc' & 'slack' providers (#7826)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`9a04013b0e <https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2>`_  2020-01-27   ``[AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)``
`c42a375e79 <https://github.com/apache/airflow/commit/c42a375e799e5adb3f9536616372dc90ff47e6c8>`_  2020-01-27   ``[AIRFLOW-6644][AIP-21] Move service classes to providers package (#7265)``
=================================================================================================  ===========  ==================================================================================
