
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


Package apache-airflow-providers-alibaba
------------------------------------------------------

Alibaba Cloud integration (including `Alibaba Cloud <https://www.alibabacloud.com//>`__).


This is detailed commit list of changes for versions provider package: ``alibaba``.
For high-level changelog, see :doc:`package information including changelog <index>`.



2.6.0
.....

Latest change: 2023-10-05

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
`99f320354b <https://github.com/apache/airflow/commit/99f320354b075fb780e54057d223d2d16ddf08b8>`_  2023-10-04   ``Refactor: consolidate import time in providers (#34402)``
`7ebf4220c9 <https://github.com/apache/airflow/commit/7ebf4220c9abd001f1fa23c95f882efddd5afbac>`_  2023-09-28   ``Refactor usage of str() in providers (#34320)``
`caee896403 <https://github.com/apache/airflow/commit/caee89640350ac8aaf46e3a8a948981c58392dcf>`_  2023-09-18   ``Consolidate hook management in AnalyticDBSparkSensor (#34435)``
`0891742ecb <https://github.com/apache/airflow/commit/0891742ecba380f062b1bb528491231f13aea02b>`_  2023-09-18   ``Consolidate hook management in AnalyticDBSparkBaseOperator (#34434)``
`f8ae8dba66 <https://github.com/apache/airflow/commit/f8ae8dba667997128ae66fec28e33fa5cb30997e>`_  2023-09-18   ``Deprecate get_hook in OSSKeySensor and use hook instead (#34426)``
=================================================================================================  ===========  =======================================================================

2.5.3
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`7696e416ae <https://github.com/apache/airflow/commit/7696e416aef95295bdb360a8bf5ce68f9fd41e3e>`_  2023-09-07   ``fix(providers/alibaba): respect soft_fail argument when exception is raised (#34157)``
=================================================================================================  ===========  ========================================================================================

2.5.2
.....

Latest change: 2023-08-26

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`32feab4100 <https://github.com/apache/airflow/commit/32feab41006897de182bfa684813be230027aca1>`_  2023-08-22   ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``
`af0a4a0698 <https://github.com/apache/airflow/commit/af0a4a0698040c1b61cdeafc135d99dcb182c9ef>`_  2023-08-21   ``Simplify conditions on len() in providers/apache (#33564)``
`c645d8e40c <https://github.com/apache/airflow/commit/c645d8e40c167ea1f6c332cdc3ea0ca5a9363205>`_  2023-08-12   ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
=================================================================================================  ===========  =======================================================================

2.5.1
.....

Latest change: 2023-08-11

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`b5a4d36383 <https://github.com/apache/airflow/commit/b5a4d36383c4143f46e168b8b7a4ba2dc7c54076>`_  2023-08-11   ``Prepare docs for Aug 2023 2nd wave of Providers (#33291)``
`4e83660f4c <https://github.com/apache/airflow/commit/4e83660f4c713d5a00ad9a583ba3a56c374d7e92>`_  2023-08-08   ``Refactor: Simplify code in providers/alibaba (#33225)``
=================================================================================================  ===========  ============================================================

2.5.0
.....

Latest change: 2023-07-06

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`0bc689ee6d <https://github.com/apache/airflow/commit/0bc689ee6d4b6967d7ae99a202031aac14d181a2>`_  2023-06-28   ``D205 Support - Providers: Airbyte and Alibaba (#32214)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
`f05beb7219 <https://github.com/apache/airflow/commit/f05beb721940c36f2c501ea8657516262b99c01e>`_  2023-06-27   ``Add Alibaba Cloud AnalyticDB Spark Support (#31787)``
=================================================================================================  ===========  ================================================================

2.4.1
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`8b146152d6 <https://github.com/apache/airflow/commit/8b146152d62118defb3004c997c89c99348ef948>`_  2023-06-20   ``Add note about dropping Python 3.7 for providers (#32015)``
`a59076eaee <https://github.com/apache/airflow/commit/a59076eaeed03dd46e749ad58160193b4ef3660c>`_  2023-06-02   ``Add D400 pydocstyle check - Providers (#31427)``
`9fa75aaf7a <https://github.com/apache/airflow/commit/9fa75aaf7a391ebf0e6b6949445c060f6de2ceb9>`_  2023-05-29   ``Remove Python 3.7 support (#30963)``
=================================================================================================  ===========  =============================================================

2.4.0
.....

Latest change: 2023-05-19

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`45548b9451 <https://github.com/apache/airflow/commit/45548b9451fba4e48c6f0c0ba6050482c2ea2956>`_  2023-05-19   ``Prepare RC2 docs for May 2023 wave of Providers (#31416)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`a7eb32a5b2 <https://github.com/apache/airflow/commit/a7eb32a5b222e236454d3e474eec478ded7c368d>`_  2023-04-30   ``Bump minimum Airflow version in providers (#30917)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  ======================================================================================

2.3.0
.....

Latest change: 2023-04-02

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`55dbf1ff1f <https://github.com/apache/airflow/commit/55dbf1ff1fb0b22714f695a66f6108b3249d1199>`_  2023-04-02   ``Prepare docs for April 2023 wave of Providers (#30378)``
`b6392ae5fd <https://github.com/apache/airflow/commit/b6392ae5fd466fa06ca92c061a0f93272e27a26b>`_  2023-03-07   ``Support deleting the local log files when using remote logging (#29772)``
=================================================================================================  ===========  ===========================================================================

2.2.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`00af5c007e <https://github.com/apache/airflow/commit/00af5c007ef2200401b53c40236e664758e47f27>`_  2022-11-14   ``Replace urlparse with urlsplit (#27389)``
`8c15b0a6d1 <https://github.com/apache/airflow/commit/8c15b0a6d1a846cc477618e326a50cd96f76380f>`_  2022-11-07   ``Use log.exception where more economical than log.error (#27517)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
=================================================================================================  ===========  =========================================================================

2.1.0
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`1f7b296227 <https://github.com/apache/airflow/commit/1f7b296227fee772de9ba15af6ce107937ef9b9b>`_  2022-09-18   ``Auto tail file logs in Web UI (#26169)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
=================================================================================================  ===========  ====================================================================================

2.0.1
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`9c59831ee7 <https://github.com/apache/airflow/commit/9c59831ee78f14de96421c74986933c494407afa>`_  2022-06-21   ``Update providers to use functools compat for ''cached_property'' (#24582)``
=================================================================================================  ===========  =============================================================================

2.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  =======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`c23826915d <https://github.com/apache/airflow/commit/c23826915dcdca4f22b52b74633336cb2f4a1eca>`_  2022-06-07   ``Apply per-run log templates to log handlers (#24153)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`c887275bb8 <https://github.com/apache/airflow/commit/c887275bb8ad29930f5a2cc5e5270533f92b80d1>`_  2022-06-03   ``Migrate Alibaba example DAGs to new design #22437 (#24130)``
`d19cb86660 <https://github.com/apache/airflow/commit/d19cb86660d40e665d8c4fe2b07d76b88532bd8b>`_  2022-05-31   ``SSL Bucket, Light Logic Refactor and Docstring Update for Alibaba Provider (#23891)``
=================================================================================================  ===========  =======================================================================================

1.1.1
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

1.1.0
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`7bd8b2d7f3 <https://github.com/apache/airflow/commit/7bd8b2d7f3bca39a919cf0aeef91da1c476d792d>`_  2022-03-11   ``Add oss_task_handler into alibaba-provider and enable remote logging to OSS (#21785)``
=================================================================================================  ===========  ========================================================================================

1.0.1
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`73c0d241d8 <https://github.com/apache/airflow/commit/73c0d241d804507abc651a365f93d60c543349d5>`_  2022-01-21   ``Remove a few stray ':type's in docs (#21014)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`730db3fb77 <https://github.com/apache/airflow/commit/730db3fb774f60127ab1c865e19031f1f9c193f7>`_  2022-01-18   ``Remove all "fake" stub files (#20936)``
`f8fd0f7b4c <https://github.com/apache/airflow/commit/f8fd0f7b4ca6cb52307be4323028bf4e409566e7>`_  2022-01-13   ``Explain stub files are introduced for Mypy errors in examples (#20827)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`3299064958 <https://github.com/apache/airflow/commit/3299064958e5fbcfc8e91e905ababb18d7339421>`_  2021-12-29   ``Use isort on pyi files (#20556)``
`b5d520cf73 <https://github.com/apache/airflow/commit/b5d520cf73100df714c71ac9898a97bc0df29a31>`_  2021-12-29   ``Reinstate 'region' to 'default_args' for Alibaba example DAGs (#20423)``
`73ab0edce5 <https://github.com/apache/airflow/commit/73ab0edce58d996e2854d478f054b25c4bb627c4>`_  2021-12-18   ``Fix MyPy Errors for Alibaba provider. (#20393)``
`2fb5e1d0ec <https://github.com/apache/airflow/commit/2fb5e1d0ec306839a3ff21d0bddbde1d022ee8c7>`_  2021-12-15   ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`f5ad26dcdd <https://github.com/apache/airflow/commit/f5ad26dcdd7bcb724992528dce71056965b94d26>`_  2021-10-21   ``Fixup string concatenations (#19099)``
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`0fe0b06bb0 <https://github.com/apache/airflow/commit/0fe0b06bb0bf468f46195946f938f9e6e3d46216>`_  2021-09-03   ``Adding missing init file in example_dags directory (#18019)``
=================================================================================================  ===========  ======================================================================================

1.0.0
.....

Latest change: 2021-08-23

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`75ca6543da <https://github.com/apache/airflow/commit/75ca6543da3754f0dfb53d059588ad66a2f8235a>`_  2021-08-07   ``[AIRFLOW-17200] Add Alibaba Cloud OSS support (#17201)``
=================================================================================================  ===========  ============================================================================
