
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


Package apache-airflow-providers-elasticsearch
------------------------------------------------------

`Elasticsearch <https://www.elastic.co/elasticsearch>`__


This is detailed commit list of changes for versions provider package: ``elasticsearch``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.3.2
.....

Latest change: 2022-12-07

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`99bbcd3780 <https://github.com/apache/airflow/commit/99bbcd3780dd08a0794ba99eb69006c106dcf5d2>`_  2022-12-07   ``Support restricted index patterns in Elasticsearch log handler (#23888)``
=================================================================================================  ===========  ===========================================================================

4.3.1
.....

Latest change: 2022-11-26

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`25bdbc8e67 <https://github.com/apache/airflow/commit/25bdbc8e6768712bad6043618242eec9c6632618>`_  2022-11-26   ``Updated docs for RC3 wave of providers (#27937)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
`80c327bd3b <https://github.com/apache/airflow/commit/80c327bd3b45807ff2e38d532325bccd6fe0ede0>`_  2022-11-24   ``Bump common.sql provider to 1.3.1 (#27888)``
=================================================================================================  ===========  ================================================================

4.3.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
=================================================================================================  ===========  =========================================================================

4.2.1
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`ca9229b6fe <https://github.com/apache/airflow/commit/ca9229b6fe7eda198c7ce32da13afb97ab9f3e28>`_  2022-08-18   ``Add common-sql lower bound for common-sql (#25789)``
=================================================================================================  ===========  ====================================================================================

4.2.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`a25f8c36ec <https://github.com/apache/airflow/commit/a25f8c36ec5971199f7e540e87e6196ad547d53b>`_  2022-07-22   ``Improve ElasticsearchTaskHandler (#21942)``
=================================================================================================  ===========  =================================================================

4.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`ef79a0d1c4 <https://github.com/apache/airflow/commit/ef79a0d1c4c0a041d7ebf83b93cbb25aa3778a70>`_  2022-07-11   ``Only assert stuff for mypy when type checking (#24937)``
`2ddc100405 <https://github.com/apache/airflow/commit/2ddc1004050464c112c18fee81b03f87a7a11610>`_  2022-07-08   ``Adding ElasticserachPythonHook - ES Hook With The Python Client (#24895)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`97948ecae7 <https://github.com/apache/airflow/commit/97948ecae7fcbb7dfdfb169cfe653bd20a108def>`_  2022-07-01   ``Move fallible ti.task.dag assignment back inside try/except block (#24533) (#24592)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
=================================================================================================  ===========  =======================================================================================

4.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`c23826915d <https://github.com/apache/airflow/commit/c23826915dcdca4f22b52b74633336cb2f4a1eca>`_  2022-06-07   ``Apply per-run log templates to log handlers (#24153)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`cba3c17254 <https://github.com/apache/airflow/commit/cba3c17254a1a864ea30d009e7939203f32bf9dd>`_  2022-06-04   ``removed old files (#24172)``
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
=================================================================================================  ===========  ==================================================================================

3.0.3
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`9c28e766b3 <https://github.com/apache/airflow/commit/9c28e766b3a7bf93b4c8ec5422a5a25f10117fcc>`_  2022-04-07   ``Make ElasticSearch Provider compatible for Airflow<2.3 (#22814)``
`c063fc688c <https://github.com/apache/airflow/commit/c063fc688cf20c37ed830de5e3dac4a664fd8241>`_  2022-03-25   ``Update black precommit (#22521)``
=================================================================================================  ===========  ===================================================================

3.0.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`0f977daa3c <https://github.com/apache/airflow/commit/0f977daa3cb0b7e08a33eb86c60220ee53089ece>`_  2022-03-22   ``Fix "run_id" k8s and elasticsearch compatibility with Airflow 2.1 (#22385)``
=================================================================================================  ===========  ==============================================================================

3.0.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

3.0.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`7be87d87e2 <https://github.com/apache/airflow/commit/7be87d87e27c1e64eee9ce866dbd622a551081cf>`_  2022-02-15   ``Type TaskInstance.task to Operator and call unmap() when needed (#21563)``
`2258e13cc7 <https://github.com/apache/airflow/commit/2258e13cc78faec80054c223eca9378fd33e18fd>`_  2022-02-15   ``Change default log filename template to include map_index (#21495)``
=================================================================================================  ===========  ============================================================================

2.2.0
.....

Latest change: 2022-02-14

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`28378d867a <https://github.com/apache/airflow/commit/28378d867afaac497529bd2e1d2c878edf66f460>`_  2022-02-14   ``Add documentation for RC3 release of providers for Jan 2022 (#21553)``
`44bd211b19 <https://github.com/apache/airflow/commit/44bd211b19dcb75eeb53ced5bea2cf0c80654b1a>`_  2022-02-12   ``Use compat data interval shim in log handlers (#21289)``
`0a3ff43d41 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6184facbaa <https://github.com/apache/airflow/commit/6184facbaa6c654d15d493b9de918e7ee74b3cce>`_  2022-02-07   ``Emit "logs not found" message when ES logs appear to be missing (#21261)``
`d8c4449a91 <https://github.com/apache/airflow/commit/d8c4449a91b9b93691c03e1af45bdedc5e23fd5e>`_  2022-02-06   ``Clarify ElasticsearchTaskHandler docstring (#21255)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`6e51608f28 <https://github.com/apache/airflow/commit/6e51608f28f4c769c019624ea0caaa0c6e671f80>`_  2021-12-16   ``Fix mypy for providers: elasticsearch, oracle, yandex (#20344)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`fe682ec3d3 <https://github.com/apache/airflow/commit/fe682ec3d376f0983410d64beb4f3529fb7b0f99>`_  2021-11-24   ``Fix duplicate changelog entries (#19759)``
=================================================================================================  ===========  ============================================================================

2.1.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`de9b02f797 <https://github.com/apache/airflow/commit/de9b02f797931efbd081996b4f81ba14ca76a17d>`_  2021-09-28   ``Updating the Elasticsearch example DAG to use the TaskFlow API (#18565)``
`060345c0d9 <https://github.com/apache/airflow/commit/060345c0d982765e39da5fa8b2e2c6a01e89e394>`_  2021-09-21   ``Add docs for AIP 39: Timetables (#17552)``
`a0a05ffedd <https://github.com/apache/airflow/commit/a0a05ffeddab54199e43b76016703c7ccaed3cd1>`_  2021-09-04   ``Adds example showing the ES_hook (#17944)``
=================================================================================================  ===========  ===========================================================================

2.0.3
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`29aab6434f <https://github.com/apache/airflow/commit/29aab6434ffe0fb8c83b6fd6c9e44310966d496a>`_  2021-08-17   ``Adds secrets backend/logging/auth information to provider yaml (#17625)``
`944dc32c2b <https://github.com/apache/airflow/commit/944dc32c2b4a758564259133a08f2ea8d28dcb6c>`_  2021-08-12   ``Fix Invalid log order in ElasticsearchTaskHandler (#17551)``
=================================================================================================  ===========  ============================================================================

2.0.2
.....

Latest change: 2021-06-28

=================================================================================================  ===========  ===========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`6f445e69c3 <https://github.com/apache/airflow/commit/6f445e69c3c9d95bff18f327aeeab126cc36a6e1>`_  2021-06-26   ``Update release documentation for elasticsearch (#16662)``
=================================================================================================  ===========  ===========================================================

2.0.1
.....

Latest change: 2021-06-18

=================================================================================================  ===========  =====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`3cf67be387 <https://github.com/apache/airflow/commit/3cf67be3875fa0b91408ed0433779970e4f6acf5>`_  2021-06-16   ``Support non-https elasticsearch external links (#16489)``
`247ba31872 <https://github.com/apache/airflow/commit/247ba31872aa5a8a9e92f781a6beba75945ece1b>`_  2021-06-16   ``Fix Elasticsearch external log link with ''json_format'' (#16467)``
`5e12b3de31 <https://github.com/apache/airflow/commit/5e12b3de31dd1cf3d6e5088edbf497f91dcae4d8>`_  2021-06-16   ``Remove support jinja templated log_id in elasticsearch (#16465)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`e31e515b28 <https://github.com/apache/airflow/commit/e31e515b28a745b7428b42f1559ab456305fb3a0>`_  2021-06-15   ``Fix external elasticsearch logs link (#16357)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`e8d3de828f <https://github.com/apache/airflow/commit/e8d3de828f834ea9527c8d3d0d434675c0b3ee41>`_  2021-06-15   ``Add ElasticSearch Connection Doc (#16436)``
`5cd0bf733b <https://github.com/apache/airflow/commit/5cd0bf733b839951c075c54e808a595ac923c4e8>`_  2021-06-11   ``Support remote logging in elasticsearch with filebeat 7 (#14625)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`3bdcd1a7d4 <https://github.com/apache/airflow/commit/3bdcd1a7d46ca06115a93b97f394486e0acaf52d>`_  2021-06-05   ``Docs: Fix url for ''Elasticsearch'' (#16275)``
`476d0f6e3d <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
=================================================================================================  ===========  =====================================================================

1.0.4
.....

Latest change: 2021-05-01

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`657384615f <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`71c673e427 <https://github.com/apache/airflow/commit/71c673e427a89cae2a9f3174c32c5c85556d6342>`_  2021-04-22   ``Update Docstrings of Modules with Missing Params (#15391)``
`5da831910c <https://github.com/apache/airflow/commit/5da831910c358ecbd7a5c33ee31fe0d909508bea>`_  2021-04-10   ``Fix exception caused by missing keys in the ElasticSearch Record (#15163)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`eb884cd380 <https://github.com/apache/airflow/commit/eb884cd380f2f010b027bf6657f4da2e2e3309aa>`_  2021-03-14   ``Add elasticsearch to the fixes of backport providers (#14763)``
=================================================================================================  ===========  =============================================================================

1.0.3
.....

Latest change: 2021-03-13

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`662cb8c6ac <https://github.com/apache/airflow/commit/662cb8c6ac8becb26ff405f8b21acfccdd8de2ae>`_  2021-03-13   ``Prepare for releasing Elasticsearch Provider 1.0.3 (#14748)``
`923bde2b91 <https://github.com/apache/airflow/commit/923bde2b917099135adfe470a5453f663131fd5f>`_  2021-03-09   ``Elasticsearch Provider: Fix logs downloading for tasks (#14686)``
=================================================================================================  ===========  ===================================================================

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

=================================================================================================  ===========  =====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`b6bf253062 <https://github.com/apache/airflow/commit/b6bf25306243e78bf12528f9a080ea100a575641>`_  2020-12-25   ``Respect LogFormat when using ES logging with Json Format (#13310)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  =====================================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  =========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`2037303eef <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`61feb6ec45 <https://github.com/apache/airflow/commit/61feb6ec453f8dda1a0e1fe3ebcc0f1e3224b634>`_  2020-11-09   ``Provider's readmes generated for elasticsearch and google packages (#12194)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`ac943c9e18 <https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c>`_  2020-09-08   ``[AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499)``
`70f05ac677 <https://github.com/apache/airflow/commit/70f05ac6775152d856d212f845e9561282232844>`_  2020-09-01   ``Add 'log_id' field to log lines on ES handler (#10411)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d760265452 <https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2>`_  2020-08-25   ``PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`d5d119babc <https://github.com/apache/airflow/commit/d5d119babc97bbe3f3f690ad4a93e3b73bd3b172>`_  2020-07-21   ``Increase typing coverage for Elasticsearch (#9911)``
`a79e2d4c4a <https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407>`_  2020-07-06   ``Move provider's log task handlers to the provider package (#9604)``
`e13a14c873 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`65dd28eb77 <https://github.com/apache/airflow/commit/65dd28eb77d996ec8306c67d5ce1ccee2c14cc9d>`_  2020-02-18   ``[AIRFLOW-1202] Create Elasticsearch Hook (#7358)``
=================================================================================================  ===========  =========================================================================================
