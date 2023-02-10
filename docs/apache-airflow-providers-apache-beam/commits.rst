
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


Package apache-airflow-providers-apache-beam
------------------------------------------------------

`Apache Beam <https://beam.apache.org/>`__.


This is detailed commit list of changes for versions provider package: ``apache.beam``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.2.0
.....

Latest change: 2023-01-23

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`3374fdfcbd <https://github.com/apache/airflow/commit/3374fdfcbddb630b4fc70ceedd5aed673e6c0a0d>`_  2023-01-23   ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``
`8c4303e1ac <https://github.com/apache/airflow/commit/8c4303e1ace0774244b556a8d86a19058af2b16d>`_  2023-01-18   ``Add support for running a Beam Go pipeline with an executable binary (#28764)``
=================================================================================================  ===========  =================================================================================

4.1.1
.....

Latest change: 2023-01-14

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`8da678ccd2 <https://github.com/apache/airflow/commit/8da678ccd2e5a30f9c2d22c7526b7a238c185d2f>`_  2023-01-03   ``Ensure Beam Go file downloaded from GCS still exists when referenced (#28664)``
=================================================================================================  ===========  =================================================================================

4.1.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`e8ab8ccc0e <https://github.com/apache/airflow/commit/e8ab8ccc0e7b82efc0dbf8bd31e0bbf57b1d5637>`_  2022-11-11   ``Add backward compatibility with old versions of Apache Beam (#27263)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
=================================================================================================  ===========  ====================================================================================

4.0.0
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
`34e0ab9b23 <https://github.com/apache/airflow/commit/34e0ab9b23e0dcf416169777240b97f4de08f772>`_  2022-06-05   ``AIP-47 - Migrate beam DAGs to new design #22439 (#24211)``
`41e94b475e <https://github.com/apache/airflow/commit/41e94b475e06f63db39b0943c9d9a7476367083c>`_  2022-05-31   ``Support impersonation service account parameter for Dataflow runner (#23961)``
`4a5250774b <https://github.com/apache/airflow/commit/4a5250774be8f48629294785801879277f42cc62>`_  2022-05-30   ``Added missing project_id to the wait_for_job (#24020)``
=================================================================================================  ===========  ==================================================================================

3.4.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`de65a5cc5a <https://github.com/apache/airflow/commit/de65a5cc5acaa1fc87ae8f65d367e101034294a6>`_  2022-04-25   ``Support serviceAccount attr for dataflow in the Apache beam``
=================================================================================================  ===========  ===============================================================

3.3.0
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`4a1503b39b <https://github.com/apache/airflow/commit/4a1503b39b0aaf50940c29ac886c6eeda35a79ff>`_  2022-03-17   ``Add recipe for BeamRunGoPipelineOperator (#22296)``
=================================================================================================  ===========  ==============================================================

3.2.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

3.2.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`295efd36ea <https://github.com/apache/airflow/commit/295efd36eac074578e4b54a69d71c2924984326d>`_  2022-02-17   ``Dataflow Assets (#21639)``
`da485da29a <https://github.com/apache/airflow/commit/da485da29a06ecdda720a7ba75f04a2680aac0a2>`_  2022-02-13   ``Add support for BeamGoPipelineOperator (#20386)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`a71332ebc6 <https://github.com/apache/airflow/commit/a71332ebc6375ba9907c84103a7e8f774ba9001a>`_  2022-01-01   ``Fix mypy apache beam operators (#20610)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`43efde6230 <https://github.com/apache/airflow/commit/43efde6230487b003f715e04d195126f63f261ff>`_  2021-12-15   ``Fix MyPy Errors for Apache Beam (and Dataflow) provider. (#20301)``
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`7640ba4e8e <https://github.com/apache/airflow/commit/7640ba4e8ee239d6e2bbf950d53d624b9df93059>`_  2021-11-29   ``Fix broken anchors markdown files (#19847)``
`ae044884d1 <https://github.com/apache/airflow/commit/ae044884d1dacce8dbf47c618f543b58f9ff101f>`_  2021-11-03   ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``
=================================================================================================  ===========  ==============================================================================

3.1.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ===================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`a418fd96f7 <https://github.com/apache/airflow/commit/a418fd96f70eac1d4d7dc91553f41d5153beda93>`_  2021-10-17   ``Use google cloud credentials when executing beam command in subprocess (#18992)``
=================================================================================================  ===========  ===================================================================================

3.0.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
=================================================================================================  ===========  ===================================================================

3.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`1e647029e4 <https://github.com/apache/airflow/commit/1e647029e469c1bb17e9ad051d0184f3357644c3>`_  2021-06-01   ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
`904709d34f <https://github.com/apache/airflow/commit/904709d34fbe0b6062d72932b72954afe13ec148>`_  2021-05-27   ``Check synctatic correctness for code-snippets (#16005)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`0f97a3970d <https://github.com/apache/airflow/commit/0f97a3970d2c652beedbf2fbaa33e2b2bfd69bce>`_  2021-05-04   ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
=================================================================================================  ===========  ==============================================================================

2.0.0
.....

Latest change: 2021-04-29

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`814e471d13 <https://github.com/apache/airflow/commit/814e471d137aad68bd64a21d20736e7b88403f97>`_  2021-04-29   ``Update pre-commit checks (#15583)``
`40a2476a5d <https://github.com/apache/airflow/commit/40a2476a5db14ee26b5108d72635da116eab720b>`_  2021-04-28   ``Adds interactivity when generating provider documentation. (#15518)``
`4b031d39e1 <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`e229f3541d <https://github.com/apache/airflow/commit/e229f3541dd764db54785625875a7c5e94225736>`_  2021-04-27   ``Use Pip 21.* to install airflow officially (#15513)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
=================================================================================================  ===========  =======================================================================

1.0.1
.....

Latest change: 2021-03-08

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`b753c7fa60 <https://github.com/apache/airflow/commit/b753c7fa60e8d92bbaab68b557a1fbbdc1ec5dd0>`_  2021-03-08   ``Prepare ad-hoc release of the four previously excluded providers (#14655)``
`4e57630606 <https://github.com/apache/airflow/commit/4e5763060683456405ab6173cdee1f2facc231e5>`_  2021-03-03   ``Remove WARNINGs from BeamHook (#14554)``
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`8a731f536c <https://github.com/apache/airflow/commit/8a731f536cc946cc62c20921187354b828df931e>`_  2021-02-05   ``Improve Apache Beam operators - refactor operator - common Dataflow logic (#14094)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  ======================================================================================

1.0.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`d45739f7ce <https://github.com/apache/airflow/commit/d45739f7ce0de183329d67fff88a9da3943a9280>`_  2021-02-04   ``Fixes to release process after releasing 2nd wave of providers (#14059)``
`1872d8719d <https://github.com/apache/airflow/commit/1872d8719d24f94aeb1dcba9694837070b9884ca>`_  2021-02-03   ``Add Apache Beam operators (#12814)``
=================================================================================================  ===========  ===========================================================================
