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

Package apache-airflow-providers-ftp
------------------------------------------------------

`File Transfer Protocol (FTP) <https://tools.ietf.org/html/rfc114>`__


This is detailed commit list of changes for versions provider package: ``ftp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



3.6.1
.....

Latest change: 2023-10-31

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`63cc915cd3 <https://github.com/apache/airflow/commit/63cc915cd38a5034df6bf9c618e12f8690eeade0>`_  2023-10-31   ``Switch from Black to Ruff formatter (#35287)``
`ff30dcc1e1 <https://github.com/apache/airflow/commit/ff30dcc1e18abf267e4381bcc64a247da3c9af35>`_  2023-10-31   ``FTPHook methods should not change working directory (#35105)``
`d1c58d86de <https://github.com/apache/airflow/commit/d1c58d86de1267d9268a1efe0a0c102633c051a1>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
`3592ff4046 <https://github.com/apache/airflow/commit/3592ff40465032fa041600be740ee6bc25e7c242>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
`dd7ba3cae1 <https://github.com/apache/airflow/commit/dd7ba3cae139cb10d71c5ebc25fc496c67ee784e>`_  2023-10-19   ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
`b75f9e8806 <https://github.com/apache/airflow/commit/b75f9e880614fa0427e7d24a1817955f5de658b3>`_  2023-10-18   ``Upgrade pre-commits (#35033)``
`7a93b19138 <https://github.com/apache/airflow/commit/7a93b1913845710eb67ab4670c1be9e9382c030b>`_  2023-10-16   ``D401 Support - Providers: DaskExecutor to Github (Inclusive) (#34935)``
=================================================================================================  ===========  =========================================================================

3.6.0
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`e9987d5059 <https://github.com/apache/airflow/commit/e9987d50598f70d84cbb2a5d964e21020e81c080>`_  2023-10-13   ``Prepare docs 1st wave of Providers in October 2023 (#34916)``
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
=================================================================================================  ===========  ===============================================================

3.5.2
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`4f0e4254e4 <https://github.com/apache/airflow/commit/4f0e4254e4d8b0c3f14b7caf228e30f9e1577123>`_  2023-09-07   ``fix(providers/ftp): respect soft_fail argument when exception is raised (#34161)``
`9079093291 <https://github.com/apache/airflow/commit/907909329195c6655d1e2989b05609466ef50563>`_  2023-09-07   ``Consolidate importing of os.path.* (#34060)``
`a7310f9c91 <https://github.com/apache/airflow/commit/a7310f9c9127cf87a71e0bfa141c066d6a0bc82b>`_  2023-09-05   ``Refactor regex in providers (#33898)``
=================================================================================================  ===========  ====================================================================================

3.5.1
.....

Latest change: 2023-08-26

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`85acbb4ae9 <https://github.com/apache/airflow/commit/85acbb4ae9bc26248ca624fa4d289feccba00836>`_  2023-08-24   ``Refactor: Remove useless str() calls (#33629)``
`7e79997594 <https://github.com/apache/airflow/commit/7e799975948573ca2a1c4b2051d3eadc32bb8ba7>`_  2023-08-11   ``D205 Support - Providers - Final Pass (#33303)``
=================================================================================================  ===========  ============================================================

3.5.0
.....

Latest change: 2023-08-05

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`60677b0ba3 <https://github.com/apache/airflow/commit/60677b0ba3c9e81595ec2aa3d4be2737e5b32054>`_  2023-08-05   ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
`e1dd9b5cd2 <https://github.com/apache/airflow/commit/e1dd9b5cd2c5b19aec152f097709c7de02c42f34>`_  2023-07-30   ``openlineage, ftp: add OpenLineage support for FTPFileTransferOperator (#31354)``
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`d1aa509bbd <https://github.com/apache/airflow/commit/d1aa509bbd1941ceb3fe31789efeebbddd58d32f>`_  2023-06-28   ``D205 Support - Providers: Databricks to Github (inclusive) (#32243)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  ==================================================================================

3.4.2
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

3.4.1
.....

Latest change: 2023-05-24

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`d745cee3db <https://github.com/apache/airflow/commit/d745cee3dbde6b437a817aa64e385a1a948389d5>`_  2023-05-24   ``Prepare adhoc wave of Providers (#31478)``
`547e352578 <https://github.com/apache/airflow/commit/547e352578fac92f072b269dc257d21cdc279d97>`_  2023-05-23   ``Bring back min-airflow-version for preinstalled providers (#31469)``
=================================================================================================  ===========  ======================================================================

3.4.0
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
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  ======================================================================================

3.3.1
.....

Latest change: 2023-02-08

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`ce6ae2457e <https://github.com/apache/airflow/commit/ce6ae2457ef3d9f44f0086b58026909170bbf22a>`_  2023-02-08   ``Prepare docs for Feb 2023 wave of Providers (#29379)``
`2b7071c600 <https://github.com/apache/airflow/commit/2b7071c60022b3c483406839d3c0ef734db5daad>`_  2023-01-21   ``FTP operator has logic in __init__ (#29073)``
=================================================================================================  ===========  ========================================================

3.3.0
.....

Latest change: 2023-01-02

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`0e349d80bb <https://github.com/apache/airflow/commit/0e349d80bb329300b45875cdff8f8fc52141e82b>`_  2022-12-28   ``Add FTPSFileTransmitOperator (#28318)``
`39f501d4f4 <https://github.com/apache/airflow/commit/39f501d4f4e87635c80d97bb599daf61096d23b8>`_  2022-12-06   ``Add 'FTPFileTransmitOperator' (#26974)``
=================================================================================================  ===========  ========================================================

3.2.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
=================================================================================================  ===========  ====================================================================================

3.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`64412ee867 <https://github.com/apache/airflow/commit/64412ee867fe0918cc3b616b3fb0b72dcd88125c>`_  2022-07-07   ``Add blocksize arg for ftp hook (#24860)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
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
=================================================================================================  ===========  ==================================================================================

2.1.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

2.1.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

2.1.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`26e8d6d766 <https://github.com/apache/airflow/commit/26e8d6d7664bbaae717438bdb41766550ff57e4f>`_  2022-03-06   ``Updates FTPHook provider to have test_connection (#21997)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
=================================================================================================  ===========  ==========================================================================

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
`904709d34f <https://github.com/apache/airflow/commit/904709d34fbe0b6062d72932b72954afe13ec148>`_  2021-05-27   ``Check synctatic correctness for code-snippets (#16005)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
=================================================================================================  ===========  =================================================================

1.1.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`4b031d39e1 <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`7a0d412245 <https://github.com/apache/airflow/commit/7a0d4122459289e0f2db78ad2849d5ba42df4468>`_  2021-04-25   ``Add Connection Documentation to more Providers (#15408)``
`44a6648fd7 <https://github.com/apache/airflow/commit/44a6648fd7482f61ca59153f0caaf0b019c5b3fd>`_  2021-04-07   ``Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`6e6526a0f6 <https://github.com/apache/airflow/commit/6e6526a0f650119cb1ad7c2e2a1b87f0fa45c60e>`_  2021-03-13   ``Update documentation for broken package releases (#14734)``
=================================================================================================  ===========  ==============================================================================

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

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`bcdd3bb7bb <https://github.com/apache/airflow/commit/bcdd3bb7bb0e73ec957fa4077b025eb5c1fef90d>`_  2020-09-24   ``Increasing type coverage FTP (#11107)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`c60e476fb2 <https://github.com/apache/airflow/commit/c60e476fb24d4fa2eb192f8fce51edea4166f1d0>`_  2020-08-25   ``Remove mlsd function from hooks/ftp.py (#10538)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`027cc1682c <https://github.com/apache/airflow/commit/027cc1682c3b068dfeee143ca538b5e8dadfcd17>`_  2020-07-17   ``Improve type annotations for Ftp provider (#9868)``
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
`87969a350d <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`4bde99f132 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`74c2a6ded4 <https://github.com/apache/airflow/commit/74c2a6ded4d615de8e1b1c04a25146344138e920>`_  2020-03-23   ``Add call to Super class in 'ftp' & 'ssh' providers (#7822)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`9a04013b0e <https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2>`_  2020-01-27   ``[AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)``
=================================================================================================  ===========  ==================================================================================
