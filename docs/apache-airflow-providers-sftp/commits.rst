
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


Package apache-airflow-providers-sftp
------------------------------------------------------

`SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__


This is detailed commit list of changes for versions provider package: ``sftp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.2.4
.....

Latest change: 2023-02-28

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`9357c81828 <https://github.com/apache/airflow/commit/9357c81828626754c990c3e8192880511a510544>`_  2023-02-28   ``Fix SFTPSensor when using newer_than and there are multiple matched files (#29794)``
=================================================================================================  ===========  ======================================================================================

4.2.3
.....

Latest change: 2023-02-18

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`470fdaea27 <https://github.com/apache/airflow/commit/470fdaea275660970777c0f72b8867b382eabc14>`_  2023-02-18   ``Prepare docs for 02 2023 midmonth wave of Providers (#29589)``
`8e24387d6d <https://github.com/apache/airflow/commit/8e24387d6db177c662342245bb183bfd73fb9ee8>`_  2023-02-13   ``Fix sftp sensor with pattern (#29467)``
=================================================================================================  ===========  ================================================================

4.2.2
.....

Latest change: 2023-02-08

=================================================================================================  ===========  ===========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================
`ce6ae2457e <https://github.com/apache/airflow/commit/ce6ae2457ef3d9f44f0086b58026909170bbf22a>`_  2023-02-08   ``Prepare docs for Feb 2023 wave of Providers (#29379)``
`2b7071c600 <https://github.com/apache/airflow/commit/2b7071c60022b3c483406839d3c0ef734db5daad>`_  2023-01-21   ``FTP operator has logic in __init__ (#29073)``
`bac7b3027d <https://github.com/apache/airflow/commit/bac7b3027d57d2a31acb9a2d078c6af4dc777162>`_  2023-01-20   ``Fix SFTP operator's template fields processing (#29068)``
=================================================================================================  ===========  ===========================================================

4.2.1
.....

Latest change: 2023-01-02

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`c0a7bf2434 <https://github.com/apache/airflow/commit/c0a7bf243461bf5e546367094e46eaab41e3831e>`_  2022-12-24   ``Update codespell and fix typos (#28568)``
`4a3a429658 <https://github.com/apache/airflow/commit/4a3a42965801823c39baaccfa96c5e4cffae4012>`_  2022-12-03   ``[misc] Get rid of 'pass' statement in conditions (#27775)``
=================================================================================================  ===========  =============================================================

4.2.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`662a8aaac3 <https://github.com/apache/airflow/commit/662a8aaac302aae9e1768c7c55e15aa8c539208e>`_  2022-10-25   ``SFTP Provider: Fix default folder permissions  (#26593)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
=================================================================================================  ===========  =========================================================================

4.1.0
.....

Latest change: 2022-09-28

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`5f073e38dd <https://github.com/apache/airflow/commit/5f073e38dd46217b64dbc16d7b1055d89e8c3459>`_  2022-09-27   ``SFTPOperator - add support for list of file paths (#26666)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
=================================================================================================  ===========  ====================================================================================

4.0.0
.....

Latest change: 2022-07-17

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`abb0341135 <https://github.com/apache/airflow/commit/abb034113540b708e87379665a1b5caadb8748bc>`_  2022-07-17   ``Add documentation for follow-up release for june providers (#25111)``
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`acaa0635c8 <https://github.com/apache/airflow/commit/acaa0635c8477c98ab78da9f6d86e6f1bad2737d>`_  2022-07-08   ``Automatically detect if non-lazy logging interpolation is used (#24910)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`f3aacebe50 <https://github.com/apache/airflow/commit/f3aacebe502c4ea5dc2b7d29373539296fa037eb>`_  2022-06-20   ``Convert sftp hook to use paramiko instead of pysftp (#24512)``
`ad297c9177 <https://github.com/apache/airflow/commit/ad297c91777277e2b76dd7b7f0e3e3fc5c32e07c>`_  2022-06-17   ``Update 'actual_file_to_check' with rendered 'path' (#24451)``
=================================================================================================  ===========  ===========================================================================

3.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`e656e1de55 <https://github.com/apache/airflow/commit/e656e1de55094e8369cab80b9b1669b1d1225f54>`_  2022-06-06   ``Adding fnmatch type regex to SFTPSensor (#24084)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
=================================================================================================  ===========  ==================================================================================

2.6.0
.....

Latest change: 2022-04-07

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`a6609d5268 <https://github.com/apache/airflow/commit/a6609d5268ebe55bcb150a828d249153582aa936>`_  2022-03-29   ``add newer_than parameter to SFTP sensor (#21655) (#22377)``
=================================================================================================  ===========  =============================================================

2.5.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

2.5.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

2.5.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`26e8d6d766 <https://github.com/apache/airflow/commit/26e8d6d7664bbaae717438bdb41766550ff57e4f>`_  2022-03-06   ``Updates FTPHook provider to have test_connection (#21997)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`cb73053211 <https://github.com/apache/airflow/commit/cb73053211367e2c2dd76d5279cdc7dc7b190124>`_  2022-01-27   ``Add optional features in providers. (#21074)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
=================================================================================================  ===========  ==========================================================================

2.4.1
.....

Latest change: 2022-01-08

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`da9210e89c <https://github.com/apache/airflow/commit/da9210e89c618611b1e450617277b738ce92ffd7>`_  2022-01-08   ``Add documentation for an ad-hoc release of 2 providers (#20765)``
`c2fc760c90 <https://github.com/apache/airflow/commit/c2fc760c9024ce9f0bec287679d9981f6ab1fd98>`_  2022-01-08   ``Bugfix: ''SFTPHook'' does not respect ''ssh_conn_id'' arg (#20756)``
`1c113d6b2f <https://github.com/apache/airflow/commit/1c113d6b2fe2f24a71f0a2c3911e8141fa6e3ff9>`_  2022-01-06   ``fix deprecation messages for SFTPHook (#20692)``
=================================================================================================  ===========  ======================================================================

2.4.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`f35ad27080 <https://github.com/apache/airflow/commit/f35ad27080a2e1f29efc20a9bd0613af0f6ff2ec>`_  2021-12-14   ``Making SFTPHook's constructor consistent with its superclass SSHHook (#20164)``
`756b1207a9 <https://github.com/apache/airflow/commit/756b1207a96ff65f7e0f83864a7ad0386df9fa9d>`_  2021-12-13   ``Fix MyPy Errors for SFTP provider (#20242)``
=================================================================================================  ===========  =================================================================================

2.3.0
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`ccb809550d <https://github.com/apache/airflow/commit/ccb809550db95089178385184cc898f5412cd266>`_  2021-11-16   ``Add test_connection method for sftp hook (#19609)``
=================================================================================================  ===========  ======================================================================

2.2.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`83eb80d4ec <https://github.com/apache/airflow/commit/83eb80d4eccb881775dd44bab19818e50e42270b>`_  2021-10-27   ``SFTP hook to prefer the SSH paramiko key over the key file path (#18988)``
`86a2a19ad2 <https://github.com/apache/airflow/commit/86a2a19ad2bdc87a9ad14bb7fde9313b2d7489bb>`_  2021-10-17   ``More f-strings (#18855)``
=================================================================================================  ===========  ============================================================================

2.1.1
.....

Latest change: 2021-08-30

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
=================================================================================================  ===========  ============================================================================

2.1.0
.....

Latest change: 2021-07-26

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`81be82bfb7 <https://github.com/apache/airflow/commit/81be82bfb73f263ecd3b2d5f664e9c1ea751408a>`_  2021-07-02   ``Remove redundant logging in SFTP Hook (#16704)``
`50e334df32 <https://github.com/apache/airflow/commit/50e334df3245072f55ce75c2611dca2df0cbd031>`_  2021-06-24   ``Add support for non-RSA type key for SFTP hook (#16314)``
=================================================================================================  ===========  ===================================================================

2.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  ====================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9351e2a2d4 <https://github.com/apache/airflow/commit/9351e2a2d43b6ec3c019df19afb445a3dcf587ac>`_  2021-06-10   ``Depreciate private_key_pass in SFTPHook conn extra and rename to private_key_passphrase (#14028)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
=================================================================================================  ===========  ====================================================================================================

1.2.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`7a0d412245 <https://github.com/apache/airflow/commit/7a0d4122459289e0f2db78ad2849d5ba42df4468>`_  2021-04-25   ``Add Connection Documentation to more Providers (#15408)``
`15e044c7e4 <https://github.com/apache/airflow/commit/15e044c7e412a85946a8831dd7eb68424d96c164>`_  2021-04-14   ``Undeprecate private_key option in SFTPHook (#15348)``
`44a6648fd7 <https://github.com/apache/airflow/commit/44a6648fd7482f61ca59153f0caaf0b019c5b3fd>`_  2021-04-07   ``Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
=================================================================================================  ===========  ==============================================================================

1.1.1
.....

Latest change: 2021-02-27

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  =======================================================================

1.1.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`d45739f7ce <https://github.com/apache/airflow/commit/d45739f7ce0de183329d67fff88a9da3943a9280>`_  2021-02-04   ``Fixes to release process after releasing 2nd wave of providers (#14059)``
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`9af452aee5 <https://github.com/apache/airflow/commit/9af452aee53dd300535dadd108ce3e60c5f11a2e>`_  2021-02-03   ``Add retryer to SFTP hook connection (#13065)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`52339a55c0 <https://github.com/apache/airflow/commit/52339a55c054bddd1d46253575274a3d5d141ebe>`_  2021-01-08   ``[AIRFLOW-7044] Host key can be specified via SSH connection extras. (#12944)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  ================================================================================

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
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`ae791e1916 <https://github.com/apache/airflow/commit/ae791e19163b4ee6e84940d2567adbf0a2626fb4>`_  2020-10-21   ``Fix formatting errors introduced in #11720 (#11733)``
`1fb3c28e1a <https://github.com/apache/airflow/commit/1fb3c28e1a4fd54c9d83dccd413659a7a87c7315>`_  2020-10-21   ``Add support for setting ciphers for SFTPHook (#11720)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`d305876bee <https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731>`_  2020-10-12   ``Remove redundant None provided as default to dict.get() (#11448)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`0161b5ea2b <https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1>`_  2020-09-26   ``Increasing type coverage for multiple provider (#11159)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`4e3799fec4 <https://github.com/apache/airflow/commit/4e3799fec4c23d0f43603a0489c5a6158aeba035>`_  2020-08-02   ``[AIRFLOW-4541] Replace os.mkdirs usage with pathlib.Path(path).mkdir (#10117)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
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
`bac0ab27cf <https://github.com/apache/airflow/commit/bac0ab27cfc89e715efddc97214fcd7738084361>`_  2020-03-30   ``close sftp connection without error (#7953)``
`42eef38217 <https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e>`_  2020-03-07   ``[AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`ceea293c16 <https://github.com/apache/airflow/commit/ceea293c1652240e7e856c201e4341a87ef97a0f>`_  2020-01-28   ``[AIRFLOW-6656] Fix AIP-21 moving (#7272)``
`9a04013b0e <https://github.com/apache/airflow/commit/9a04013b0e40b0d744ff4ac9f008491806d60df2>`_  2020-01-27   ``[AIRFLOW-6646][AIP-21] Move protocols classes to providers package (#7268)``
`69629a5a94 <https://github.com/apache/airflow/commit/69629a5a948ab2c4ac04a4a4dca6ac86d19c11bd>`_  2019-12-09   ``[AIRFLOW-5807] Move SFTP from contrib to providers. (#6464)``
=================================================================================================  ===========  ==================================================================================
