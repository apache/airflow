
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

Package apache-airflow-providers-smtp
------------------------------------------------------

`Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc5321>`__


This is detailed commit list of changes for versions provider package: ``smtp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.4.1
.....

Latest change: 2023-10-28

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`7857ca2d20 <https://github.com/apache/airflow/commit/7857ca2d203f4a672fc618c41027d7f4b67d4320>`_  2023-10-28   ``feat: make 'cc' and 'bcc' templated fields in EmailOperator (#35235)``
`3592ff4046 <https://github.com/apache/airflow/commit/3592ff40465032fa041600be740ee6bc25e7c242>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
`dd7ba3cae1 <https://github.com/apache/airflow/commit/dd7ba3cae139cb10d71c5ebc25fc496c67ee784e>`_  2023-10-19   ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
`b75f9e8806 <https://github.com/apache/airflow/commit/b75f9e880614fa0427e7d24a1817955f5de658b3>`_  2023-10-18   ``Upgrade pre-commits (#35033)``
=================================================================================================  ===========  ========================================================================

1.4.0
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`e9987d5059 <https://github.com/apache/airflow/commit/e9987d50598f70d84cbb2a5d964e21020e81c080>`_  2023-10-13   ``Prepare docs 1st wave of Providers in October 2023 (#34916)``
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
=================================================================================================  ===========  ===============================================================

1.3.2
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ===================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`875387afa5 <https://github.com/apache/airflow/commit/875387afa53c207364fa20b515d154100b5d0a8d>`_  2023-09-01   ``Refactor unneeded  jumps in providers (#33833)``
`9d8c77e447 <https://github.com/apache/airflow/commit/9d8c77e447f5515b9a6aa85fa72511a86a128c28>`_  2023-08-27   ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
=================================================================================================  ===========  ===================================================================================================

1.3.1
.....

Latest change: 2023-08-26

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`7700fb12cc <https://github.com/apache/airflow/commit/7700fb12cc6c7a97901662e6ac6aa1e4e932d969>`_  2023-08-20   ``Simplify 'X for X in Y' to 'Y' where applicable (#33453)``
=================================================================================================  ===========  ============================================================

1.3.0
.....

Latest change: 2023-08-05

=================================================================================================  ===========  =====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================================
`60677b0ba3 <https://github.com/apache/airflow/commit/60677b0ba3c9e81595ec2aa3d4be2737e5b32054>`_  2023-08-05   ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
`cf7e0c5aa5 <https://github.com/apache/airflow/commit/cf7e0c5aa5ccc7b8a3963b14eadde0c8bc7c4eb7>`_  2023-08-04   ``Add possibility to use 'ssl_context' extra for SMTP and IMAP connections (#33112)``
`e20325db38 <https://github.com/apache/airflow/commit/e20325db38fdfdd9db423a345b13d18aab6fe578>`_  2023-08-04   ``Allows to choose SSL context for SMTP provider (#33075)``
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`e45bee8840 <https://github.com/apache/airflow/commit/e45bee884068399e7265421511e17fed106ce5b4>`_  2023-07-05   ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  =====================================================================================

1.2.0
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`8b146152d6 <https://github.com/apache/airflow/commit/8b146152d62118defb3004c997c89c99348ef948>`_  2023-06-20   ``Add note about dropping Python 3.7 for providers (#32015)``
`b7796895cb <https://github.com/apache/airflow/commit/b7796895cb41d8e5e79e6d8eee150b11d8c302a7>`_  2023-06-07   ``Fix ruff static check (#31762)``
`cce4ca5505 <https://github.com/apache/airflow/commit/cce4ca55058b605d19841bb9d43043f0d45665cb>`_  2023-06-07   ``Add notifier for Smtp (#31359)``
`a59076eaee <https://github.com/apache/airflow/commit/a59076eaeed03dd46e749ad58160193b4ef3660c>`_  2023-06-02   ``Add D400 pydocstyle check - Providers (#31427)``
=================================================================================================  ===========  =============================================================

1.1.0
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
=================================================================================================  ===========  ======================================================================================

1.0.1
.....

Latest change: 2023-04-09

=================================================================================================  ===========  =======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================================
`874ea9588e <https://github.com/apache/airflow/commit/874ea9588e3ce7869759440302e53bb6a730a11e>`_  2023-04-09   ``Prepare docs for ad hoc release of Providers (#30545)``
`806b0279ac <https://github.com/apache/airflow/commit/806b0279acd5897e2ad6b816764bb25b4bcdf5b0>`_  2023-04-09   ``Accept None for 'EmailOperator.from_email' to load it from smtp connection (#30533)``
`a15e734785 <https://github.com/apache/airflow/commit/a15e73478521707487e1a6d6f7ef7f213b282023>`_  2023-04-07   ``'EmailOperator': fix wrong assignment of 'from_email' (#30524)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  =======================================================================================

1.0.0
.....

Latest change: 2023-03-14

=================================================================================================  ===========  ===================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================
`3fac5c3540 <https://github.com/apache/airflow/commit/3fac5c35409ccfde771ce08ea8daeaac056b2c10>`_  2023-03-14   ``Creating SMTP provider (#29968)``
=================================================================================================  ===========  ===================================
