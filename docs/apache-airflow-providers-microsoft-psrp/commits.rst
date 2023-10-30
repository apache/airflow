
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


Package apache-airflow-providers-microsoft-psrp
------------------------------------------------------

This package provides remote execution capabilities via the
`PowerShell Remoting Protocol (PSRP)
<https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-psrp/>`__.


This is detailed commit list of changes for versions provider package: ``microsoft.psrp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



2.4.0
.....

Latest change: 2023-10-05

=================================================================================================  ===========  ==================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
=================================================================================================  ===========  ==================================================

2.3.2
.....

Latest change: 2023-09-08

=================================================================================================  ===========  ===================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`9d8c77e447 <https://github.com/apache/airflow/commit/9d8c77e447f5515b9a6aa85fa72511a86a128c28>`_  2023-08-27   ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
`bb2689ab45 <https://github.com/apache/airflow/commit/bb2689ab455ca5d82f4f9b8d4b73ec071849c439>`_  2023-08-21   ``Use str.splitlines() to split lines in providers (#33593)``
`b5a4d36383 <https://github.com/apache/airflow/commit/b5a4d36383c4143f46e168b8b7a4ba2dc7c54076>`_  2023-08-11   ``Prepare docs for Aug 2023 2nd wave of Providers (#33291)``
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`1240dcc167 <https://github.com/apache/airflow/commit/1240dcc167c4b47331db81deff61fc688df118c2>`_  2023-07-05   ``D205 Support - Providers: GRPC to Oracle (inclusive) (#32357)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  ===================================================================================================

2.3.1
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`8b146152d6 <https://github.com/apache/airflow/commit/8b146152d62118defb3004c997c89c99348ef948>`_  2023-06-20   ``Add note about dropping Python 3.7 for providers (#32015)``
=================================================================================================  ===========  =============================================================

2.3.0
.....

Latest change: 2023-05-19

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`45548b9451 <https://github.com/apache/airflow/commit/45548b9451fba4e48c6f0c0ba6050482c2ea2956>`_  2023-05-19   ``Prepare RC2 docs for May 2023 wave of Providers (#31416)``
`abea189022 <https://github.com/apache/airflow/commit/abea18902257c0250fedb764edda462f9e5abc84>`_  2023-05-18   ``Use '__version__' in providers not 'version' (#31393)``
`f5aed58d9f <https://github.com/apache/airflow/commit/f5aed58d9fb2137fa5f0e3ce75b6709bf8393a94>`_  2023-05-18   ``Fixing circular import error in providers caused by airflow version check (#31379)``
`d9ff55cf6d <https://github.com/apache/airflow/commit/d9ff55cf6d95bb342fed7a87613db7b9e7c8dd0f>`_  2023-05-16   ``Prepare docs for May 2023 wave of Providers (#31252)``
`0a30706aa7 <https://github.com/apache/airflow/commit/0a30706aa7c581905ca99a8b6e2f05960d480729>`_  2023-05-03   ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
`eef5bc7f16 <https://github.com/apache/airflow/commit/eef5bc7f166dc357fea0cc592d39714b1a5e3c14>`_  2023-05-03   ``Add full automation for min Airflow version for providers (#30994)``
`a7eb32a5b2 <https://github.com/apache/airflow/commit/a7eb32a5b222e236454d3e474eec478ded7c368d>`_  2023-04-30   ``Bump minimum Airflow version in providers (#30917)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
`41fade2d21 <https://github.com/apache/airflow/commit/41fade2d219c1841fafa439cc5dbb036f34ee32a>`_  2023-02-13   ``Upgrade Mypy to 1.0 (#29468)``
`6c1eeb5839 <https://github.com/apache/airflow/commit/6c1eeb58393173895944d5414793b38abdc7510d>`_  2023-02-11   ``Revert "Upgrade mypy to 0.991 (#28926)" (#29470)``
`6ae0a80cba <https://github.com/apache/airflow/commit/6ae0a80cbaf1d33343b763c7f82612b4522afc40>`_  2023-02-11   ``Upgrade mypy to 0.991 (#28926)``
=================================================================================================  ===========  ======================================================================================

2.2.0
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`3b6ced6512 <https://github.com/apache/airflow/commit/3b6ced6512752b18e48bc68616f898855c7d3c4c>`_  2023-01-03   ``Add option to add arguments to PSRP hook and operator (#27689)``
=================================================================================================  ===========  ==================================================================

2.1.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
=================================================================================================  ===========  ====================================================================================

2.0.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`e58985598f <https://github.com/apache/airflow/commit/e58985598f202395098e15b686aec33645a906ff>`_  2022-05-30   ``Ensure @contextmanager decorates generator func (#23103)``
`92ddcf4ac6 <https://github.com/apache/airflow/commit/92ddcf4ac6fa452c5056b1f7cad1fca4d5759802>`_  2022-05-27   ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
=================================================================================================  ===========  ==================================================================================

1.1.4
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ==========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`be0a4e4131 <https://github.com/apache/airflow/commit/be0a4e413117c5ceb0a245402cdfabe1a3d82ace>`_  2022-03-28   ``PowerShell Remoting fail on non-zero exitcode (#22503)``
=================================================================================================  ===========  ==========================================================

1.1.3
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

1.1.2
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

1.1.1
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ===========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`0a3ff43d41 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
=================================================================================================  ===========  ===========================================================

1.1.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`b8564daf50 <https://github.com/apache/airflow/commit/b8564daf50e049bdb27971104973b8981b7ea121>`_  2022-02-01   ``PSRP improvements (#19806)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`e63e23c582 <https://github.com/apache/airflow/commit/e63e23c582cd757ea6593bdb4dfde66d76a8c9f1>`_  2021-12-23   ``Fixing MyPy issues inside providers/microsoft (#20409)``
=================================================================================================  ===========  ==========================================================================

1.0.1
.....

Latest change: 2021-09-03

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`fd5d65751c <https://github.com/apache/airflow/commit/fd5d65751ca026d2b5f0ec1e4d9ce1b1e09e5b22>`_  2021-09-03   ``Update release notes for 3 extra providers released (#18018)``
`9c644194ed <https://github.com/apache/airflow/commit/9c644194ed6bc6a3a065b72ea7cf89c02d1c5275>`_  2021-09-03   ``Fix unexpected bug in exiting hook context manager (#18014)``
=================================================================================================  ===========  ================================================================

1.0.0
.....

Latest change: 2021-08-27

=================================================================================================  ===========  ============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================
`41632e03b8 <https://github.com/apache/airflow/commit/41632e03b8caf71de308414c48e9cb211a083761>`_  2021-08-27   ``Fix provider.yaml errors due to exit(0) in test (#17858)``
`69d2ed65cb <https://github.com/apache/airflow/commit/69d2ed65cb7c9384d309ae5e499d5798c2c3ac96>`_  2021-08-08   ``Add Microsoft PSRP provider (#17361)``
=================================================================================================  ===========  ============================================================
