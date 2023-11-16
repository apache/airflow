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

Package apache-airflow-providers-apache-spark
------------------------------------------------------

`Apache Spark <https://spark.apache.org/>`__


This is detailed commit list of changes for versions provider package: ``apache.spark``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.4.0
.....

Latest change: 2023-11-07

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`3b8db612ff <https://github.com/apache/airflow/commit/3b8db612ff39abbf9e965081c859e9e439ed832d>`_  2023-11-07   ``Add information about Qubole removal and make it possible to release it (#35492)``
`0a4ed7d557 <https://github.com/apache/airflow/commit/0a4ed7d557809ad81ecc50d197c33c8d178c42ce>`_  2023-11-01   ``Add pyspark decorator (#35247)``
`880a85bbb7 <https://github.com/apache/airflow/commit/880a85bbb704724492a7a727583e0c81341e78e1>`_  2023-11-01   ``Add use_krb5ccache option to SparkSubmitOperator (#35331)``
=================================================================================================  ===========  ====================================================================================

4.3.0
.....

Latest change: 2023-10-28

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`d1c58d86de <https://github.com/apache/airflow/commit/d1c58d86de1267d9268a1efe0a0c102633c051a1>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
`3592ff4046 <https://github.com/apache/airflow/commit/3592ff40465032fa041600be740ee6bc25e7c242>`_  2023-10-28   ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
`645d52f129 <https://github.com/apache/airflow/commit/645d52f1298c49b2111d058971e1a9f159f1e257>`_  2023-10-21   ``Add 'use_krb5ccache' option to 'SparkSubmitHook' (#34386)``
`dd7ba3cae1 <https://github.com/apache/airflow/commit/dd7ba3cae139cb10d71c5ebc25fc496c67ee784e>`_  2023-10-19   ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
`b75f9e8806 <https://github.com/apache/airflow/commit/b75f9e880614fa0427e7d24a1817955f5de658b3>`_  2023-10-18   ``Upgrade pre-commits (#35033)``
=================================================================================================  ===========  ==================================================================

4.2.0
.....

Latest change: 2023-10-13

=================================================================================================  ===========  ===============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================
`e9987d5059 <https://github.com/apache/airflow/commit/e9987d50598f70d84cbb2a5d964e21020e81c080>`_  2023-10-13   ``Prepare docs 1st wave of Providers in October 2023 (#34916)``
`0c8e30e43b <https://github.com/apache/airflow/commit/0c8e30e43b70e9d033e1686b327eb00aab82479c>`_  2023-10-05   ``Bump min airflow version of providers (#34728)``
`7ebf4220c9 <https://github.com/apache/airflow/commit/7ebf4220c9abd001f1fa23c95f882efddd5afbac>`_  2023-09-28   ``Refactor usage of str() in providers (#34320)``
=================================================================================================  ===========  ===============================================================

4.1.5
.....

Latest change: 2023-09-08

=================================================================================================  ===========  =============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================
`21990ed894 <https://github.com/apache/airflow/commit/21990ed8943ee4dc6e060ee2f11648490c714a3b>`_  2023-09-08   ``Prepare docs for 09 2023 - 1st wave of Providers (#34201)``
`a7310f9c91 <https://github.com/apache/airflow/commit/a7310f9c9127cf87a71e0bfa141c066d6a0bc82b>`_  2023-09-05   ``Refactor regex in providers (#33898)``
=================================================================================================  ===========  =============================================================

4.1.4
.....

Latest change: 2023-08-26

=================================================================================================  ===========  =======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =======================================================================
`c077d19060 <https://github.com/apache/airflow/commit/c077d190609f931387c1fcd7b8cc34f12e2372b9>`_  2023-08-26   ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
`32feab4100 <https://github.com/apache/airflow/commit/32feab41006897de182bfa684813be230027aca1>`_  2023-08-22   ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``
`c645d8e40c <https://github.com/apache/airflow/commit/c645d8e40c167ea1f6c332cdc3ea0ca5a9363205>`_  2023-08-12   ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
=================================================================================================  ===========  =======================================================================

4.1.3
.....

Latest change: 2023-08-05

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`60677b0ba3 <https://github.com/apache/airflow/commit/60677b0ba3c9e81595ec2aa3d4be2737e5b32054>`_  2023-08-05   ``Prepare docs for Aug 2023 1st wave of Providers (#33128)``
`4f83e831d2 <https://github.com/apache/airflow/commit/4f83e831d2e6985b6c82b2e0c45673b58ef81074>`_  2023-07-31   ``Validate conn_prefix in extra field for Spark JDBC hook (#32946)``
=================================================================================================  ===========  ====================================================================

4.1.2
.....

Latest change: 2023-07-29

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`d06b7af69a <https://github.com/apache/airflow/commit/d06b7af69a65c50321ba2a9904551f3b8affc7f1>`_  2023-07-29   ``Prepare docs for July 2023 3rd wave of Providers (#32875)``
`e93460383f <https://github.com/apache/airflow/commit/e93460383f287f9b2af4b6bda3ea6ba17ba3c08b>`_  2023-07-26   ``Move all k8S classes to cncf.kubernetes provider (#32767)``
`225e3041d2 <https://github.com/apache/airflow/commit/225e3041d269698d0456e09586924c1898d09434>`_  2023-07-06   ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
`3878fe6fab <https://github.com/apache/airflow/commit/3878fe6fab3ccc1461932b456c48996f2763139f>`_  2023-07-05   ``Remove spurious headers for provider changelogs (#32373)``
`cb4927a018 <https://github.com/apache/airflow/commit/cb4927a01887e2413c45d8d9cb63e74aa994ee74>`_  2023-07-05   ``Prepare docs for July 2023 wave of Providers (#32298)``
`8c37b74a20 <https://github.com/apache/airflow/commit/8c37b74a208a808d905c1b86d081d69d7a1aa900>`_  2023-06-28   ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
`09d4718d3a <https://github.com/apache/airflow/commit/09d4718d3a46aecf3355d14d3d23022002f4a818>`_  2023-06-27   ``Improve provider documentation and README structure (#32125)``
=================================================================================================  ===========  ===================================================================

4.1.1
.....

Latest change: 2023-06-20

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`79bcc2e668 <https://github.com/apache/airflow/commit/79bcc2e668e648098aad6eaa87fe8823c76bc69a>`_  2023-06-20   ``Prepare RC1 docs for June 2023 wave of Providers (#32001)``
`8b146152d6 <https://github.com/apache/airflow/commit/8b146152d62118defb3004c997c89c99348ef948>`_  2023-06-20   ``Add note about dropping Python 3.7 for providers (#32015)``
`6becb70316 <https://github.com/apache/airflow/commit/6becb7031618867bc253aefc9e3e216629575d2d>`_  2023-06-16   ``SparkSubmitOperator: rename spark_conn_id to conn_id (#31952)``
`13890788ae <https://github.com/apache/airflow/commit/13890788ae939328d451daeaea54f493f4aaaa69>`_  2023-06-07   ``Apache provider docstring improvements (#31730)``
`9276310a43 <https://github.com/apache/airflow/commit/9276310a43d17a9e9e38c2cb83686a15656896b2>`_  2023-06-05   ``Improve docstrings in providers (#31681)``
`a473facf6c <https://github.com/apache/airflow/commit/a473facf6c0b36f7d051ecc2d1aa94ba6957468d>`_  2023-06-01   ``Add D400 pydocstyle check - Apache providers only (#31424)``
=================================================================================================  ===========  =================================================================

4.1.0
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

4.0.1
.....

Latest change: 2023-04-02

=================================================================================================  ===========  =====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================
`55dbf1ff1f <https://github.com/apache/airflow/commit/55dbf1ff1fb0b22714f695a66f6108b3249d1199>`_  2023-04-02   ``Prepare docs for April 2023 wave of Providers (#30378)``
`5d1f201bb0 <https://github.com/apache/airflow/commit/5d1f201bb0411d7060fd4fe49807fd49495f973e>`_  2023-03-22   ``Only restrict spark binary passed via extra (#30213)``
`d9dea5ce17 <https://github.com/apache/airflow/commit/d9dea5ce17f0c5859dc705bba8e6ef22e5659955>`_  2023-03-22   ``Validate host and schema for Spark JDBC Hook (#30223)``
`b3259877fa <https://github.com/apache/airflow/commit/b3259877fac7330d2b65ca7f96fcfc27243582d6>`_  2023-03-15   ``Add spark3-submit to list of allowed spark-binary values (#30068)``
=================================================================================================  ===========  =====================================================================

4.0.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`9358928815 <https://github.com/apache/airflow/commit/93589288156d56aff4b1f822b77695e3c58e4568>`_  2022-11-13   ``Remove custom spark home and custom binarires for spark (#27646)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
=================================================================================================  ===========  ====================================================================================

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
`9dc2851671 <https://github.com/apache/airflow/commit/9dc2851671cd5cdce445f01f380985f2d7a9b4cf>`_  2022-06-05   ``Fix backwards-compatibility introduced by fixing mypy problems (#24230)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`a2bfc0e62d <https://github.com/apache/airflow/commit/a2bfc0e62dddb8b4e17d833bdf22d282cb265935>`_  2022-06-05   ``AIP-47 - Migrate spark DAGs to new design #22439 (#24210)``
`71e4deb1b0 <https://github.com/apache/airflow/commit/71e4deb1b093b7ad9320eb5eb34eca8ea440a238>`_  2022-05-16   ``Add typing for airflow/configuration.py (#23716)``
=================================================================================================  ===========  ==================================================================================

2.1.3
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
=================================================================================================  ===========  ==============================================================

2.1.2
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

2.1.1
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ===========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`6322dad2ca <https://github.com/apache/airflow/commit/6322dad2caa0e5b4d339c5d9a73ec7ff3fd4bc25>`_  2022-02-25   ``fix param rendering in docs of SparkSubmitHook (#21788)``
=================================================================================================  ===========  ===========================================================

2.1.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`39e395f981 <https://github.com/apache/airflow/commit/39e395f9816c04ef2f033eb0b4f635fc3018d803>`_  2022-02-04   ``Add more SQL template fields renderers (#21237)``
`cb73053211 <https://github.com/apache/airflow/commit/cb73053211367e2c2dd76d5279cdc7dc7b190124>`_  2022-01-27   ``Add optional features in providers. (#21074)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
=================================================================================================  ===========  ==========================================================================

2.0.3
.....

Latest change: 2021-12-31

=================================================================================================  ===========  ================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`485ff6cc64 <https://github.com/apache/airflow/commit/485ff6cc64d8f6a15d8d6a0be50661fe6d04b2d9>`_  2021-12-29   ``Fix MyPy errors in Apache Providers (#20422)``
`dad2f8103b <https://github.com/apache/airflow/commit/dad2f8103be954afaedf15e9d098ee417b0d5d02>`_  2021-12-15   ``Fix mypy providers (#20190)``
`1a2a2498d6 <https://github.com/apache/airflow/commit/1a2a2498d68040dcc1a162b563f272ed8c49a540>`_  2021-12-14   ``Fix mypy spark hooks (#20290)``
`a50d2ac872 <https://github.com/apache/airflow/commit/a50d2ac872da7e27d4cb32a2eb12cb75545c4a60>`_  2021-12-02   ``Ensure Spark driver response is valid before setting UNKNOWN status (#19978)``
=================================================================================================  ===========  ================================================================================

2.0.2
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`79b30ff59c <https://github.com/apache/airflow/commit/79b30ff59c711883ae548ebc806a6cdd6f0689a5>`_  2021-11-24   ``fix bug of SparkSql Operator log  going to infinite loop. (#19449)``
`ae044884d1 <https://github.com/apache/airflow/commit/ae044884d1dacce8dbf47c618f543b58f9ff101f>`_  2021-11-03   ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`86a2a19ad2 <https://github.com/apache/airflow/commit/86a2a19ad2bdc87a9ad14bb7fde9313b2d7489bb>`_  2021-10-17   ``More f-strings (#18855)``
`42dc0767b8 <https://github.com/apache/airflow/commit/42dc0767b85352a57eb2255593913a94a73e570d>`_  2021-10-08   ``Remove unnecessary string concatenations in AirflowException messages (#18817)``
=================================================================================================  ===========  ==================================================================================

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
`91f4d80ff0 <https://github.com/apache/airflow/commit/91f4d80ff09093de49478214c5bd027e02c92a0e>`_  2021-07-23   ``Updating Apache example DAGs to use XComArgs (#16869)``
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
`5c86e3d509 <https://github.com/apache/airflow/commit/5c86e3d50970e61d0eabd0965ebdc7b5ecf3bf14>`_  2021-06-14   ``Make SparkSqlHook use Connection (#15794)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
=================================================================================================  ===========  =================================================================

1.0.3
.....

Latest change: 2021-05-01

=================================================================================================  ===========  =========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`5b2fe0e740 <https://github.com/apache/airflow/commit/5b2fe0e74013cd08d1f76f5c115f2c8f990ff9bc>`_  2021-04-27   ``Add Connection Documentation for Popular Providers (#15393)``
`4b031d39e1 <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`657384615f <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`9015beb316 <https://github.com/apache/airflow/commit/9015beb316a7614616c9d8c5108f5b54e1b47843>`_  2021-04-10   ``Pass environment variables to process with yarn kill command (#15304)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
=================================================================================================  ===========  =========================================================================

1.0.2
.....

Latest change: 2021-02-27

=================================================================================================  ===========  ===========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`f9c9e9c38f <https://github.com/apache/airflow/commit/f9c9e9c38f444a39987478f3d1a262db909de8c4>`_  2021-02-11   ``Use apache.spark provider without kubernetes (#14187)``
=================================================================================================  ===========  ===========================================================

1.0.1
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ========================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c89 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
=================================================================================================  ===========  ========================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ====================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`d305876bee <https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731>`_  2020-10-12   ``Remove redundant None provided as default to dict.get() (#11448)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d760265452 <https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2>`_  2020-08-25   ``PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)``
`d1bce91bb2 <https://github.com/apache/airflow/commit/d1bce91bb21d5a468fa6a0207156c28fe1ca6513>`_  2020-08-25   ``PyDocStyle: Enable D403: Capitalized first word of docstring (#10530)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`7c206a82a6 <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`3b3287d7ac <https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726>`_  2020-08-05   ``Enforce keyword only arguments on apache operators (#10170)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`1427e4acb4 <https://github.com/apache/airflow/commit/1427e4acb4a1dc5be28cfeef75c90032d515aab6>`_  2020-07-22   ``Update Spark submit operator for Spark 3 support (#8730)``
`4d74ac2111 <https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2>`_  2020-07-19   ``Increase typing for Apache and http provider package (#9729)``
`0873070e08 <https://github.com/apache/airflow/commit/0873070e08f7216b6949e7de4e2329175a764321>`_  2020-07-11   ``Mask other forms of password arguments in SparkSubmitOperator (#9615)``
`13a827d80f <https://github.com/apache/airflow/commit/13a827d80fef738e25f30ea20c095ad4dbd401f6>`_  2020-07-09   ``Ensure Kerberos token is valid in SparkSubmitOperator before running 'yarn kill' (#9044)``
`067806d598 <https://github.com/apache/airflow/commit/067806d5985301f21da78f0a81056dbec348e6ba>`_  2020-06-29   ``Add tests for spark_jdbc_script (#9491)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`40bf8f28f9 <https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee>`_  2020-06-18   ``Detect automatically the lack of reference to the guide in the operator descriptions (#9290)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`7506c73f17 <https://github.com/apache/airflow/commit/7506c73f1721151e9c50ef8bdb70d2136a16190b>`_  2020-05-10   ``Add default 'conf' parameter to Spark JDBC Hook (#8787)``
`487b5cc50c <https://github.com/apache/airflow/commit/487b5cc50c5b28a045cb12a1527a5453b0a6a7af>`_  2020-05-06   ``Add guide for Apache Spark operators (#8305)``
`87969a350d <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`be1451b0e1 <https://github.com/apache/airflow/commit/be1451b0e1b7e33f4621e24649f6a4fa87c34e01>`_  2020-04-02   ``[AIRFLOW-7026] Improve SparkSqlHook's error message (#7749)``
`4bde99f132 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`7e6372a681 <https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388>`_  2020-03-23   ``Add call to Super call in apache providers (#7820)``
`2327aa5a26 <https://github.com/apache/airflow/commit/2327aa5a263f25beeaf4ba79670f10f001daf0bf>`_  2020-03-12   ``[AIRFLOW-7025] Fix SparkSqlHook.run_query to handle its parameter properly (#7677)``
`024b4bf962 <https://github.com/apache/airflow/commit/024b4bf962bc30ecb70da9650e68b523a0dbcff8>`_  2020-03-10   ``[AIRFLOW-7024] Add the verbose parameter support to SparkSqlOperator (#7676)``
`b59042b5ab <https://github.com/apache/airflow/commit/b59042b5ab083c77ba08ba804df76b7c728815dc>`_  2020-02-28   ``[AIRFLOW-6949] Respect explicit 'spark.kubernetes.namespace' conf to SparkSubmitOperator (#7575)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`0481b9a957 <https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b>`_  2020-01-12   ``[AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)``
=================================================================================================  ===========  ====================================================================================================
