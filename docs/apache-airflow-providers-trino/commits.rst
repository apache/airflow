
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


Package apache-airflow-providers-trino
------------------------------------------------------

`Trino <https://trino.io/>`__


This is detailed commit list of changes for versions provider package: ``trino``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.0.1
.....

Latest change: 2022-08-27

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`a74d934991 <https://github.com/apache/airflow/commit/a74d9349919b340638f0db01bc3abb86f71c6093>`_  2022-08-27   ``Fix placeholders in 'TrinoHook', 'PrestoHook', 'SqliteHook' (#25939)``
`ca9229b6fe <https://github.com/apache/airflow/commit/ca9229b6fe7eda198c7ce32da13afb97ab9f3e28>`_  2022-08-18   ``Add common-sql lower bound for common-sql (#25789)``
=================================================================================================  ===========  ========================================================================

4.0.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`5d4abbd58c <https://github.com/apache/airflow/commit/5d4abbd58c33e7dfa8505e307d43420459d3df55>`_  2022-07-27   ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``
`df00436569 <https://github.com/apache/airflow/commit/df00436569bb6fb79ce8c0b7ca71dddf02b854ef>`_  2022-07-22   ``Unify DbApiHook.run() method with the methods which override it (#23971)``
=================================================================================================  ===========  ============================================================================

3.1.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`46bbfdade0 <https://github.com/apache/airflow/commit/46bbfdade0638cb8a5d187e47034b84e68ddf762>`_  2022-07-07   ``Move all SQL classes to common-sql provider (#24836)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`122d2f69bb <https://github.com/apache/airflow/commit/122d2f69bbf39c445ddcaa5a9c09a9ea86434a55>`_  2022-06-21   ``Add test_connection method to Trino hook (#24583)``
`4f4f37c855 <https://github.com/apache/airflow/commit/4f4f37c855d58f99c9b999fa0f19e0c0505274f4>`_  2022-06-20   ``Add 'on_kill()' to kill Trino query if the task is killed (#24559)``
`865406cbab <https://github.com/apache/airflow/commit/865406cbab4defd35c95afbf0a8d5987ff7788b1>`_  2022-06-20   ``Add TrinoOperator (#24415)``
=================================================================================================  ===========  ======================================================================

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
`7489962e75 <https://github.com/apache/airflow/commit/7489962e75a23071620a30c1e070fb7c9e107179>`_  2022-06-02   ``AIP-47 | Migrate Trino example DAGs to new design (#24118)``
=================================================================================================  ===========  ==================================================================================

2.3.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`ccb5ce934c <https://github.com/apache/airflow/commit/ccb5ce934cd521dc3af74b83623ca0843211be62>`_  2022-05-06   ``TrinoHook add authentication via JWT token and Impersonation  (#23116)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`5164cdbe98 <https://github.com/apache/airflow/commit/5164cdbe98ad63754d969b4b300a7a0167565e33>`_  2022-04-19   ``Make presto and trino compatible with airflow 2.1 (#23061)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
=================================================================================================  ===========  ===========================================================================

2.2.0
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ======================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`0c30564992 <https://github.com/apache/airflow/commit/0c30564992a9250e294106e809123f0d5b1c2b78>`_  2022-03-27   ``Pass X-Trino-Client-Info in trino hook (#22535)``
=================================================================================================  ===========  ======================================================

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
`942f8fd657 <https://github.com/apache/airflow/commit/942f8fd65799f07a01f64c855c0587ae044e55ec>`_  2022-02-27   ``Add GCSToTrinoOperator (#21704)``
`2807193594 <https://github.com/apache/airflow/commit/2807193594ed4e1f3acbe8da7dd372fe1c2fff94>`_  2022-02-22   ``Replaced hql references to sql in TrinoHook and PrestoHook (#21630)``
`1884f2227d <https://github.com/apache/airflow/commit/1884f2227d1e41d7bb37246ece4da5d871036c1f>`_  2022-02-15   ``Pass Trino hook params to DbApiHook (#21479)``
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`dad2f8103b <https://github.com/apache/airflow/commit/dad2f8103be954afaedf15e9d098ee417b0d5d02>`_  2021-12-15   ``Fix mypy providers (#20190)``
=================================================================================================  ===========  ==========================================================================

2.0.2
.....

Latest change: 2021-10-29

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`6bc0f87755 <https://github.com/apache/airflow/commit/6bc0f87755e3f9b3d736d7c1232b7cd93001ad06>`_  2021-10-07   ``Properly handle verify parameter in TrinoHook (#18791)``
=================================================================================================  ===========  =================================================================

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
=================================================================================================  ===========  =================================================================

1.0.0
.....

Latest change: 2021-04-06

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`eae22cec9c <https://github.com/apache/airflow/commit/eae22cec9c87e8dad4d6e8599e45af1bdd452062>`_  2021-04-06   ``Adds 'Trino' provider (with lower memory footprint for tests) (#15187)``
=================================================================================================  ===========  ==========================================================================
