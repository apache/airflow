
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


Package apache-airflow-providers-cncf-kubernetes
------------------------------------------------------

`Kubernetes <https://kubernetes.io/>`__


This is detailed commit list of changes for versions provider package: ``cncf.kubernetes``.
For high-level changelog, see :doc:`package information including changelog <index>`.



6.1.0
.....

Latest change: 2023-04-21

=================================================================================================  ===========  ===========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================================================
`ebe2f2f626 <https://github.com/apache/airflow/commit/ebe2f2f626ffee4b9d0f038fe5b89c322125a49b>`_  2023-04-21   ``Remove skip_exit_code from KubernetesPodOperator (#30788)``
`afdc95435b <https://github.com/apache/airflow/commit/afdc95435b9814d06f5d517ea6950442d3e4019a>`_  2023-04-21   ``Add multiple exit code handling in skip logic for 'DockerOperator' and 'KubernetesPodOperator' (#30769)``
`99a3bf2318 <https://github.com/apache/airflow/commit/99a3bf23182374699f437cfd8ed3b74af3dafba7>`_  2023-04-19   ``Deprecate 'skip_exit_code' in 'DockerOperator' and 'KubernetesPodOperator' (#30733)``
`f511653e5a <https://github.com/apache/airflow/commit/f511653e5a06bdd87cf4f55e3a1e0986e09e36fc>`_  2023-04-15   ``Skip KubernetesPodOperator task when it returns a provided exit code (#29000)``
=================================================================================================  ===========  ===========================================================================================================

6.0.0
.....

Latest change: 2023-04-09

=================================================================================================  ===========  ================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================
`874ea9588e <https://github.com/apache/airflow/commit/874ea9588e3ce7869759440302e53bb6a730a11e>`_  2023-04-09   ``Prepare docs for ad hoc release of Providers (#30545)``
`85b9135722 <https://github.com/apache/airflow/commit/85b9135722c330dfe1a15e50f5f77f3d58109a52>`_  2023-04-08   ``Use default connection id for KubernetesPodOperator (#28848)``
`dc4dd9178c <https://github.com/apache/airflow/commit/dc4dd9178cfab46640c02561be63afd1da55fd52>`_  2023-04-05   ``Allow to set limits for XCOM container (#28125)``
`d23a3bbed8 <https://github.com/apache/airflow/commit/d23a3bbed89ae04369983f21455bf85ccc1ae1cb>`_  2023-04-04   ``Add mechanism to suspend providers (#30422)``
=================================================================================================  ===========  ================================================================

5.3.0
.....

Latest change: 2023-04-02

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`55dbf1ff1f <https://github.com/apache/airflow/commit/55dbf1ff1fb0b22714f695a66f6108b3249d1199>`_  2023-04-02   ``Prepare docs for April 2023 wave of Providers (#30378)``
`df49ad179b <https://github.com/apache/airflow/commit/df49ad179bddcdb098b3eccbf9bb6361cfbafc36>`_  2023-03-24   ``Ensure setup/teardown work on a previously decorated function (#30216)``
`b8ab594130 <https://github.com/apache/airflow/commit/b8ab594130a1525fcf30c31a917a7dfdaef9dccf>`_  2023-03-15   ``Remove "boilerplate" from all taskflow decorators (#30118)``
`9a4f674852 <https://github.com/apache/airflow/commit/9a4f6748521c9c3b66d96598036be08fd94ccf89>`_  2023-03-14   ``enhance spark_k8s_operator (#29977)``
`c3867781e0 <https://github.com/apache/airflow/commit/c3867781e09b7e0e0d19c0991865a2453194d9a8>`_  2023-03-08   ``adding trigger info to provider yaml (#29950)``
`1e81a98cc6 <https://github.com/apache/airflow/commit/1e81a98cc69344a35c50b00e2d25a6d48a9bded2>`_  2023-03-07   ``Fix KubernetesPodOperator xcom push when 'get_logs=False' (#29052)``
`971039454a <https://github.com/apache/airflow/commit/971039454a3684d0ea7261dfe91f34ac4b62af72>`_  2023-03-04   ``Align cncf provider file names with AIP-21 (#29905)``
`6d2face107 <https://github.com/apache/airflow/commit/6d2face107f24b7e7dce4b98ae3def1178e1fc4c>`_  2023-03-04   ``Fixed hanged KubernetesPodOperator (#28336)``
=================================================================================================  ===========  ==========================================================================

5.2.2
.....

Latest change: 2023-03-03

=================================================================================================  ===========  ===================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================================================================
`fcd3c0149f <https://github.com/apache/airflow/commit/fcd3c0149f17b364dfb94c0523d23e3145976bbe>`_  2023-03-03   ``Prepare docs for 03/2023 wave of Providers (#29878)``
`1e536eb43d <https://github.com/apache/airflow/commit/1e536eb43de4408612bf7bb7d9d2114470c6f43a>`_  2023-02-28   ``'KubernetesPodOperator._render_nested_template_fields' improved by changing the conditionals for a map (#29760)``
`dba390e323 <https://github.com/apache/airflow/commit/dba390e32330675e1b94442c8001ea980754c189>`_  2023-02-22   ``Fix and augment 'check-for-inclusive-language' CI check (#29549)``
=================================================================================================  ===========  ===================================================================================================================

5.2.1
.....

Latest change: 2023-02-18

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`470fdaea27 <https://github.com/apache/airflow/commit/470fdaea275660970777c0f72b8867b382eabc14>`_  2023-02-18   ``Prepare docs for 02 2023 midmonth wave of Providers (#29589)``
`9a5c3e0ac0 <https://github.com/apache/airflow/commit/9a5c3e0ac0b682d7f2c51727a56e06d68bc9f6be>`_  2023-02-18   ``Fix @task.kubernetes to receive input and send output (#28942)``
=================================================================================================  ===========  ==================================================================

5.2.0
.....

Latest change: 2023-02-08

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`ce6ae2457e <https://github.com/apache/airflow/commit/ce6ae2457ef3d9f44f0086b58026909170bbf22a>`_  2023-02-08   ``Prepare docs for Feb 2023 wave of Providers (#29379)``
`d26dc22391 <https://github.com/apache/airflow/commit/d26dc223915c50ff58252a709bb7b33f5417dfce>`_  2023-02-01   ``Patch only single label when marking KPO checked (#29279)``
`246d778e6b <https://github.com/apache/airflow/commit/246d778e6b8042850ef8510bd25c52b1198030f1>`_  2023-01-30   ``Add deferrable mode to ''KubernetesPodOperator'' (#29017)``
`70b84b51a5 <https://github.com/apache/airflow/commit/70b84b51a5802b72dc7a8fb9bf8133699adcc79c>`_  2023-01-23   ``Allow setting the name for the base container within K8s Pod Operator (#28808)``
=================================================================================================  ===========  ==================================================================================

5.1.1
.....

Latest change: 2023-01-14

=================================================================================================  ===========  ==================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================
`911b708ffd <https://github.com/apache/airflow/commit/911b708ffddd4e7cb6aaeac84048291891eb0f1f>`_  2023-01-14   ``Prepare docs for Jan 2023 mid-month wave of Providers (#28929)``
`ce858a5d71 <https://github.com/apache/airflow/commit/ce858a5d719fb1dff85ad7e4747f0777404d1f56>`_  2023-01-12   ``Switch to ruff for faster static checks (#28893)``
`ce677862be <https://github.com/apache/airflow/commit/ce677862be4a7de777208ba9ba9e62bcd0415393>`_  2023-01-07   ``Fix Incorrect 'await_container_completion' (#28771)``
=================================================================================================  ===========  ==================================================================

5.1.0
.....

Latest change: 2023-01-02

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`5246c009c5 <https://github.com/apache/airflow/commit/5246c009c557b4f6bdf1cd62bf9b89a2da63f630>`_  2023-01-02   ``Prepare docs for Jan 2023 wave of Providers (#28651)``
`c22fc000b6 <https://github.com/apache/airflow/commit/c22fc000b6c0075429b9d1e51c9ee3d384141ff3>`_  2022-12-30   ``Use labels instead of pod name for pod log read in k8s exec (#28546)``
`681835a67c <https://github.com/apache/airflow/commit/681835a67c89784944f41fce86099bcb2c3a0614>`_  2022-12-21   ``Add Flink on K8s Operator  (#28512)``
`38e40c6dc4 <https://github.com/apache/airflow/commit/38e40c6dc45b92b274a06eafd8790140a0c3c7b8>`_  2022-12-21   ``Remove outdated compat imports/code from providers (#28507)``
`401fc57e8b <https://github.com/apache/airflow/commit/401fc57e8ba1dddb041e0d777bb0277a09f227db>`_  2022-12-16   ``Restructure Docs  (#27235)``
`bdc3d2e647 <https://github.com/apache/airflow/commit/bdc3d2e6474f7f23f75683fd072b4a07ef5aaeaa>`_  2022-12-08   ``Keep pod name for k8s executor under 63 characters (#28237)``
`d93240696b <https://github.com/apache/airflow/commit/d93240696beeca7d28542d0fe0b53871b3d6612c>`_  2022-12-05   ``Allow longer pod names for k8s executor / KPO (#27736)``
`33c445d92f <https://github.com/apache/airflow/commit/33c445d92f1386ca0167356a9514cfd8a27e360e>`_  2022-12-03   ``Add volume-related nested template fields for KPO (#27719)``
`ebd7b67dcb <https://github.com/apache/airflow/commit/ebd7b67dcb9ac0864fbc5c1aefe5d7a4531df5fe>`_  2022-12-02   ``Patch "checked" when pod not successful (#27845)``
`25bdbc8e67 <https://github.com/apache/airflow/commit/25bdbc8e6768712bad6043618242eec9c6632618>`_  2022-11-26   ``Updated docs for RC3 wave of providers (#27937)``
`2e20e9f7eb <https://github.com/apache/airflow/commit/2e20e9f7ebf5f43bf27069f4c0063cdd72e6b2e2>`_  2022-11-24   ``Prepare for follow-up relase for November providers (#27774)``
=================================================================================================  ===========  ========================================================================

5.0.0
.....

Latest change: 2022-11-15

=================================================================================================  ===========  ============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================================
`12c3c39d1a <https://github.com/apache/airflow/commit/12c3c39d1a816c99c626fe4c650e88cf7b1cc1bc>`_  2022-11-15   ``pRepare docs for November 2022 wave of Providers (#27613)``
`52593b061c <https://github.com/apache/airflow/commit/52593b061c32d071243c46fe45784a78b57a04b6>`_  2022-11-11   ``Enable template rendering for env_vars field for the @task.kubernetes decorator (#27433)``
`47a2b9ee7f <https://github.com/apache/airflow/commit/47a2b9ee7f1ff2cc1cc1aa1c3d1b523c88ba29fb>`_  2022-11-09   ``Add container_resources as KubernetesPodOperator templatable (#27457)``
`aefadb8c5b <https://github.com/apache/airflow/commit/aefadb8c5b9272613d5806b054a1b46edf29d82e>`_  2022-11-08   ``Allow xcom sidecar container image to be configurable in KPO (#26766)``
`2d2f0daad6 <https://github.com/apache/airflow/commit/2d2f0daad66416d565e874e35b6a487a21e5f7b1>`_  2022-11-08   ``Fix KubernetesHook fail on an attribute absence (#25787)``
`eee3df4570 <https://github.com/apache/airflow/commit/eee3df457063df04d0fa2e57431786c6f223f700>`_  2022-11-07   ``Improve task_id to pod name conversion (#27524)``
`8c15b0a6d1 <https://github.com/apache/airflow/commit/8c15b0a6d1a846cc477618e326a50cd96f76380f>`_  2022-11-07   ``Use log.exception where more economical than log.error (#27517)``
`20ecefa416 <https://github.com/apache/airflow/commit/20ecefa416640bc9a3afc2c86848ca2e2436f6a4>`_  2022-11-05   ``KPO should use hook's get namespace method to get namespace (#27516)``
`701239abc3 <https://github.com/apache/airflow/commit/701239abc372cb235b1c313198ae2ec429be4f91>`_  2022-11-05   ``Remove deprecated backcompat objects for KPO (#27518)``
`9337aa92c0 <https://github.com/apache/airflow/commit/9337aa92c082db36e82eb314585591394fe8ff27>`_  2022-11-05   ``Remove support for node_selectors param in KPO (#27515)``
`3aadc44a13 <https://github.com/apache/airflow/commit/3aadc44a13d0d100778792691a0341818723c51c>`_  2022-11-03   ``Remove unused backcompat method in k8s hook (#27490)``
`0c26ec07be <https://github.com/apache/airflow/commit/0c26ec07be96ae250dd2052f3c3bf552221d0e03>`_  2022-10-28   ``Drop support for providing ''resource'' as dict in ''KubernetesPodOperator'' (#27197)``
`4797a0322e <https://github.com/apache/airflow/commit/4797a0322ed4b73bc34d3967376479a42d9ba190>`_  2022-10-28   ``Fix log message for kubernetes hooks (#26999)``
`9ab1a6a3e7 <https://github.com/apache/airflow/commit/9ab1a6a3e70b32a3cddddf0adede5d2f3f7e29ea>`_  2022-10-27   ``Update old style typing (#26872)``
`734995ff26 <https://github.com/apache/airflow/commit/734995ff26d97bcb63b0c8c3bfc1ab7f4bc4b010>`_  2022-10-26   ``Add deprecation warning re unset namespace in k8s hook (#27202)``
`78b8ea2f22 <https://github.com/apache/airflow/commit/78b8ea2f22239db3ef9976301234a66e50b47a94>`_  2022-10-24   ``Move min airflow version to 2.3.0 for all providers (#27196)``
`2a34dc9e84 <https://github.com/apache/airflow/commit/2a34dc9e8470285b0ed2db71109ef4265e29688b>`_  2022-10-23   ``Enable string normalization in python formatting - providers (#27205)``
`14a45872e2 <https://github.com/apache/airflow/commit/14a45872e24a367ffc29df393f68e57fe3a089c6>`_  2022-10-22   ``Remove extra__kubernetes__ prefix from k8s hook extras (#27021)``
`3ecb8dd025 <https://github.com/apache/airflow/commit/3ecb8dd0259abfce37513509e8f67b9ede72af21>`_  2022-10-22   ``Make namespace optional for KPO (#27116)``
`c9e57687b0 <https://github.com/apache/airflow/commit/c9e57687b03807a36fac1c2c03ccf8ebb2e802b9>`_  2022-10-21   ``Make pod name optional in KubernetesPodOperator (#27120)``
`2752f2add1 <https://github.com/apache/airflow/commit/2752f2add1746a1b9fa005860d65ac3496770200>`_  2022-10-12   ``Deprecate use of core get_kube_client in PodManager (#26848)``
`5c97e5be48 <https://github.com/apache/airflow/commit/5c97e5be484ff572070b0ad320c5936bc028be93>`_  2022-10-10   ``add container_name option for SparkKubernetesSensor (#26560)``
`53d68049d9 <https://github.com/apache/airflow/commit/53d68049d9bf4cec6b7d57545f15409dab0caed1>`_  2022-10-04   ``Don't consider airflow core conf for KPO (#26849)``
`f8db64c35c <https://github.com/apache/airflow/commit/f8db64c35c8589840591021a48901577cff39c07>`_  2022-09-28   ``Update docs for September Provider's release (#26731)``
=================================================================================================  ===========  ============================================================================================

4.4.0
.....

Latest change: 2022-09-22

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`1a07cbe423 <https://github.com/apache/airflow/commit/1a07cbe423dde2558c2a148a54bac1e902000e07>`_  2022-09-22   ``Prepare to release cncf.kubernetes provider (#26588)``
`e60a459d56 <https://github.com/apache/airflow/commit/e60a459d560e6f9caa83392a1901963c4bc7e15d>`_  2022-09-14   ``Avoid calculating all elements when one item is needed (#26377)``
`06acf40a43 <https://github.com/apache/airflow/commit/06acf40a4337759797f666d5bb27a5a393b74fed>`_  2022-09-13   ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
`4b26c8c541 <https://github.com/apache/airflow/commit/4b26c8c541a720044fa96475620fc70f3ac6ccab>`_  2022-09-09   ``feat(KubernetesPodOperator): Add support of container_security_context (#25530)``
`0eb0b543a9 <https://github.com/apache/airflow/commit/0eb0b543a9751f3d458beb2f03d4c6ff22fcd1c7>`_  2022-08-23   ``Add @task.kubernetes taskflow decorator (#25663)``
`db5543ef60 <https://github.com/apache/airflow/commit/db5543ef608bdd7aefdb5fefea150955d369ddf4>`_  2022-08-22   ``pretty print KubernetesPodOperator rendered template env_vars (#25850)``
`ccdd73ec50 <https://github.com/apache/airflow/commit/ccdd73ec50ab9fb9d18d1cce7a19a95fdedcf9b9>`_  2022-08-22   ``Wait for xcom sidecar container to start before sidecar exec (#25055)``
=================================================================================================  ===========  ====================================================================================

4.3.0
.....

Latest change: 2022-08-10

=================================================================================================  ===========  =================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================
`e5ac6c7cfb <https://github.com/apache/airflow/commit/e5ac6c7cfb189c33e3b247f7d5aec59fe5e89a00>`_  2022-08-10   ``Prepare docs for new providers release (August 2022) (#25618)``
`c8af0592c0 <https://github.com/apache/airflow/commit/c8af0592c08017ee48f69f608ad4a6529ee14292>`_  2022-07-26   ``Improve taskflow type hints with ParamSpec (#25173)``
`f05a06537b <https://github.com/apache/airflow/commit/f05a06537be4d12276862eae1960515c76aa11d1>`_  2022-07-16   ``Fix xcom_sidecar stuck problem (#24993)``
=================================================================================================  ===========  =================================================================

4.2.0
.....

Latest change: 2022-07-13

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`d2459a241b <https://github.com/apache/airflow/commit/d2459a241b54d596ebdb9d81637400279fff4f2d>`_  2022-07-13   ``Add documentation for July 2022 Provider's release (#25030)``
`ef79a0d1c4 <https://github.com/apache/airflow/commit/ef79a0d1c4c0a041d7ebf83b93cbb25aa3778a70>`_  2022-07-11   ``Only assert stuff for mypy when type checking (#24937)``
`e2fd41f7b1 <https://github.com/apache/airflow/commit/e2fd41f7b14adef2c3a88dde14d088b5ef93b460>`_  2022-07-04   ``Remove 'xcom_push' flag from providers (#24823)``
`9d307102b4 <https://github.com/apache/airflow/commit/9d307102b4a604034d9b1d7f293884821263575f>`_  2022-06-29   ``More typing and minor refactor for kubernetes (#24719)``
`0de31bd73a <https://github.com/apache/airflow/commit/0de31bd73a8f41dded2907f0dee59dfa6c1ed7a1>`_  2022-06-29   ``Move provider dependencies to inside provider folders (#24672)``
`45b11d4ed1 <https://github.com/apache/airflow/commit/45b11d4ed1412c00ebf32a03ab5ea3a06274f208>`_  2022-06-29   ``Use our yaml util in all providers (#24720)``
`510a6bab45 <https://github.com/apache/airflow/commit/510a6bab4595cce8bd5b1447db957309d70f35d9>`_  2022-06-28   ``Remove 'hook-class-names' from provider.yaml (#24702)``
`5326da4b83 <https://github.com/apache/airflow/commit/5326da4b83ed4405553e88d5d5464508256498d0>`_  2022-06-28   ``Add 'airflow_kpo_in_cluster' label to KPO pods (#24658)``
`45f4290712 <https://github.com/apache/airflow/commit/45f4290712f5f779e57034f81dbaab5d77d5de85>`_  2022-06-28   ``Rename 'resources' arg in Kub op to k8s_resources (#24673)``
`9c59831ee7 <https://github.com/apache/airflow/commit/9c59831ee78f14de96421c74986933c494407afa>`_  2022-06-21   ``Update providers to use functools compat for ''cached_property'' (#24582)``
`78ac48872b <https://github.com/apache/airflow/commit/78ac48872bd02d1c08c6e55525f0bb4d6e983d32>`_  2022-06-21   ``Use found pod for deletion in KubernetesPodOperator (#22092)``
`dba3e4ec51 <https://github.com/apache/airflow/commit/dba3e4ec51c03dc08449a3954fa3539388d0bc73>`_  2022-06-15   ``Revert "Fix await_container_completion condition (#23883)" (#24474)``
=================================================================================================  ===========  =============================================================================

4.1.0
.....

Latest change: 2022-06-09

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`dcdcf3a2b8 <https://github.com/apache/airflow/commit/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5>`_  2022-06-09   ``Update release notes for RC2 release of Providers for May 2022 (#24307)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`b1ad017cee <https://github.com/apache/airflow/commit/b1ad017cee66f5e042144cc7baa2d44b23b47c4f>`_  2022-06-07   ``pydocstyle D202 added (#24221)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`98b4e48fbc <https://github.com/apache/airflow/commit/98b4e48fbc1262f1381e7a4ca6cce31d96e6f5e9>`_  2022-06-06   ``Add param docs to KubernetesHook and KubernetesPodOperator (#23955) (#24054)``
`42abbf0d61 <https://github.com/apache/airflow/commit/42abbf0d61f94ec50026af0c0f95eb378e403042>`_  2022-06-06   ``Fix await_container_completion condition (#23883)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`7ad4e67c1a <https://github.com/apache/airflow/commit/7ad4e67c1ad504f6338c1f616fa4245685cf1abd>`_  2022-06-03   ``Migrate Cncf.Kubernetes example DAGs to new design #22441 (#24132)``
`60eb9e106f <https://github.com/apache/airflow/commit/60eb9e106f5915398eafd6aa339ec710c102dc09>`_  2022-05-31   ``Use KubernetesHook to create api client in KubernetesPodOperator (#20578)``
`e240132934 <https://github.com/apache/airflow/commit/e2401329345dcc5effa933b92ca969b8779755e4>`_  2022-05-27   ``[FEATURE] KPO use K8S hook (#22086)``
`6bbe015905 <https://github.com/apache/airflow/commit/6bbe015905bd2709e621455d9f71a78b374d1337>`_  2022-05-26   ``Use "remote" pod when patching KPO pod as "checked" (#23676)``
`ec6761a5c0 <https://github.com/apache/airflow/commit/ec6761a5c0d031221d53ce213c0e42813606c55d>`_  2022-05-23   ``Clean up f-strings in logging calls (#23597)``
`064c41afda <https://github.com/apache/airflow/commit/064c41afdadc4cc44ac6f879556387db2c050bf8>`_  2022-05-20   ``Don't use the root logger in KPO _suppress function (#23835)``
=================================================================================================  ===========  ==================================================================================

4.0.2
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ===========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`2eeb120bf4 <https://github.com/apache/airflow/commit/2eeb120bf4da8b42eab8685979d5452b1b9b79a1>`_  2022-05-12   ``Revert "Fix k8s pod.execute randomly stuck indefinitely by logs consumption (#23497) (#23618)" (#23656)``
`ee342b85b9 <https://github.com/apache/airflow/commit/ee342b85b97649e2e29fcf83f439279b68f1b4d4>`_  2022-05-11   ``Fix k8s pod.execute randomly stuck indefinitely by logs consumption (#23497) (#23618)``
`863b257642 <https://github.com/apache/airflow/commit/863b2576423e1a7933750b297a9b4518ae598db9>`_  2022-05-10   ``Fix: Exception when parsing log #20966 (#23301)``
`faae9faae3 <https://github.com/apache/airflow/commit/faae9faae396610086d5ea18d61c356a78a3d365>`_  2022-05-10   ``Fixed Kubernetes Operator large xcom content Defect  (#23490)``
`dbdcd0fd1d <https://github.com/apache/airflow/commit/dbdcd0fd1de102f5edf77b9ef2a485860b05001b>`_  2022-04-30   ``Clarify 'reattach_on_restart' behavior (#23377)``
`a914ec22c1 <https://github.com/apache/airflow/commit/a914ec22c1a807596786d3e785bda9dd263b2400>`_  2022-04-30   ``Add YANKED to yanked releases of the cncf.kubernetes (#23378)``
=================================================================================================  ===========  ===========================================================================================================

4.0.1
.....

Latest change: 2022-04-30

=================================================================================================  ===========  ====================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================================
`11bbe471cd <https://github.com/apache/airflow/commit/11bbe471cd138c39435b612dfda3226959d30257>`_  2022-04-30   ``Prepare documentation for cncf.kubernetes 4.0.1 release (#23374)``
`8e3abe4180 <https://github.com/apache/airflow/commit/8e3abe418021a3ba241ead1cad79a1c5b492c587>`_  2022-04-29   ``Fix ''KubernetesPodOperator'' with 'KubernetesExecutor'' on 2.3.0 (#23371)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`c7399c7190 <https://github.com/apache/airflow/commit/c7399c7190750ba705b8255b7a92de2554e6eef3>`_  2022-04-21   ``KubernetesHook should try incluster first when not otherwise configured (#23126)``
`70eede5dd6 <https://github.com/apache/airflow/commit/70eede5dd6924a4eb74b7600cce2c627e51a3b7e>`_  2022-04-20   ``Fix KPO to have hyphen instead of period (#22982)``
`c3d883a971 <https://github.com/apache/airflow/commit/c3d883a971a8e4e65ccc774891928daaaa0f4442>`_  2022-04-19   ``KubernetesPodOperator should patch "already checked" always (#22734)``
`d81703c577 <https://github.com/apache/airflow/commit/d81703c5778e13470fcd267578697158776b8318>`_  2022-04-14   ``Add k8s container's error message in airflow exception (#22871)``
`3c5bc73579 <https://github.com/apache/airflow/commit/3c5bc73579080248b0583d74152f57548aef53a2>`_  2022-04-12   ``Delete old Spark Application in SparkKubernetesOperator (#21092)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
`04082ac091 <https://github.com/apache/airflow/commit/04082ac091e92587b22c8323170ebe38bc68a19a>`_  2022-04-09   ``Cleanup dup code now that k8s provider requires 2.3.0+ (#22845)``
=================================================================================================  ===========  ====================================================================================

4.0.0
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`67e2723b73 <https://github.com/apache/airflow/commit/67e2723b7364ce1f73aee801522693d12d615310>`_  2022-03-29   ``Log traceback only on ''DEBUG'' for KPO logs read interruption (#22595)``
`6db30f3207 <https://github.com/apache/airflow/commit/6db30f32074e4ef50993628e810781cd704d4ddd>`_  2022-03-29   ``Update our approach for executor-bound dependencies (#22573)``
`0d64d66cea <https://github.com/apache/airflow/commit/0d64d66ceab1c5da09b56bae5da339e2f608a2c4>`_  2022-03-28   ``Stop crashing when empty logs are received from kubernetes client (#22566)``
`0a99be7411 <https://github.com/apache/airflow/commit/0a99be741108470608a81964007aaf0a83f66a9f>`_  2022-03-22   ``Optionally not follow logs in KPO pod_manager (#22412)``
=================================================================================================  ===========  ==============================================================================

3.1.2
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ==============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==============================================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`0f977daa3c <https://github.com/apache/airflow/commit/0f977daa3cb0b7e08a33eb86c60220ee53089ece>`_  2022-03-22   ``Fix "run_id" k8s and elasticsearch compatibility with Airflow 2.1 (#22385)``
`7bd165fbe2 <https://github.com/apache/airflow/commit/7bd165fbe2cbbfa8208803ec352c5d16ca2bd3ec>`_  2022-03-16   ``Remove RefreshConfiguration workaround for K8s token refreshing (#20759)``
=================================================================================================  ===========  ==============================================================================

3.1.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  ====================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ====================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
=================================================================================================  ===========  ====================================================================

3.1.0
.....

Latest change: 2022-03-07

=================================================================================================  ===========  ========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`8d8d072289 <https://github.com/apache/airflow/commit/8d8d07228907d32403056af7acb3b2da003a7542>`_  2022-03-03   ``Change KubePodOperator labels from exeuction_date to run_id (#21960)``
`6c37e47cf6 <https://github.com/apache/airflow/commit/6c37e47cf69083326c0ee535e5fb950c5dfa4c4a>`_  2022-03-02   ``Add map_index label to mapped KubernetesPodOperator (#21916)``
`351fa53432 <https://github.com/apache/airflow/commit/351fa53432d8f5fa9b26f7161ea4c8b468c7167e>`_  2022-03-01   ``Fix Kubernetes example with wrong operator casing (#21898)``
`a159ae828f <https://github.com/apache/airflow/commit/a159ae828f92eb2590f47762a52d10ea03b1a465>`_  2022-02-25   ``Remove types from KPO docstring (#21826)``
`0a3ff43d41 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
=================================================================================================  ===========  ========================================================================

3.0.2
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`4a73d8f3d1 <https://github.com/apache/airflow/commit/4a73d8f3d1f0c2cb52707901f9e9a34198573d5e>`_  2022-02-01   ``Add missed deprecations for cncf (#20031)``
`cb73053211 <https://github.com/apache/airflow/commit/cb73053211367e2c2dd76d5279cdc7dc7b190124>`_  2022-01-27   ``Add optional features in providers. (#21074)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`428bd5f228 <https://github.com/apache/airflow/commit/428bd5f228444ff4c76fd927f64aaa71c8074301>`_  2022-01-10   ``Make ''delete_pod'' change more prominent in K8s changelog (#20753)``
`5569b868a9 <https://github.com/apache/airflow/commit/5569b868a990c97dfc63a0e014a814ec1cc0f953>`_  2022-01-09   ``Fix MyPy Errors for providers: Tableau, CNCF, Apache (#20654)``
=================================================================================================  ===========  ==========================================================================

3.0.1
.....

Latest change: 2022-01-08

=================================================================================================  ===========  ===================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===================================================================
`da9210e89c <https://github.com/apache/airflow/commit/da9210e89c618611b1e450617277b738ce92ffd7>`_  2022-01-08   ``Add documentation for an ad-hoc release of 2 providers (#20765)``
`7222f68d37 <https://github.com/apache/airflow/commit/7222f68d374787f95acc7110a1165bd21e7722a1>`_  2022-01-04   ``Update Kubernetes library version (#18797)``
=================================================================================================  ===========  ===================================================================

3.0.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  =================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`746ee587da <https://github.com/apache/airflow/commit/746ee587da485acdc816129fe71df23e4f024e0b>`_  2021-12-31   ``Delete pods by default in KubernetesPodOperator (#20575)``
`d56ff765e1 <https://github.com/apache/airflow/commit/d56ff765e15f9fcd582bc6d1ec0e83b0fedf476a>`_  2021-12-30   ``Implement dry_run for KubernetesPodOperator (#20573)``
`e63417553f <https://github.com/apache/airflow/commit/e63417553ff86ed28f7740500f05179ed5486a7b>`_  2021-12-30   ``Move pod_mutation_hook call from PodManager to KubernetesPodOperator (#20596)``
`ca6c210b7d <https://github.com/apache/airflow/commit/ca6c210b7de7405b96b0a4b2a6257f0c6f80f5a2>`_  2021-12-30   ``Rename ''PodLauncher'' to ''PodManager'' (#20576)``
`e07e831946 <https://github.com/apache/airflow/commit/e07e8319465ea4598791b6b61b5fe7c46f159f86>`_  2021-12-30   ``Clarify docstring for ''build_pod_request_obj'' in K8s providers (#20574)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`f200bb1977 <https://github.com/apache/airflow/commit/f200bb1977655455f8acb79c9bd265df36f8ffce>`_  2021-12-29   ``Simplify ''KubernetesPodOperator'' (#19572)``
`4b8a1201ae <https://github.com/apache/airflow/commit/4b8a1201ae7635e5a751dd079a887831783bb6cb>`_  2021-12-16   ``Fix Volume/VolumeMount KPO DeprecationWarning (#19726)``
`2fb5e1d0ec <https://github.com/apache/airflow/commit/2fb5e1d0ec306839a3ff21d0bddbde1d022ee8c7>`_  2021-12-15   ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
`f9eab1c185 <https://github.com/apache/airflow/commit/f9eab1c1859dc2a9549e2ffd9af821d0d8d72a4f>`_  2021-12-06   ``Add params config, in_cluster, and cluster_context to KubernetesHook (#19695)``
=================================================================================================  ===========  =================================================================================

2.2.0
.....

Latest change: 2021-11-30

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`fe682ec3d3 <https://github.com/apache/airflow/commit/fe682ec3d376f0983410d64beb4f3529fb7b0f99>`_  2021-11-24   ``Fix duplicate changelog entries (#19759)``
`0d60d1af41 <https://github.com/apache/airflow/commit/0d60d1af41280d3ee70bf9b1582419ada200e5e3>`_  2021-11-23   ``Checking event.status.container_statuses before filtering (#19713)``
`1e57022953 <https://github.com/apache/airflow/commit/1e570229533c4bbf5d3c901d5db21261fa4b1137>`_  2021-11-19   ``Added namespace as a template field in the KPO. (#19718)``
`f7410dfba2 <https://github.com/apache/airflow/commit/f7410dfba268c6b6bbb7832a13c547a6d98afabe>`_  2021-11-19   ``Coalesce 'extra' params to None in KubernetesHook (#19694)``
`bf5f452413 <https://github.com/apache/airflow/commit/bf5f4524135113053d2c06e7807fe7c0eb3cb659>`_  2021-11-08   ``Change to correct type in KubernetesPodOperator (#19459)``
`854b70b904 <https://github.com/apache/airflow/commit/854b70b9048c4bbe97abde2252b3992892a4aab0>`_  2021-11-07   ``Decouple name randomization from name kwarg (#19398)``
=================================================================================================  ===========  ======================================================================

2.1.0
.....

Latest change: 2021-10-29

=================================================================================================  ===========  ======================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`0a6850647e <https://github.com/apache/airflow/commit/0a6850647e531b08f68118ff8ca20577a5b4062c>`_  2021-10-21   ``Update docstring to let users use 'node_selector' (#19057)``
`1571f80546 <https://github.com/apache/airflow/commit/1571f80546853688778c2a3ec5194e5c8be0edbd>`_  2021-10-14   ``Add pre-commit hook for common misspelling check in files (#18964)``
`b2045d6d1d <https://github.com/apache/airflow/commit/b2045d6d1d4d2424c02d7d9b40520440aa4e5070>`_  2021-10-13   ``Add more type hints to PodLauncher (#18928)``
`c8b86e69e4 <https://github.com/apache/airflow/commit/c8b86e69e49e330ab2f551358a6998d5800adb9a>`_  2021-10-12   ``Add more information to PodLauncher timeout error (#17953)``
=================================================================================================  ===========  ======================================================================

2.0.3
.....

Latest change: 2021-09-30

=================================================================================================  ===========  ======================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`ef037e7021 <https://github.com/apache/airflow/commit/ef037e702182e4370cb00c853c4fb0e246a0479c>`_  2021-09-29   ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
`7808be7ffb <https://github.com/apache/airflow/commit/7808be7ffb693de2e4ea73d0c1e6e2470cde9095>`_  2021-09-21   ``Make Kubernetes job description fit on one log line (#18377)``
`b8d06e812a <https://github.com/apache/airflow/commit/b8d06e812ac56af6b0d17830c63b705ace9d4959>`_  2021-09-08   ``Fix KubernetesPodOperator reattach when not deleting pods (#18070)``
`64d2f5488f <https://github.com/apache/airflow/commit/64d2f5488f6764194a2f4f8a01f961990c75b840>`_  2021-09-07   ``Do not fail KubernetesPodOperator tasks if log reading fails (#17649)``
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`42e13e1a5a <https://github.com/apache/airflow/commit/42e13e1a5a4c97a2085ddf96f7d93e7bf71949b8>`_  2021-08-30   ``Remove all deprecation warnings in providers (#17900)``
=================================================================================================  ===========  ======================================================================================

2.0.2
.....

Latest change: 2021-08-24

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`bb5602c652 <https://github.com/apache/airflow/commit/bb5602c652988d0b31ea5e0db8f03725a2f22d34>`_  2021-08-24   ``Prepare release for Kubernetes Provider (#17798)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`73d2b720e0 <https://github.com/apache/airflow/commit/73d2b720e0c79323a29741882a07eb8962256762>`_  2021-08-21   ``Fix using XCom with ''KubernetesPodOperator'' (#17760)``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`97428efc41 <https://github.com/apache/airflow/commit/97428efc41e5902183827fb9e4e56d067ca771df>`_  2021-08-02   ``Fix messed-up changelog in 3 providers (#17380)``
`b0b2591071 <https://github.com/apache/airflow/commit/b0b25910713dd39e0193bdcd95b2cfd9e3fed5e7>`_  2021-07-27   ``Fix static checks (#17256)``
`997f7d0beb <https://github.com/apache/airflow/commit/997f7d0beb1f0a954ba0127efeb3b250daf8b290>`_  2021-07-27   ``Update spark_kubernetes.py (#17237)``
=================================================================================================  ===========  ============================================================================

2.0.1
.....

Latest change: 2021-07-26

=================================================================================================  ===========  ==========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`d48b4e0caf <https://github.com/apache/airflow/commit/d48b4e0caf6218558378c7c3349b22adfc5c0785>`_  2021-07-21   ``Simplify 'default_args' in Kubernetes example DAGs (#16870)``
`3939e84161 <https://github.com/apache/airflow/commit/3939e841616d70ea2d930f55e6a5f73a2a99be07>`_  2021-07-20   ``Enable using custom pod launcher in Kubernetes Pod Operator (#16945)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`b2c66e45b7 <https://github.com/apache/airflow/commit/b2c66e45b7c27d187491ec6a1dd5cc92ac7a1e32>`_  2021-07-11   ``BugFix: Using 'json' string in template_field causes issue with K8s Operators (#16930)``
`9d6ae609b6 <https://github.com/apache/airflow/commit/9d6ae609b60449bd274c2f96e72486d73ad2b8f9>`_  2021-06-28   ``Updating task dependencies (#16624)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
=================================================================================================  ===========  ==========================================================================================

2.0.0
.....

Latest change: 2021-06-18

=================================================================================================  ===========  ===============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================================
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`4c9735ff9b <https://github.com/apache/airflow/commit/4c9735ff9b0201758564fcd64166abde318ec8a7>`_  2021-06-17   ``Fix unsuccessful KubernetesPod final_state call when 'is_delete_operator_pod=True' (#15490)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`4752fb3eb8 <https://github.com/apache/airflow/commit/4752fb3eb8ac8827e6af6022fbcf751829ecb17a>`_  2021-06-14   ``Fix issue with parsing error logs in the KPO (#15638)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`2f16757e1a <https://github.com/apache/airflow/commit/2f16757e1a11ef42ac2b1a62622a5d34f8a1e996>`_  2021-06-03   ``Bug Pod Template File Values Ignored (#16095)``
`476d0f6e3d <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
`85b2ccb0c5 <https://github.com/apache/airflow/commit/85b2ccb0c5e03495c58e7c4fb0513ceb4419a103>`_  2021-05-20   ``Add 'KubernetesPodOperat' 'pod-template-file' jinja template support (#15942)``
`733bec9a04 <https://github.com/apache/airflow/commit/733bec9a04ab718a0f6289d93f4e2e4ea3e03d54>`_  2021-05-20   ``Bug Fix Pod-Template Affinity Ignored due to empty Affinity K8S Object (#15787)``
`37d549bde7 <https://github.com/apache/airflow/commit/37d549bde79cd560d24748ebe7f94730115c0e88>`_  2021-05-14   ``Save pod name to xcom for KubernetesPodOperator (#15755)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
=================================================================================================  ===========  ===============================================================================================

1.2.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  ===========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`5b2fe0e740 <https://github.com/apache/airflow/commit/5b2fe0e74013cd08d1f76f5c115f2c8f990ff9bc>`_  2021-04-27   ``Add Connection Documentation for Popular Providers (#15393)``
`53fc1a9679 <https://github.com/apache/airflow/commit/53fc1a96797fde66cd68345a29a111ae86c1a35a>`_  2021-04-26   ``Change KPO node_selectors warning to proper deprecationwarning (#15507)``
`d3cc67aa7a <https://github.com/apache/airflow/commit/d3cc67aa7a7213db4325e77ca0246548bf1c0184>`_  2021-04-24   ``Fix timeout when using XCom with KubernetesPodOperator (#15388)``
`be421a6b07 <https://github.com/apache/airflow/commit/be421a6b07c2ae9167150b77dc1185a94812b358>`_  2021-04-23   ``Fix labels on the pod created by ''KubernetsPodOperator'' (#15492)``
`44480d3673 <https://github.com/apache/airflow/commit/44480d3673e8349fe784c10d38e4915f08b82b94>`_  2021-04-14   ``Require 'name' with KubernetesPodOperator (#15373)``
`b4770725a3 <https://github.com/apache/airflow/commit/b4770725a3aa03bd50a0a8c8e01db667bff93862>`_  2021-04-12   ``Add links to new modules for deprecated modules (#15316)``
=================================================================================================  ===========  ===========================================================================

1.1.0
.....

Latest change: 2021-04-07

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`1806670383 <https://github.com/apache/airflow/commit/18066703832319968ee3d6122907746fdfda5d4c>`_  2021-04-07   ``Retry pod launching on 409 ApiExceptions (#15137)``
`042be2e4e0 <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`6d7a70b88e <https://github.com/apache/airflow/commit/6d7a70b88e8b1d1edc04c6c50bde02c4d407e15a>`_  2021-04-05   ``Separate Kubernetes pod_launcher from core airflow (#15165)``
`00453dc4a2 <https://github.com/apache/airflow/commit/00453dc4a2d41da6c46e73cd66cac88e7556de71>`_  2021-03-20   ``Add ability to specify api group and version for Spark operators (#14898)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`e7bb17aeb8 <https://github.com/apache/airflow/commit/e7bb17aeb83b2218620c5320241b0c9f902d74ff>`_  2021-03-06   ``Use built-in 'cached_property' on Python 3.8 where possible (#14606)``
`7daebefd15 <https://github.com/apache/airflow/commit/7daebefd15355b3f1331c6c58f66f3f88d38a10a>`_  2021-03-05   ``Use libyaml C library when available. (#14577)``
=================================================================================================  ===========  =============================================================================

1.0.2
.....

Latest change: 2021-02-27

=================================================================================================  ===========  ============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`809b4f9b18 <https://github.com/apache/airflow/commit/809b4f9b18c7040682e17879248d714f2664273d>`_  2021-02-23   ``Unique pod name (#14186)``
`649335c043 <https://github.com/apache/airflow/commit/649335c043a9312ef272fa77f2bb830d52cde056>`_  2021-02-07   ``Template k8s.V1EnvVar without adding custom attributes to dict. (#14123)``
`d4c4db8a18 <https://github.com/apache/airflow/commit/d4c4db8a1833d07b1c03e4c062acea49c79bf5d6>`_  2021-02-05   ``Allow users of the KPO to template environment variables (#14083)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  ============================================================================

1.0.1
.....

Latest change: 2021-02-04

=================================================================================================  ===========  ==========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`1b9e3d1c28 <https://github.com/apache/airflow/commit/1b9e3d1c285cb381f2f964c0c923711cd5e1e3d0>`_  2021-01-22   ``Revert "Fix error with quick-failing tasks in KubernetesPodOperator (#13621)" (#13835)``
`94d3ed61d6 <https://github.com/apache/airflow/commit/94d3ed61d60b134d649a4e9785b2d9c2a88cff05>`_  2021-01-21   ``Fix error with quick-failing tasks in KubernetesPodOperator (#13621)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`7a560ab6de <https://github.com/apache/airflow/commit/7a560ab6de7243e736b66599842b241ae60d1cda>`_  2020-12-24   ``Pass image_pull_policy in KubernetesPodOperator correctly (#13289)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
=================================================================================================  ===========  ==========================================================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`2037303eef <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`de3b1e687b <https://github.com/apache/airflow/commit/de3b1e687b26c524c6909b7b4dfbb60d25019751>`_  2020-11-28   ``Move connection guides to provider documentation packages (#12653)``
`c02a3f59e4 <https://github.com/apache/airflow/commit/c02a3f59e45d3cdd0e4c1c3bda2c62b951bcbea3>`_  2020-11-23   ``Spark-on-k8s sensor logs - properly pass defined namespace to pod log call (#11199)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`9e089ab895 <https://github.com/apache/airflow/commit/9e089ab89567b0a52b232f22ed3e708a05137924>`_  2020-11-19   ``Fix Kube tests (#12479)``
`d32fe78c0d <https://github.com/apache/airflow/commit/d32fe78c0d9d14f016df70a462dc3972f28abe9d>`_  2020-11-18   ``Update readmes for cncf.kube provider fixes (#12457)``
`d84a52dc8f <https://github.com/apache/airflow/commit/d84a52dc8fc597d89c5bb4941df67f5f35b70a29>`_  2020-11-18   ``Fix broken example_kubernetes DAG (#12455)``
`7c8b71d201 <https://github.com/apache/airflow/commit/7c8b71d2012d56888f21b24c4844a6838dc3e4b1>`_  2020-11-18   ``Fix backwards compatibility further (#12451)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f121 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`763b40d223 <https://github.com/apache/airflow/commit/763b40d223e5e5512494a97f8335e16960e6adc3>`_  2020-11-18   ``Raise correct Warning in kubernetes/backcompat/volume_mount.py (#12432)``
`bc4bb30588 <https://github.com/apache/airflow/commit/bc4bb30588607b10b069ab63ddf2ba7b7ee673ed>`_  2020-11-18   ``Fix docstrings for Kubernetes Backcompat module (#12422)``
`cab86d80d4 <https://github.com/apache/airflow/commit/cab86d80d48227849906319917126f6d558b2e00>`_  2020-11-17   ``Make K8sPodOperator backwards compatible (#12384)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`221f809c1b <https://github.com/apache/airflow/commit/221f809c1b4e4b78d5a437d012aa7daffd8410a4>`_  2020-11-14   ``Fix full_pod_spec for k8spodoperator (#12354)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`3f59e75cdf <https://github.com/apache/airflow/commit/3f59e75cdf4a95829ac60b151135e03267e63a12>`_  2020-11-09   ``KubernetesPodOperator: use randomized name to get the failure status (#12171)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`7825be50d8 <https://github.com/apache/airflow/commit/7825be50d80d04da0db8fcee55df5e1339864c88>`_  2020-11-05   ``Randomize pod name (#12117)``
`91a64db505 <https://github.com/apache/airflow/commit/91a64db505e50712cd53928b4f2b84aece3cc1c0>`_  2020-11-04   ``Format all files (without excepions) by black (#12091)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`53e6062105 <https://github.com/apache/airflow/commit/53e6062105be0ae1761a354e2055eb0779d12e73>`_  2020-10-21   ``Enforce strict rules for yamllint (#11709)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`eee4e30f2c <https://github.com/apache/airflow/commit/eee4e30f2caf02e16088ff5d1af1ea380a73e982>`_  2020-10-15   ``Add better debug logging to K8sexec and K8sPodOp (#11502)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`8640fb6c10 <https://github.com/apache/airflow/commit/8640fb6c100a2c6aa231798559ba194331576975>`_  2020-10-09   ``fix tests (#11368)``
`298052fcee <https://github.com/apache/airflow/commit/298052fcee9d30b1f60b8dc1c9006398cd16645e>`_  2020-10-10   ``[airflow/providers/cncf/kubernetes] correct hook methods name (#11008)``
`49aad025b5 <https://github.com/apache/airflow/commit/49aad025b53211a5815b10aa35f7d7b489cb5316>`_  2020-10-09   ``Users can specify sub-secrets and paths k8spodop (#11369)``
`b93b6c5be3 <https://github.com/apache/airflow/commit/b93b6c5be3ab60960f650d0d4ee6c91271ac7909>`_  2020-10-05   ``Allow labels in KubernetesPodOperator to be templated (#10796)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`a888198c27 <https://github.com/apache/airflow/commit/a888198c27bcdbc4538c02360c308ffcaca182fa>`_  2020-09-27   ``Allow overrides for pod_template_file (#11162)``
`0161b5ea2b <https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1>`_  2020-09-26   ``Increasing type coverage for multiple provider (#11159)``
`e3f96ce7a8 <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`b61225a885 <https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da>`_  2020-09-21   ``Add D204 pydocstyle check (#11031)``
`cba51d49ee <https://github.com/apache/airflow/commit/cba51d49eea6a0563044191c8111978836d697ef>`_  2020-09-17   ``Simplify the K8sExecutor and K8sPodOperator (#10393)``
`1294e15d44 <https://github.com/apache/airflow/commit/1294e15d44c08498e7f1022fdd6f0bc5e50e533f>`_  2020-09-16   ``KubernetesPodOperator template fix (#10963)``
`5d6d5a2f7d <https://github.com/apache/airflow/commit/5d6d5a2f7d330c83297e1dc35728a0ba803aa866>`_  2020-09-14   ``Allow to specify path to kubeconfig in KubernetesHook (#10453)``
`7edfac957b <https://github.com/apache/airflow/commit/7edfac957bc17c9abcdcfe8d524772bd2783ac5a>`_  2020-09-09   ``Add connection caching to KubernetesHook (#10447)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`90c1505686 <https://github.com/apache/airflow/commit/90c1505686b063332dba87c0c948a8b29d8fd1d4>`_  2020-09-04   ``Make grace_period_seconds option on K8sPodOperator (#10727)``
`338b412c04 <https://github.com/apache/airflow/commit/338b412c04abc3fef8126f9724b448d1a9fd0bbc>`_  2020-09-02   ``Add on_kill support for the KubernetesPodOperator (#10666)``
`596bc13379 <https://github.com/apache/airflow/commit/596bc1337988f9377571295ddb748ef8703c19c0>`_  2020-08-31   ``Adds 'cncf.kubernetes' package back to backport provider packages. (#10659)``
`1e5aa4465c <https://github.com/apache/airflow/commit/1e5aa4465c5ef8f05745bda64da62fe542f2fe28>`_  2020-08-26   ``Spark-on-K8S sensor - add driver logs (#10023)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`2f2d8dbfaf <https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148>`_  2020-08-25   ``Remove all "noinspection" comments native to IntelliJ (#10525)``
`7c206a82a6 <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`8cd2be9e16 <https://github.com/apache/airflow/commit/8cd2be9e161635480581a0dc723b69ed24166f8d>`_  2020-08-11   ``Fix KubernetesPodOperator reattachment (#10230)``
`cdec301254 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`f1fd3e2c45 <https://github.com/apache/airflow/commit/f1fd3e2c453ddce3e87ce63787598fea0707ffcf>`_  2020-07-31   ``Fix typo on reattach property of kubernetespodoperator (#10056)``
`03c4351744 <https://github.com/apache/airflow/commit/03c43517445019081c55b4ac5fad3b0debdee336>`_  2020-07-31   ``Allow 'image' in 'KubernetesPodOperator' to be templated (#10068)``
`88c1603060 <https://github.com/apache/airflow/commit/88c1603060fd484d4145bc253c0dc0e6797e13dd>`_  2020-07-31   ``Improve docstring note about GKEStartPodOperator on KubernetesPodOperator (#10049)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`c2db0dfeb1 <https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed>`_  2020-07-22   ``More strict rules in mypy (#9705) (#9906)``
`719ae2bf62 <https://github.com/apache/airflow/commit/719ae2bf6227894c3e926f717eb4dc669549d615>`_  2020-07-22   ``Dump Pod as YAML in logs for KubernetesPodOperator (#9895)``
`840799d559 <https://github.com/apache/airflow/commit/840799d5597f0d005e1deec154f6c95bad6dce61>`_  2020-07-20   ``Improve KubernetesPodOperator guide (#9079)``
`44d4ae809c <https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3>`_  2020-07-06   ``Upgrade to latest pre-commit checks (#9686)``
`8bd15ef634 <https://github.com/apache/airflow/commit/8bd15ef634cca40f3cf6ca3442262f3e05144512>`_  2020-07-01   ``Switches to Helm Chart for Kubernetes tests (#9468)``
`40bf8f28f9 <https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee>`_  2020-06-18   ``Detect automatically the lack of reference to the guide in the operator descriptions (#9290)``
`1d36b0303b <https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea>`_  2020-05-23   ``Fix references in docs (#8984)``
`e742ef7c70 <https://github.com/apache/airflow/commit/e742ef7c704c18bf69b7a7235adb7f75e742f902>`_  2020-05-23   ``Fix typo in test_project_structure (#8978)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`8985df0bfc <https://github.com/apache/airflow/commit/8985df0bfcb5f2b2cd69a21b9814021f9f8ce953>`_  2020-05-16   ``Monitor pods by labels instead of names (#6377)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`f82ad452b0 <https://github.com/apache/airflow/commit/f82ad452b0f4ebd1428bc9669641a632dc87bb8c>`_  2020-05-15   ``Fix KubernetesPodOperator pod name length validation (#8829)``
`1ccafc617c <https://github.com/apache/airflow/commit/1ccafc617c4cb9622e3460ad7c190f3ee67c3b32>`_  2020-04-02   ``Add spark_kubernetes system test (#7875)``
`cd546b664f <https://github.com/apache/airflow/commit/cd546b664fa35a2bf85acd77af578c909a327d92>`_  2020-03-23   ``Add missing call to Super class in 'cncf' & 'docker' providers (#7825)``
`6c39a3bf97 <https://github.com/apache/airflow/commit/6c39a3bf97414ba2438669894db65c36ccbeb61a>`_  2020-03-10   ``[AIRFLOW-6542] Add spark-on-k8s operator/hook/sensor (#7163)``
`42eef38217 <https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e>`_  2020-03-07   ``[AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)``
`3320e432a1 <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`0ec2774120 <https://github.com/apache/airflow/commit/0ec2774120d43fa667a371b384e6006e1d1c7821>`_  2020-02-24   ``[AIRFLOW-5629] Implement Kubernetes priorityClassName in KubernetesPodOperator (#7395)``
`9cbd7de6d1 <https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b>`_  2020-02-18   ``[AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)``
`967930c0cb <https://github.com/apache/airflow/commit/967930c0cb6e2293f2a49e5c9add5aa1917f3527>`_  2020-02-11   ``[AIRFLOW-5413] Allow K8S worker pod to be configured from JSON/YAML file (#6230)``
`96f834389e <https://github.com/apache/airflow/commit/96f834389e03884025534fabd862155061f53fd0>`_  2020-02-03   ``[AIRFLOW-6678] Pull event logs from Kubernetes (#7292)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`cf141506a2 <https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721>`_  2020-02-02   ``[AIRFLOW-6708] Set unique logger names (#7330)``
`373c6aa4a2 <https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b>`_  2020-01-30   ``[AIRFLOW-6682] Move GCP classes to providers package (#7295)``
`83c037873f <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`059eda05f8 <https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf>`_  2020-01-21   ``[AIRFLOW-6610] Move software classes to providers package (#7231)``
=================================================================================================  ===========  ================================================================================================
