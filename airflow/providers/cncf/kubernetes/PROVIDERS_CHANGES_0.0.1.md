

### Release 0.0.1

| Commit                                                                                         | Committed   | Subject                                                                                      |
|:-----------------------------------------------------------------------------------------------|:------------|:---------------------------------------------------------------------------------------------|
| [8640fb6c1](https://github.com/apache/airflow/commit/8640fb6c100a2c6aa231798559ba194331576975) | 2020-10-09  | fix tests (#11368)                                                                           |
| [298052fce](https://github.com/apache/airflow/commit/298052fcee9d30b1f60b8dc1c9006398cd16645e) | 2020-10-10  | [airflow/providers/cncf/kubernetes] correct hook methods name (#11008)                       |
| [49aad025b](https://github.com/apache/airflow/commit/49aad025b53211a5815b10aa35f7d7b489cb5316) | 2020-10-09  | Users can specify sub-secrets and paths k8spodop (#11369)                                    |
| [b93b6c5be](https://github.com/apache/airflow/commit/b93b6c5be3ab60960f650d0d4ee6c91271ac7909) | 2020-10-05  | Allow labels in KubernetesPodOperator to be templated (#10796)                               |
| [0a0e1af80](https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa) | 2020-10-03  | Fix Broken Markdown links in Providers README TOC (#11249)                                   |
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                                         |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                                 |
| [a888198c2](https://github.com/apache/airflow/commit/a888198c27bcdbc4538c02360c308ffcaca182fa) | 2020-09-27  | Allow overrides for pod_template_file (#11162)                                               |
| [0161b5ea2](https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1) | 2020-09-26  | Increasing type coverage for multiple provider (#11159)                                      |
| [e3f96ce7a](https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57) | 2020-09-24  | Fix incorrect Usage of Optional[bool] (#11138)                                               |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                                           |
| [b61225a88](https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da) | 2020-09-21  | Add D204 pydocstyle check (#11031)                                                           |
| [cba51d49e](https://github.com/apache/airflow/commit/cba51d49eea6a0563044191c8111978836d697ef) | 2020-09-17  | Simplify the K8sExecutor and K8sPodOperator (#10393)                                         |
| [1294e15d4](https://github.com/apache/airflow/commit/1294e15d44c08498e7f1022fdd6f0bc5e50e533f) | 2020-09-16  | KubernetesPodOperator template fix (#10963)                                                  |
| [5d6d5a2f7](https://github.com/apache/airflow/commit/5d6d5a2f7d330c83297e1dc35728a0ba803aa866) | 2020-09-14  | Allow to specify path to kubeconfig in KubernetesHook (#10453)                               |
| [7edfac957](https://github.com/apache/airflow/commit/7edfac957bc17c9abcdcfe8d524772bd2783ac5a) | 2020-09-09  | Add connection caching to KubernetesHook (#10447)                                            |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                                             |
| [90c150568](https://github.com/apache/airflow/commit/90c1505686b063332dba87c0c948a8b29d8fd1d4) | 2020-09-04  | Make grace_period_seconds option on K8sPodOperator (#10727)                                  |
| [338b412c0](https://github.com/apache/airflow/commit/338b412c04abc3fef8126f9724b448d1a9fd0bbc) | 2020-09-02  | Add on_kill support for the KubernetesPodOperator (#10666)                                   |
| [596bc1337](https://github.com/apache/airflow/commit/596bc1337988f9377571295ddb748ef8703c19c0) | 2020-08-31  | Adds &#39;cncf.kubernetes&#39; package back to backport provider packages. (#10659)                  |
| [1e5aa4465](https://github.com/apache/airflow/commit/1e5aa4465c5ef8f05745bda64da62fe542f2fe28) | 2020-08-26  | Spark-on-K8S sensor - add driver logs (#10023)                                               |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                                  |
| [2f2d8dbfa](https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148) | 2020-08-25  | Remove all &#34;noinspection&#34; comments native to IntelliJ (#10525)                               |
| [7c206a82a](https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054) | 2020-08-22  | Replace assigment with Augmented assignment (#10468)                                         |
| [8cd2be9e1](https://github.com/apache/airflow/commit/8cd2be9e161635480581a0dc723b69ed24166f8d) | 2020-08-11  | Fix KubernetesPodOperator reattachment (#10230)                                              |
| [cdec30125](https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a) | 2020-08-07  | Add correct signature to all operators and sensors (#10205)                                  |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)                         |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)                         |
| [f1fd3e2c4](https://github.com/apache/airflow/commit/f1fd3e2c453ddce3e87ce63787598fea0707ffcf) | 2020-07-31  | Fix typo on reattach property of kubernetespodoperator (#10056)                              |
| [03c435174](https://github.com/apache/airflow/commit/03c43517445019081c55b4ac5fad3b0debdee336) | 2020-07-31  | Allow `image` in `KubernetesPodOperator` to be templated (#10068)                            |
| [88c160306](https://github.com/apache/airflow/commit/88c1603060fd484d4145bc253c0dc0e6797e13dd) | 2020-07-31  | Improve docstring note about GKEStartPodOperator on KubernetesPodOperator (#10049)           |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)                            |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                                  |
| [c2db0dfeb](https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed) | 2020-07-22  | More strict rules in mypy (#9705) (#9906)                                                    |
| [719ae2bf6](https://github.com/apache/airflow/commit/719ae2bf6227894c3e926f717eb4dc669549d615) | 2020-07-22  | Dump Pod as YAML in logs for KubernetesPodOperator (#9895)                                   |
| [840799d55](https://github.com/apache/airflow/commit/840799d5597f0d005e1deec154f6c95bad6dce61) | 2020-07-20  | Improve KubernetesPodOperator guide (#9079)                                                  |
| [44d4ae809](https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3) | 2020-07-06  | Upgrade to latest pre-commit checks (#9686)                                                  |
| [8bd15ef63](https://github.com/apache/airflow/commit/8bd15ef634cca40f3cf6ca3442262f3e05144512) | 2020-07-01  | Switches to Helm Chart for Kubernetes tests (#9468)                                          |
| [40bf8f28f](https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee) | 2020-06-18  | Detect automatically the lack of reference to the guide in the operator descriptions (#9290) |
| [1d36b0303](https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea) | 2020-05-23  | Fix references in docs (#8984)                                                               |
| [e742ef7c7](https://github.com/apache/airflow/commit/e742ef7c704c18bf69b7a7235adb7f75e742f902) | 2020-05-23  | Fix typo in test_project_structure (#8978)                                                   |
| [375d1ca22](https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f) | 2020-05-19  | Release candidate 2 for backport packages 2020.05.20 (#8898)                                 |
| [12c5e5d8a](https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79) | 2020-05-17  | Prepare release candidate for backport packages (#8891)                                      |
| [8985df0bf](https://github.com/apache/airflow/commit/8985df0bfcb5f2b2cd69a21b9814021f9f8ce953) | 2020-05-16  | Monitor pods by labels instead of names (#6377)                                              |
| [f3521fb0e](https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca) | 2020-05-16  | Regenerate readme files for backport package release (#8886)                                 |
| [92585ca4c](https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92) | 2020-05-15  | Added automated release notes generation for backport operators (#8807)                      |
| [f82ad452b](https://github.com/apache/airflow/commit/f82ad452b0f4ebd1428bc9669641a632dc87bb8c) | 2020-05-15  | Fix KubernetesPodOperator pod name length validation (#8829)                                 |
| [1ccafc617](https://github.com/apache/airflow/commit/1ccafc617c4cb9622e3460ad7c190f3ee67c3b32) | 2020-04-02  | Add spark_kubernetes system test (#7875)                                                     |
| [cd546b664](https://github.com/apache/airflow/commit/cd546b664fa35a2bf85acd77af578c909a327d92) | 2020-03-23  | Add missing call to Super class in &#39;cncf&#39; &amp; &#39;docker&#39; providers (#7825)                       |
| [6c39a3bf9](https://github.com/apache/airflow/commit/6c39a3bf97414ba2438669894db65c36ccbeb61a) | 2020-03-10  | [AIRFLOW-6542] Add spark-on-k8s operator/hook/sensor (#7163)                                 |
| [42eef3821](https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e) | 2020-03-07  | [AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)                             |
| [3320e432a](https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc) | 2020-02-24  | [AIRFLOW-6817] Lazy-load `airflow.DAG` to keep user-facing API untouched (#7517)             |
| [0ec277412](https://github.com/apache/airflow/commit/0ec2774120d43fa667a371b384e6006e1d1c7821) | 2020-02-24  | [AIRFLOW-5629] Implement Kubernetes priorityClassName in KubernetesPodOperator (#7395)       |
| [9cbd7de6d](https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b) | 2020-02-18  | [AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)     |
| [967930c0c](https://github.com/apache/airflow/commit/967930c0cb6e2293f2a49e5c9add5aa1917f3527) | 2020-02-11  | [AIRFLOW-5413] Allow K8S worker pod to be configured from JSON/YAML file (#6230)             |
| [96f834389](https://github.com/apache/airflow/commit/96f834389e03884025534fabd862155061f53fd0) | 2020-02-03  | [AIRFLOW-6678] Pull event logs from Kubernetes (#7292)                                       |
| [97a429f9d](https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55) | 2020-02-02  | [AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)                                     |
| [cf141506a](https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721) | 2020-02-02  | [AIRFLOW-6708] Set unique logger names (#7330)                                               |
| [373c6aa4a](https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b) | 2020-01-30  | [AIRFLOW-6682] Move GCP classes to providers package (#7295)                                 |
| [83c037873](https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2) | 2020-01-30  | [AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)                           |
| [059eda05f](https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf) | 2020-01-21  | [AIRFLOW-6610] Move software classes to providers package (#7231)                            |
