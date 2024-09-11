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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-cncf-kubernetes``


Changelog
---------

8.4.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: 'KubernetesExecutor' failing the task in case the watcher receives an event with the reason ProviderFailed (#41186)``
* ``fix: 'do_xcom_push' and 'get_logs' functionality for KubernetesJobOperator (#40814)``
* ``fix: 'KubernetesHook' loading config file with '_is_in_cluster' set as False (#41464)``
* ``fix: Missing 'slots_occupied' in 'CeleryKubernetesExecutor' and 'LocalKubernetesExecutor' (#41602)``

8.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``Describe behaviour in docstring correctly (#41458)``
* ``Remove deprecated SubDags (#41390)``
* ``reorder docstring of 'SparkKubernetesOperator' (#41372)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.3.4
.....

Bug Fixes
~~~~~~~~~

* ``Pass content of kube/config file to triggerer as a dictionary (#41178)``
* ``Fix confusing log message in kubernetes executor (#41035)``
* ``Fix ApiException handling when adopting completed pods (#41109)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Solve failing KPO task with task decorator and imported typing elements (#40642)``

Misc
~~~~


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

8.3.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix Scheduler restarting due to too many completed pods in cluster (#40183)``

Misc
~~~~

* ``Bump minimum kubernetes lib version to kubernetes 29.0.0 (#40253)``

8.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fixes KubernetesPodTrigger failing running pods with timeout (#40019)``
* ``Refresh properties on KubernetesPodOperator on token expiration also when logging (#39789)``
* ``Fix reattach_on_restart parameter for the sync mode (#39329)``
* ``Avoid resetting adopted task instances when retrying for kubernetes executor (#39406)``

Misc
~~~~

* ``Include fatal reason for pod pending events (#39924)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``iMPlement per-provider tests with lowest-direct dependency resolution (#39946)``
   * ``Resolve common providers deprecations in tests (#40036)``

8.3.0
.....

Features
~~~~~~~~

* ``Add timeout when watching pod events in k8s executor (#39551)``
* ``Add retry logic for KubernetesCreateResourceOperator and KubernetesJobOperator (#39201)``

Bug Fixes
~~~~~~~~~

* ``Fix deprecated calls in 'cncf.kubernetes' provider (#39381)``
* ``Handling exception getting logs when pods finish success (#39296)``
* ``fix wrong arguments in read_namespaced_pod_log call (#39874)``

Misc
~~~~

* ``Move Kubernetes cli to provider package (#39587)``
* ``Remove compat code for 2.7.0 - its now the min Airflow version (#39591)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Replace pod_manager.read_pod_logs with client.read_namespaced_pod_log in KubernetesPodOperator._write_logs (#39112)``
* ``Add a warning message to KPO to warn of one second interval logs duplication (#39861)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``
   * ``Faster 'airflow_version' imports (#39552)``
   * ``Prepare docs 3rd wave May 2024 (#39738)``

8.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


Features
~~~~~~~~

* ``Add missing informative logs in KPO trigger until container has not finished (#37546)``

Bug Fixes
~~~~~~~~~

* ``fixes templated env vars for k8s pod operator (#39139)``
* ``Fix SparkKubernetesOperator when using initContainers (#38119)``
* ``Refresh properties on KubernetesPodOperator when k8s fails due to token expiration (#39325)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Remove unnecessary validation from cncf provider. (#39238)``
* ``Moves airflow import in deprecated pod_generator to local (#39062)``
* ``KPO xcom sidecar PodDefault usage (#38951)``

8.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Avoid logging empty line KPO (#38247)``

8.1.0
.....

Features
~~~~~~~~

* ``KPO Add follow log in termination step (#38081)``
* ``Add GKECreateCustomResourceOperator and GKEDeleteCustomResourceOperator operators (#37616)``
* ``Implement deferrable mode for KubernetesJobOperator (#38251)``
* ``Create KubernetesPatchJobOperator operator (#38146)``
* ``Implement delete_on_status parameter for KubernetesDeleteJobOperator (#38458)``
* ``Implement deferrable mode for GKEStartJobOperator (#38454)``
* ``Use startup_check_interval_seconds instead of poll_interval to check pod while startup (#38075)``
* ``Implement wait_until_job_complete parameter for KubernetesJobOperator (#37998)``

Bug Fixes
~~~~~~~~~

* ``Use SIGINT signal number instead of signal name (#37905)``
* ``Fix spark operator log retrieval from driver (#38106)``
* ``Fix dynamic allocation specs handling for custom launcher (#38223)``
* ``Fix case if 'SparkKubernetesOperator.application_file' is templated file (#38035)``
* ``fix: reduce irrelevant error logs for pod events. (#37944)``

Misc
~~~~

* ``Add GKEListJobsOperator and GKEDescribeJobOperator (#37598)``
* ``removed usage of deprecated function  for naming the pod in provider k8s pod.py (#38638)``
* ``Create DeleteKubernetesJobOperator and GKEDeleteJobOperator operators (#37793)``
* ``Refactor GKE hooks (#38404)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider cncf kubernetes (#38799)``
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``

8.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Immediately fail the task in case of worker pod having a fatal container state (#37670)``
* ``Skip pod cleanup in case of pod creation failed (#37671)``

Misc
~~~~

* ``Avoid non-recommended usage of logging (#37792)``
* ``Migrate executor docs to respective providers (#37728)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Avoid to use too broad 'noqa' (#37862)``

8.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

In the case of Kube API exceeded quota errors, we have introduced the ``task_publish_max_retries``
flag to control the re-queuing task behavior. Changed the default behavior from unlimited
retries to 0. The default behavior is no retries (``task_publish_max_retries==0``). For
unlimited retries, set ``task_publish_max_retries=-1``. For a fixed number of retries, set
``task_publish_max_retries`` to any positive integer.

* ``Fix: The task is stuck in a queued state forever in case of pod launch errors (#36882)``

Features
~~~~~~~~

* ``Add logging_interval in KubernetesPodOperator to log container log periodically (#37279)``
* ``Create GKEStartJobOperator and KubernetesJobOperator (#36847)``

Bug Fixes
~~~~~~~~~

* ``Fix occasional attr-undefined for the python_kubernetes_script (#37318)``
* ``Fix hanging KPO on deferrable task with do_xcom_push (#37300)``
* ``Fix rendering 'SparkKubernetesOperator.template_body' (#37271)``
* ``Fix assignment of template field in '__init__' in 'KubernetesPodOperator' (#37010)``
* ``KPO Maintain backward compatibility for execute_complete and trigger run method (#37454)``
* ``Fix KPO task hanging when pod fails to start within specified timeout (#37514)``
* ``Fix KeyError when KPO exits too soon (#37508)``

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``
* ``Kubernetes version bump (#37040)``
* ``Add GKEStartKueueInsideClusterOperator (#37072)``
* ``Convert Kubernetes ApiException status code to string to ensure it's correctly checked (#37405)``

.. Review and move the new changes to one of the sections above:
   * ``Add d401 support to kubernetes provider (#37301)``
   * ``Revert "KPO Maintain backward compatibility for execute_complete and trigger run method (#37363)" (#37446)``
   * ``KPO Maintain backward compatibility for execute_complete and trigger run method (#37363)``
   * ``Prepare docs 1st wave of Providers February 2024 (#37326)``
   * ``Prepare docs 1st wave (RC2) of Providers February 2024 (#37471)``
   * ``Add comment about versions updated by release manager (#37488)``

7.14.0
......

Features
~~~~~~~~

* ``Add SparkKubernetesOperator crd implementation (#22253)``
* ``Template field support for configmaps in the KubernetesPodOperator (#36922)``
* ``Create a generic callbacks class for KubernetesPodOperator (#35714)``

Bug Fixes
~~~~~~~~~

* ``fix: Avoid retrying after KubernetesPodOperator has been marked as failed (#36749)``
* ``Fix stacklevel in warnings.warn into the providers (#36831)``
* ``Increase tenacity wait in read_pod_logs (#36955)``
* ``36888-Fix k8 configmap issue in 7.14.0rc1 (#37001)``

Misc
~~~~

* ``Change field type for kube_config (#36752)``
* ``Changing wording in docstring for CNCF provider (#36547)``
* ``Add support of Pendulum 3 (#36281)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

7.13.0
......

Features
~~~~~~~~

* ``Allow changing of 'config_file' in 'KubernetesResourceBaseOperator' (#36397)``

Misc
~~~~

* ``Add reminder about update stub file in case of change KubernetesPodOperator's arguments (#36434)``
* ``Don't get pod status in KubernetesPodOperator if skip_on_exit_code is not set (#36355)``
* ``Remove deprecated input parameters in the k8s pod operator (#36433)``
* ``Delete get_python_source from Kubernetes decorator after bumping min airflow version to 2.6.0 (#36426)``
* ``Remove duplicated methods in K8S pod operator module and import them from helper function (#36427)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

7.12.0
......

Features
~~~~~~~~

* ``Add _request_timeout to KPO log fetch calls (#36297)``
* ``Add 'pod_template_dict' field to 'KubernetesPodOperator' (#33174)``
* ``KubernetesPodTrigger: add exception stack trace in TriggerEvent (#35716)``
* ``Make pod_name length equal to HOST_NAME_MAX (#36332)``
* ``Move KubernetesPodTrigger hook to a cached property (#36290)``

Bug Fixes
~~~~~~~~~

* ``Kubernetes executor running slots leak fix (#36240)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``
* ``list pods performance optimization (#36092)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

7.11.0
......

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix: KPO typing env_vars (#36048)``
* ``Stop converting state to TaskInstanceState when it's None (#35891)``
* ``Feature pass dictionary configuration in application_file in SparkKubernetesOperator (#35848)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Drive-by improvements to convert_env_vars (#36062)``
   * ``Use fail instead of change_state(failed) in K8S executor (#35900)``

7.10.0
......

Features
~~~~~~~~

* ``Add annotations field into  in KubernetesPodOperator (#35641)``
* ``Add custom_resource_definition to KubernetesResourceBaseOperator (#35600)``

Bug Fixes
~~~~~~~~~

* ``Revert Remove PodLoggingStatus object #35422 (#35822)``
* ``Fix K8S executor override config using pod_override_object (#35185)``
* ``Fix and reapply templates for provider documentation (#35686)``

Misc
~~~~

* ``Remove inconsequential code bits in KPO logging (#35416)``
* ``Remove non existing params from 'KubernetesResourceBaseOperator' docstring``
* ``KubernetesExecutor observability Improvements (#35579)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add bandit to pre-commit to detect common security issues (#34247)``
   * ``Use reproducible builds for provider packages (#35693)``

7.9.0
.....

Features
~~~~~~~~

* ``Add verificationy that provider docs are as expected (#35424)``
* ``Add startup_check_interval_seconds to PodManager's await_pod_start (#34231)``

Bug Fixes
~~~~~~~~~

* ``Remove before_log in KPO retry and add traceback when interrupted (#35423)``
* ``Remove tenancity on KPO logs inner func consume_logs (#35504)``

Misc
~~~~

* ``Simplify KPO multi container log reconciliation logic (#35450)``
* ``Remove PodLoggingStatus object (#35422)``
* ``Improve clear_not_launched_queued_tasks call duration (#34985)``
* ``Use constant for empty xcom result sentinel (#35451)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch from Black to Ruff formatter (#35287)``

7.8.0
.....

Features
~~~~~~~~

* ``Added to the rendering of KubernetesOperator V1VolumeMount, sub_path (#35129)``
* ``feat: add hostAliases to pod spec in KubernetesPodOperator (#35063)``

Bug Fixes
~~~~~~~~~

* ``Replace blocking IO with async IO in AsyncKubernetesHook (#35162)``
* ``Consolidate the warning stacklevel in KubernetesPodTrigger (#35079)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``D401 Support - A thru Common (Inclusive) (#34934)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

7.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix parsing KubernetesPodOperator multiline logs (#34412)``
* ``Fix KubernetesPodTrigger startup timeout (#34579)``
* ``Fix Pod not being removed after istio-sidecar is removed  (#34500)``
* ``Remove duplicated logs by reusing PodLogsConsumer (#34127)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``warn level for deprecated set to stacklevel 2 (#34530)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``
   * ``Update CHANGELOG.rst (#34625)``
   * ``Refactor shorter defaults in providers (#34347)``

7.6.0
.....

Features
~~~~~~~~

* ``Add 'progress_callback' parameter to 'KubernetesPodOperator' (#34153)``

Bug Fixes
~~~~~~~~~

* ``Move definition of Pod*Exceptions to pod_generator (#34346)``
* ``Push to xcom before 'KubernetesPodOperator' deferral (#34209)``

Misc
~~~~

* ``Refactor: Consolidate import textwrap in providers (#34220)``

7.5.1
.....

Bug Fixes
~~~~~~~~~

* ``fix(providers/spark-kubernetes): respect soft_fail argument when exception is raised (#34167)``
* ``Use 'cached_property' for hook in SparkKubernetesSensor (#34106)``
* ``Use cached property for hook in SparkKubernetesOperator (#34130)``

Misc
~~~~

* ``Combine similar if logics in providers (#33987)``
* ``Remove useless string join from providers (#33968)``
* ``Refactor unneeded  jumps in providers (#33833)``
* ``replace loop by any when looking for a positive value in providers (#33984)``
* ``Move the try outside the loop when this is possible in kubernetes provider (#33977)``
* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Replace dict.items by values when key is not used in providers (#33939)``
* ``Refactor: Consolidate import datetime (#34110)``

7.5.0
.....

Features
~~~~~~~~

* ``Add istio test, use curl /quitquitquit to exit sidecar, and some othe… (#33306)``
* ``Add 'active_deadline_seconds' parameter to 'KubernetesPodOperator' (#33379)``
* ``Make cluster_context templated (#33604)``


Bug Fixes
~~~~~~~~~

* ``Fix KubernetesPodOperator duplicating logs when interrupted (#33500)``
* ``Fix 2.7.0 db migration job errors (#33652)``
* ``Inspect container state rather than last_state when deciding whether to skip (#33702)``
* ``Fix kill istio proxy logic (#33779)``

Misc
~~~~

* ``Introducing class constant to make worker pod log lines configurable (#33378)``
* ``Adding typing for KPO SCC objects (#33381)``
* ``Refactor: Remove useless str() calls (#33629)``
* ``Refactor: Improve detection of duplicates and list sorting (#33675)``
* ``Refactor Sqlalchemy queries to 2.0 style (Part 7) (#32883)``
* ``Consolidate import and usage of itertools (#33479)``
* ``Simplify conditions on len() in other providers (#33569)``
* ``Import utc from datetime and normalize its import (#33450)``
* ``Always use 'Literal' from 'typing_extensions' (#33794)``
* ``Use literal dict instead of calling dict() in providers (#33761)``
* ``Improve modules import in cncf.kubernetes probvider by move some of them into a type-checking block (#33781)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix typos (double words and it's/its) (#33623)``
   * ``Exclude deprecated "operators.kubernetes_pod" module from provider.yaml (#33641)``
   * ``D205 Support - Providers - Final Pass (#33303)``
   * ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``

7.4.2
.....

Misc
~~~~

* ``Add missing re2 dependency to cncf.kubernetes and celery providers (#33237)``
* ``Make the 'OnFinishAction' enum inherit from str to support passing it to 'KubernetesPodOperatpor' (#33228)``
* ``Refactor: Simplify code in providers/cncf (#33230)``
* ``Replace State by TaskInstanceState in Airflow executors (#32627)``

7.4.1
.....


Bug Fixes
~~~~~~~~~

* ``Fix waiting the base container when reading the logs of other containers (#33127)``
* ``Fix: Configurable Docker image of 'xcom_sidecar' (#32858)``
* ``Fix 'KubernetesPodOperator' sub classes default container_logs (#33090)``
* ``Consider custom pod labels on pod finding process on 'KubernetesPodOperator' (#33057)``

Misc
~~~~

* ``add documentation generation for CLI commands from executors (#33081)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Fix waiting the base container when reading the logs of other containers (#33092)" (#33125)``
   * ``Fix waiting the base container when reading the logs of other containers (#33092)``

7.4.0
.....

.. note::
  This provider release is the first release that has Kubernetes Executor and
  Local Kubernetes Executor moved from the core ``apache-airflow`` package to the ``cncf.kubernetes``
  provider package.

Features
~~~~~~~~

* ``Move all k8S classes to cncf.kubernetes provider (#32767)``
* ``[AIP-51] Executors vending CLI commands (#29055)``
* ``Add 'termination_message_policy' parameter to 'KubernetesPodOperator' (#32885)``

Misc
~~~~

* ``Update the watcher resource version in SparkK8SOp when it's too old (#32768)``
* ``Add deprecation info to the providers modules and classes docstring (#32536)``
* ``Raise original import error in CLI vending of executors (#32931)``

7.3.0
.....

Features
~~~~~~~~

* ``Logging from all containers in KubernetesOperatorPod (#31663)``

Bug Fixes
~~~~~~~~~

* ``Fix async KPO by waiting pod termination in 'execute_complete' before cleanup (#32467)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Stragglers and new additions (#32447)``

7.2.0
.....

Features
~~~~~~~~

* ``Add 'on_finish_action' to 'KubernetesPodOperator' (#30718)``

Bug Fixes
~~~~~~~~~

* ``Fix KubernetesPodOperator validate xcom json and add retries (#32113)``
* ``Fix 'KubernetesPodTrigger' waiting strategy (#31348)``
* ``fix spark-kubernetes-operator compatibality (#31798)``

Misc
~~~~

* ``Add default_deferrable config (#31712)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

7.1.0
.....

.. note::
  This release dropped support for Python 3.7


Features
~~~~~~~~

* ``KubernetesResourceOperator - KubernetesDeleteResourceOperator & KubernetesCreateResourceOperator (#29930)``
* ``add a return when the event is yielded in a loop to stop the execution (#31985)``
* ``Add possibility to disable logging the pod template in a case when task fails (#31595)``


Bug Fixes
~~~~~~~~~

* ``Remove return statement after yield from triggers class (#31703)``
* ``Fix Fargate logging for AWS system tests (#31622)``

Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check (#31742)``
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

7.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  Return None when namespace is not defined in the Kubernetes connection

* ``Remove deprecated features from KubernetesHook (#31402)``

Features
~~~~~~~~

.. note::
  If ``kubernetes_default`` connection is not defined, then KubernetesHook / KubernetesPodOperator will behave as though given ``conn_id=None``.
  This should make it easier to mitigate breaking change introduced in 6.0.0

* ``K8s hook should still work with missing default conn (#31187)``
* ``Add protocol to define methods relied upon by KubernetesPodOperator (#31298)``

Bug Fixes
~~~~~~~~~

* ``Fix kubernetes task decorator pickle error (#31110)``

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Empty xcom result file log message more specific (#31228)``
* ``Add options to KubernetesPodOperator (#30992)``
* ``add missing read for K8S config file from conn in deferred 'KubernetesPodOperator'  (#29498)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add cli cmd to list the provider trigger info (#30822)``
   * ``Fix pod describing on system test failure (#31191)``
   * ``Docstring improvements (#31375)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``

6.1.0
.....

Features
~~~~~~~~

* ``Add multiple exit code handling in skip logic for 'DockerOperator' and 'KubernetesPodOperator' (#30769)``
* ``Skip KubernetesPodOperator task when it returns a provided exit code (#29000)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Deprecate 'skip_exit_code' in 'DockerOperator' and 'KubernetesPodOperator' (#30733)``
  * ``Remove skip_exit_code from KubernetesPodOperator (#30788)``

6.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Use ``kubernetes_default`` connection by default in the ``KubernetesPodOperator``.

* ``Use default connection id for KubernetesPodOperator (#28848)``

Features
~~~~~~~~

* ``Allow to set limits for XCOM container (#28125)``

.. Review and move the new changes to one of the sections above:
   * ``Add mechanism to suspend providers (#30422)``

5.3.0
.....

Features
~~~~~~~~

* ``enhance spark_k8s_operator (#29977)``

Bug Fixes
~~~~~~~~~

* ``Fix KubernetesPodOperator xcom push when 'get_logs=False' (#29052)``
* ``Fixed hanged KubernetesPodOperator (#28336)``

Misc
~~~~
* ``Align cncf provider file names with AIP-21 (#29905)``
* ``Remove "boilerplate" from all taskflow decorators (#30118)``
* ``Ensure setup/teardown work on a previously decorated function (#30216)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``adding trigger info to provider yaml (#29950)``

5.2.2
.....

Bug Fixes
~~~~~~~~~

* ``'KubernetesPodOperator._render_nested_template_fields' improved by changing the conditionals for a map (#29760)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and augment 'check-for-inclusive-language' CI check (#29549)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix @task.kubernetes to receive input and send output (#28942)``

5.2.0
.....

Features
~~~~~~~~

* ``Add deferrable mode to ''KubernetesPodOperator'' (#29017)``
* ``Allow setting the name for the base container within K8s Pod Operator (#28808)``

Bug Fixes
~~~~~~~~~

* ``Patch only single label when marking KPO checked (#29279)``

5.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix Incorrect 'await_container_completion' (#28771)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch to ruff for faster static checks (#28893)``

5.1.0
.....

Features
~~~~~~~~

* ``Add Flink on K8s Operator  (#28512)``
* ``Add volume-related nested template fields for KPO (#27719)``
* ``Allow longer pod names for k8s executor / KPO (#27736)``
* ``Use labels instead of pod name for pod log read in k8s exec (#28546)``

Bug Fixes
~~~~~~~~~

* ``Patch "checked" when pod not successful (#27845)``
* ``Keep pod name for k8s executor under 63 characters (#28237)``

Misc
~~~~

* ``Remove outdated compat imports/code from providers (#28507)``
* ``Restructure Docs  (#27235)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated docs for RC3 wave of providers (#27937)``
   * ``Prepare for follow-up relase for November providers (#27774)``

.. Review and move the new changes to one of the sections above:

5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

Previously KubernetesPodOperator considered some settings from the Airflow config's ``kubernetes`` section.
Such consideration was deprecated in 4.1.0 and is now removed.  If you previously relied on the Airflow
config, and you want client generation to have non-default configuration, you will need to define your
configuration in an Airflow connection and set KPO to use the connection.  See kubernetes provider
documentation on defining a kubernetes Airflow connection for details.

Drop support for providing ``resource`` as dict in ``KubernetesPodOperator``. You
should use ``container_resources`` with ``V1ResourceRequirements``.

Param ``node_selectors`` has been removed in ``KubernetesPodOperator``; use ``node_selector`` instead.

The following backcompat modules for KubernetesPodOperator are removed and you must now use
the corresponding objects from the kubernetes library:

* ``airflow.kubernetes.backcompat.pod``
* ``airflow.kubernetes.backcompat.pod_runtime_info_env``
* ``airflow.kubernetes.backcompat.volume``
* ``airflow.kubernetes.backcompat.volume_mount``

* ``Remove deprecated backcompat objects for KPO (#27518)``
* ``Remove support for node_selectors param in KPO (#27515)``
* ``Remove unused backcompat method in k8s hook (#27490)``
* ``Drop support for providing ''resource'' as dict in ''KubernetesPodOperator'' (#27197)``
* ``Don't consider airflow core conf for KPO (#26849)``

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Use log.exception where more economical than log.error (#27517)``

Features
~~~~~~~~

KubernetesPodOperator argument ``name`` is now optional. Previously, ``name`` was a
required argument for KubernetesPodOperator when also not supplying pod
template or full pod spec. Now, if ``name`` is not supplied, ``task_id`` will be used.

KubernetesPodOperator argument ``namespace`` is now optional.  If not supplied via KPO param or pod
template file or full pod spec, then we'll check the airflow conn,
then if in a k8s pod, try to infer the namespace from the container, then finally
will use the ``default`` namespace.

When using an Airflow connection of type ``kubernetes``, if defining the connection in an env var
or secrets backend, it's no longer necessary to prefix the "extra" fields with ``extra__kubernetes__``.
If ``extra`` contains duplicate fields (one with prefix, one without) then the non-prefixed
one will be used.

* ``Remove extra__kubernetes__ prefix from k8s hook extras (#27021)``
* ``Add container_resources as KubernetesPodOperator templatable (#27457)``
* ``add container_name option for SparkKubernetesSensor (#26560)``
* ``Allow xcom sidecar container image to be configurable in KPO (#26766)``
* ``Improve task_id to pod name conversion (#27524)``
* ``Make pod name optional in KubernetesPodOperator (#27120)``
* ``Make namespace optional for KPO (#27116)``
* ``Enable template rendering for env_vars field for the @task.kubernetes decorator (#27433)``

Bug Fixes
~~~~~~~~~

* ``Fix KubernetesHook fail on an attribute absence (#25787)``
* ``Fix log message for kubernetes hooks (#26999)``
* ``KPO should use hook's get namespace method to get namespace (#27516)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
  * ``Update old style typing (#26872)``
  * ``Enable string normalization in python formatting - providers (#27205)``
  * ``Update docs for September Provider's release (#26731)``

New deprecations
~~~~~~~~~~~~~~~~

* In ``KubernetesHook.get_namespace``, if a connection is defined but a namespace isn't set, we
   currently return 'default'; this behavior is deprecated (#27202). In the next release, we'll return ``None``.
* ``Deprecate use of core get_kube_client in PodManager (#26848)``


4.4.0
.....

Features
~~~~~~~~

* ``feat(KubernetesPodOperator): Add support of container_security_context (#25530)``
* ``Add @task.kubernetes taskflow decorator (#25663)``
* ``pretty print KubernetesPodOperator rendered template env_vars (#25850)``

Bug Fixes
~~~~~~~~~

* ``Avoid calculating all elements when one item is needed (#26377)``
* ``Wait for xcom sidecar container to start before sidecar exec (#25055)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
    * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
    * ``Prepare to release cncf.kubernetes provider (#26588)``

4.3.0
.....

Features
~~~~~~~~

* ``Improve taskflow type hints with ParamSpec (#25173)``

Bug Fixes
~~~~~~~~~

* ``Fix xcom_sidecar stuck problem (#24993)``

4.2.0
.....

Features
~~~~~~~~

* ``Add 'airflow_kpo_in_cluster' label to KPO pods (#24658)``
* ``Use found pod for deletion in KubernetesPodOperator (#22092)``

Bug Fixes
~~~~~~~~~

* ``Revert "Fix await_container_completion condition (#23883)" (#24474)``
* ``Update providers to use functools compat for ''cached_property'' (#24582)``

Misc
~~~~
* ``Rename 'resources' arg in Kub op to k8s_resources (#24673)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Only assert stuff for mypy when type checking (#24937)``
   * ``Remove 'xcom_push' flag from providers (#24823)``
   * ``More typing and minor refactor for kubernetes (#24719)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Use our yaml util in all providers (#24720)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

4.1.0
.....

Features
~~~~~~~~

* Previously, KubernetesPodOperator relied on core Airflow configuration (namely setting for kubernetes
  executor) for certain settings used in client generation.  Now KubernetesPodOperator
  uses KubernetesHook, and the consideration of core k8s settings is officially deprecated.

* If you are using the Airflow configuration settings (e.g. as opposed to operator params) to
  configure the kubernetes client, then prior to the next major release you will need to
  add an Airflow connection and set your KPO tasks to use that connection.

* ``Use KubernetesHook to create api client in KubernetesPodOperator (#20578)``
* ``[FEATURE] KPO use K8S hook (#22086)``
* ``Add param docs to KubernetesHook and KubernetesPodOperator (#23955) (#24054)``

Bug Fixes
~~~~~~~~~

* ``Use "remote" pod when patching KPO pod as "checked" (#23676)``
* ``Don't use the root logger in KPO _suppress function (#23835)``
* ``Fix await_container_completion condition (#23883)``

Misc
~~~~

* ``Migrate Cncf.Kubernetes example DAGs to new design #22441 (#24132)``
* ``Clean up f-strings in logging calls (#23597)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``pydocstyle D202 added (#24221)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

4.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix: Exception when parsing log #20966 (#23301)``
* ``Fixed Kubernetes Operator large xcom content Defect  (#23490)``
* ``Clarify 'reattach_on_restart' behavior (#23377)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add YANKED to yanked releases of the cncf.kubernetes (#23378)``

   * ``Fix k8s pod.execute randomly stuck indefinitely by logs consumption (#23497) (#23618)``
   * ``Revert "Fix k8s pod.execute randomly stuck indefinitely by logs consumption (#23497) (#23618)" (#23656)``

4.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Add k8s container's error message in airflow exception (#22871)``
* ``KubernetesHook should try incluster first when not otherwise configured (#23126)``
* ``KubernetesPodOperator should patch "already checked" always (#22734)``
* ``Delete old Spark Application in SparkKubernetesOperator (#21092)``
* ``Cleanup dup code now that k8s provider requires 2.3.0+ (#22845)``
* ``Fix ''KubernetesPodOperator'' with 'KubernetesExecutor'' on 2.3.0 (#23371)``
* ``Fix KPO to have hyphen instead of period (#22982)``
* ``Fix new MyPy errors in main (#22884)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Prepare documentation for cncf.kubernetes 4.0.1 release (#23374)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The provider in version 4.0.0 only works with Airflow 2.3+. Please upgrade
Airflow to 2.3 version if you want to use the features or fixes in 4.* line
of the provider.

The main reason for the incompatibility is using latest Kubernetes Libraries.
The ``cncf.kubernetes`` provider requires newer version of libraries than
Airflow 2.1 and 2.2 used for Kubernetes Executor and that makes the provider
incompatible with those Airflow versions.

Features
~~~~~~~~

* ``Log traceback only on ''DEBUG'' for KPO logs read interruption (#22595)``
* ``Update our approach for executor-bound dependencies (#22573)``
* ``Optionally not follow logs in KPO pod_manager (#22412)``


Bug Fixes
~~~~~~~~~

* ``Stop crashing when empty logs are received from kubernetes client (#22566)``

3.1.2 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Installing on Airflow 2.1, 2.2 allows to install unsupported kubernetes library > 11.0.0``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``
* ``Fix "run_id" k8s and elasticsearch compatibility with Airflow 2.1 (#22385)``

Misc
~~~~

* ``Remove RefreshConfiguration workaround for K8s token refreshing (#20759)``

3.1.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Installing on Airflow 2.1, 2.2 allows to install unsupported kubernetes library > 11.0.0``

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

3.1.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Installing on Airflow 2.1, 2.2 allows to install unsupported kubernetes library > 11.0.0``

Features
~~~~~~~~

* ``Add map_index label to mapped KubernetesPodOperator (#21916)``
* ``Change KubernetesPodOperator labels from execution_date to run_id (#21960)``

Misc
~~~~

* ``Support for Python 3.10``
* ``Fix Kubernetes example with wrong operator casing (#21898)``
* ``Remove types from KPO docstring (#21826)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add pre-commit check for docstring param types (#21398)``

3.0.2 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Installing on Airflow 2.1, 2.2 allows to install unsupported kubernetes library > 11.0.0``

Bug Fixes
~~~~~~~~~

* ``Add missed deprecations for cncf (#20031)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Make ''delete_pod'' change more prominent in K8s changelog (#20753)``
   * ``Fix MyPy Errors for providers: Tableau, CNCF, Apache (#20654)``
   * ``Add optional features in providers. (#21074)``
   * ``Add documentation for January 2021 providers release (#21257)``

3.0.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Installing on Airflow 2.1, 2.2 allows to install unsupported kubernetes library > 11.0.0``

Misc
~~~~

* ``Update Kubernetes library version (#18797)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Parameter is_delete_operator_pod default is changed to True (#20575)``
* ``Simplify KubernetesPodOperator (#19572)``
* ``Move pod_mutation_hook call from PodManager to KubernetesPodOperator (#20596)``
* ``Rename ''PodLauncher'' to ''PodManager'' (#20576)``

Parameter is_delete_operator_pod has new default
````````````````````````````````````````````````

Previously, the default for param ``is_delete_operator_pod`` was ``False``, which means that
after a task runs, its pod is not deleted by the operator and remains on the
cluster indefinitely.  With this release, we change the default to ``True``.

Notes on changes KubernetesPodOperator and PodLauncher
``````````````````````````````````````````````````````

.. warning:: Many methods in ``KubernetesPodOperator`` and ``PodLauncher`` have been renamed.
    If you have subclassed ``KubernetesPodOperator`` you will need to update your subclass to reflect
    the new structure. Additionally ``PodStatus`` enum has been renamed to ``PodPhase``.

Overview
''''''''

Generally speaking if you did not subclass ``KubernetesPodOperator`` and you did not use the ``PodLauncher`` class directly,
then you don't need to worry about this change.  If however you have subclassed ``KubernetesPodOperator``, what
follows are some notes on the changes in this release.

One of the principal goals of the refactor is to clearly separate the "get or create pod" and
"wait for pod completion" phases.  Previously the "wait for pod completion" logic would be invoked
differently depending on whether the operator were to  "attach to an existing pod" (e.g. after a
worker failure) or "create a new pod" and this resulted in some code duplication and a bit more
nesting of logic.  With this refactor we encapsulate  the "get or create" step
into method ``KubernetesPodOperator.get_or_create_pod``, and pull the monitoring and XCom logic up
into the top level of ``execute`` because it can be the same for "attached" pods and "new" pods.

The ``KubernetesPodOperator.get_or_create_pod`` tries first to find an existing pod using labels
specific to the task instance (see ``KubernetesPodOperator.find_pod``).
If one does not exist it ``creates a pod <~.PodManager.create_pod>``.

The "waiting" part of execution has three components.  The first step is to wait for the pod to leave the
``Pending`` phase (``~.KubernetesPodOperator.await_pod_start``). Next, if configured to do so,
the operator will follow the base container logs and forward these logs to the task logger until
the ``base`` container is done. If not configured to harvest the
logs, the operator will instead ``KubernetesPodOperator.await_container_completion``
either way, we must await container completion before harvesting xcom. After (optionally) extracting the xcom
value from the base container, we ``await pod completion <~.PodManager.await_pod_completion>``.

Previously, depending on whether the pod was "reattached to" (e.g. after a worker failure) or
created anew, the waiting logic may have occurred in either ``handle_pod_overlap`` or ``create_new_pod_for_operator``.

After the pod terminates, we execute different cleanup tasks depending on whether the pod terminated successfully.

If the pod terminates *unsuccessfully*, we attempt to log the pod events ``PodLauncher.read_pod_events>``. If
additionally the task is configured *not* to delete the pod after termination, we apply a label ``KubernetesPodOperator.patch_already_checked>``
indicating that the pod failed and should not be "reattached to" in a retry.  If the task is configured
to delete its pod, we delete it ``KubernetesPodOperator.process_pod_deletion>``.  Finally,
we raise an AirflowException to fail the task instance.

If the pod terminates successfully, we delete the pod ``KubernetesPodOperator.process_pod_deletion>``
(if configured to delete the pod) and push XCom (if configured to push XCom).

Details on method renames, refactors, and deletions
'''''''''''''''''''''''''''''''''''''''''''''''''''

In ``KubernetesPodOperator``:

* Method ``create_pod_launcher`` is converted to cached property ``pod_manager``
* Construction of k8s ``CoreV1Api`` client is now encapsulated within cached property ``client``
* Logic to search for an existing pod (e.g. after an airflow worker failure) is moved out of ``execute`` and into method ``find_pod``.
* Method ``handle_pod_overlap`` is removed. Previously it monitored a "found" pod until completion.  With this change the pod monitoring (and log following) is orchestrated directly from ``execute`` and it is the same  whether it's a "found" pod or a "new" pod. See methods ``await_pod_start``, ``follow_container_logs``, ``await_container_completion`` and ``await_pod_completion``.
* Method ``create_pod_request_obj`` is renamed ``build_pod_request_obj``.  It now takes argument ``context`` in order to add TI-specific pod labels; previously they were added after return.
* Method ``create_labels_for_pod`` is renamed ``_get_ti_pod_labels``.  This method doesn't return *all* labels, but only those specific to the TI. We also add parameter ``include_try_number`` to control the inclusion of this label instead of possibly filtering it out later.
* Method ``_get_pod_identifying_label_string`` is renamed ``_build_find_pod_label_selector``
* Method ``_try_numbers_match`` is removed.
* Method ``create_new_pod_for_operator`` is removed. Previously it would mutate the labels on ``self.pod``, launch the pod, monitor the pod to completion etc.  Now this logic is in part handled by ``get_or_create_pod``, where a new pod will be created if necessary. The monitoring etc is now orchestrated directly from ``execute``.  Again, see the calls to methods ``await_pod_start``, ``follow_container_logs``, ``await_container_completion`` and ``await_pod_completion``.

In class ``PodManager`` (formerly ``PodLauncher``):

* Method ``start_pod`` is removed and split into two methods: ``create_pod`` and ``await_pod_start``.
* Method ``monitor_pod`` is removed and split into methods ``follow_container_logs``, ``await_container_completion``, ``await_pod_completion``
* Methods ``pod_not_started``, ``pod_is_running``, ``process_status``, and ``_task_status`` are removed.  These were needed due to the way in which pod ``phase`` was mapped to task instance states; but we no longer do such a mapping and instead deal with pod phases directly and untransformed.
* Method ``_extract_xcom`` is renamed  ``extract_xcom``.
* Method ``read_pod_logs`` now takes kwarg ``container_name``


Other changes in ``pod_manager.py`` (formerly ``pod_launcher.py``):

* Class ``pod_launcher.PodLauncher`` renamed to ``pod_manager.PodManager``
* Enum-like class ``PodStatus`` is renamed ``PodPhase``, and the values are no longer lower-cased.
* The ``airflow.settings.pod_mutation_hook`` is no longer called in
  ``cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async``. For ``KubernetesPodOperator``,
  mutation now occurs in ``build_pod_request_obj``.
* Parameter ``is_delete_operator_pod`` default is changed to ``True`` so that pods are deleted after task
  completion and not left to accumulate. In practice it seems more common to disable pod deletion only on a
  temporary basis for debugging purposes and therefore pod deletion is the more sensible default.

Features
~~~~~~~~

* ``Add params config, in_cluster, and cluster_context to KubernetesHook (#19695)``
* ``Implement dry_run for KubernetesPodOperator (#20573)``
* ``Clarify docstring for ''build_pod_request_obj'' in K8s providers (#20574)``

Bug Fixes
~~~~~~~~~

* ``Fix Volume/VolumeMount KPO DeprecationWarning (#19726)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
     * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
     * ``Use typed Context EVERYWHERE (#20565)``
     * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
     * ``Even more typing in operators (template_fields/ext) (#20608)``
     * ``Update documentation for provider December 2021 release (#20523)``

2.2.0
.....

Features
~~~~~~~~

* ``Added namespace as a template field in the KPO. (#19718)``
* ``Decouple name randomization from name kwarg (#19398)``

Bug Fixes
~~~~~~~~~

* ``Checking event.status.container_statuses before filtering (#19713)``
* ``Coalesce 'extra' params to None in KubernetesHook (#19694)``
* ``Change to correct type in KubernetesPodOperator (#19459)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix duplicate changelog entries (#19759)``

2.1.0
.....

Features
~~~~~~~~

* ``Add more type hints to PodLauncher (#18928)``
* ``Add more information to PodLauncher timeout error (#17953)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update docstring to let users use 'node_selector' (#19057)``
   * ``Add pre-commit hook for common misspelling check in files (#18964)``

2.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix KubernetesPodOperator reattach when not deleting pods (#18070)``
* ``Make Kubernetes job description fit on one log line (#18377)``
* ``Do not fail KubernetesPodOperator tasks if log reading fails (#17649)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add August 2021 Provider's documentation (#17890)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Remove all deprecation warnings in providers (#17900)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix using XCom with ''KubernetesPodOperator'' (#17760)``
* ``Import Hooks lazily individually in providers manager (#17682)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix messed-up changelog in 3 providers (#17380)``
   * ``Fix static checks (#17256)``
   * ``Update spark_kubernetes.py (#17237)``

2.0.1
.....


Features
~~~~~~~~

* ``Enable using custom pod launcher in Kubernetes Pod Operator (#16945)``

Bug Fixes
~~~~~~~~~

* ``BugFix: Using 'json' string in template_field causes issue with K8s Operators (#16930)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify 'default_args' in Kubernetes example DAGs (#16870)``
   * ``Updating task dependencies (#16624)``
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``Add 'KubernetesPodOperator' 'pod-template-file' jinja template support (#15942)``
* ``Save pod name to xcom for KubernetesPodOperator (#15755)``

Bug Fixes
~~~~~~~~~

* ``Bug Fix Pod-Template Affinity Ignored due to empty Affinity K8S Object (#15787)``
* ``Bug Pod Template File Values Ignored (#16095)``
* ``Fix issue with parsing error logs in the KPO (#15638)``
* ``Fix unsuccessful KubernetesPodOperator final_state call when 'is_delete_operator_pod=True' (#15490)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.2.0
.....

Features
~~~~~~~~

* ``Require 'name' with KubernetesPodOperator (#15373)``
* ``Change KPO node_selectors warning to proper deprecationwarning (#15507)``

Bug Fixes
~~~~~~~~~

* ``Fix timeout when using XCom with KubernetesPodOperator (#15388)``
* ``Fix labels on the pod created by ''KubernetesPodOperator'' (#15492)``

1.1.0
.....

Features
~~~~~~~~

* ``Separate Kubernetes pod_launcher from core airflow (#15165)``
* ``Add ability to specify api group and version for Spark operators (#14898)``
* ``Use libyaml C library when available. (#14577)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Allow pod name override in KubernetesPodOperator if pod_template is used. (#14186)``
* ``Allow users of the KPO to *actually* template environment variables (#14083)``

1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Pass image_pull_policy in KubernetesPodOperator correctly (#13289)``

1.0.0
.....

Initial version of the provider.
