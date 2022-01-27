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


Changelog
---------

3.0.1
.....


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

Generally speaking if you did not subclass ``KubernetesPodOperator`` and you didn't use the ``PodLauncher`` class directly,
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

.. Review and move the new changes to one of the sections above:
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

* ``Add 'KubernetesPodOperat' 'pod-template-file' jinja template support (#15942)``
* ``Save pod name to xcom for KubernetesPodOperator (#15755)``

Bug Fixes
~~~~~~~~~

* ``Bug Fix Pod-Template Affinity Ignored due to empty Affinity K8S Object (#15787)``
* ``Bug Pod Template File Values Ignored (#16095)``
* ``Fix issue with parsing error logs in the KPO (#15638)``
* ``Fix unsuccessful KubernetesPod final_state call when 'is_delete_operator_pod=True' (#15490)``

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
* ``Fix labels on the pod created by ''KubernetsPodOperator'' (#15492)``

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
