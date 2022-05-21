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

.. contents:: Apache Airflow Helm Chart Releases
   :local:
   :depth: 1

Run ``helm repo update`` before upgrading the chart to the latest version.

.. towncrier release notes start

Airflow Helm Chart 1.6.0 (2022-05-20)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.3.0`` (#23386)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.3.0``, previously it was ``2.2.4``.

``ingress.enabled`` is deprecated
"""""""""""""""""""""""""""""""""

Instead of having a single flag to control ingress resources for both the webserver and flower, there
are now separate flags to control them individually, ``ingress.web.enabled`` and ``ingress.flower.enabled``.
``ingress.enabled`` is now deprecated, but will still continue to control them both.

Flower disabled by default
""""""""""""""""""""""""""

Flower is no longer enabled by default when using CeleryExecutor. If you'd like to deploy it, set
``flower.enabed`` to true in your values file.

New Features
^^^^^^^^^^^^

- Support ``annotations`` on ``volumeClaimTemplates`` (#23433)
- Add support for ``topologySpreadConstraints`` to Helm Chart (#22712)
- Helm support for LocalKubernetesExecutor (#22388)
- Add ``securityContext`` config for Redis to Helm chart (#22182)
- Allow ``annotations`` on Helm DAG PVC (#22261)
- enable optional ``subPath`` for DAGs volume mount (#22323)
- Added support to override ``auth_type`` in ``auth_file`` in PgBouncer Helm configuration (#21999)
- Add ``extraVolumeMounts`` to Flower (#22414)
- Add webserver ``PodDisruptionBudget`` (#21735)

Improvements
^^^^^^^^^^^^

- Ensure the messages from migration job show up early (#23479)
- Allow migration jobs and init containers to be optional (#22195)
- Use jobs check command for liveness probe check in Airflow 2 (#22143)

Doc only changes
^^^^^^^^^^^^^^^^

- Adds ``resultBackendSecretName`` warning in Helm production docs (#23307)

Misc
^^^^

- Update default Airflow version to ``2.3.0`` (#23386)
- Move the database configuration to a new section (#22284)
- Disable flower in chart by default (#23737)


Airflow Helm Chart 1.5.0, (2022-03-07)
--------------------------------------

Significant changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.2.4``
"""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.2.4``, previously it was ``2.2.3``.

Removed ``config.api``
""""""""""""""""""""""

This section configured the authentication backend for the Airflow API but used the same values as the Airflow default setting, which made it unnecessary to
declare the same again.

New Features
^^^^^^^^^^^^

- Add support for custom command and args in jobs (#20864)
- Support for ``priorityClassName`` (#20794)
- Add ``envFrom`` to the Flower deployment (#21401)
- Add annotations to cleanup pods (#21484)

Improvements
^^^^^^^^^^^^

- Speedup liveness probe for scheduler and triggerer (#20833, #21108)
- Update git-sync to v3.4.0 (#21309)
- Remove default auth backend setting (#21640)

Bug Fixes
^^^^^^^^^

- Fix elasticsearch URL when username/password are empty (#21222)
- Mount ``airflow.cfg`` in wait-for-airflow-migrations containers (#20609)
- Grant pod log reader to triggerer ServiceAccount (#21111)

Doc only changes
^^^^^^^^^^^^^^^^

- Simplify chart docs for configuring Airflow (#21747)
- Add extra information about time synchronization needed (#21685)
- Fix extra containers docs (#20787)

Misc
^^^^

- Use ``2.2.4`` as default Airflow version (#21745)
- Change Redis image to bullseye (#21875)

Airflow Helm Chart 1.4.0, (2022-01-10)
--------------------------------------

Significant changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.2.3``
"""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.2.3``, previously it was ``2.2.1``.

``ingress.web.hosts`` and ``ingress.flower.hosts`` parameters data type has changed and ``ingress.web.tls`` and ``ingress.flower.tls`` have moved
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``ingress.web.hosts`` and ``ingress.flower.hosts`` have had their types have been changed from an array of strings to an array of objects. ``ingress.web.tls`` and ``ingress.flower.tls`` can now be specified per host in ``ingress.web.hosts`` and ``ingress.flower.hosts`` respectively.

The old parameter names will continue to work, however support for them will be removed in a future release so please update your values file.

Fixed precedence of ``nodeSelector``, ``affinity`` and ``tolerations`` params
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``nodeSelector``, ``affinity`` and ``tolerations`` params precedence has been fixed on all components. Now component-specific params
(e.g. ``webserver.affinity``) takes precedence over the global param (e.g. ``affinity``).

Default ``KubernetesExecutor`` worker affinity removed
""""""""""""""""""""""""""""""""""""""""""""""""""""""

Previously a default affinity was added to ``KubernetesExecutor`` workers to spread the workers out across nodes. This default affinity is no
longer set because, in general, there is no reason to spread task-specific workers across nodes.

Changes in webserver and flower ``NetworkPolicy`` default ports
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The defaults for ``webserver.networkPolicy.ingress.ports`` and ``flower.networkPolicy.ingress.ports`` moved away from using named ports to numerical ports to avoid issues with OpenShift.

Increase default ``livenessProbe`` ``timeoutSeconds`` for scheduler and triggerer
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default timeout for the scheduler and triggerer ``livenessProbe`` has been increased from 10 seconds to 20 seconds.

New Features
^^^^^^^^^^^^

- Add ``type`` to extra secrets param (#20599)
- Support elasticsearch connection ``scheme`` (#20564)
- Allows to disable built-in secret variables individually (#18974)
- Add support for ``securityContext`` (#18249)
- Add extra containers, volumes and volume mounts for jobs (#18808)
- Allow ingress multiple hostnames w/diff secrets (#18542)
- PgBouncer extra volumes, volume mounts, and ``sslmode`` (#19749)
- Allow specifying kerberos keytab (#19054)
- Allow disabling the Helm hooks (#18776, #20018)
- Add ``migration-wait-timeout`` (#20069)

Improvements
^^^^^^^^^^^^

- Increase default ``livenessProbe`` timeout (#20698)
- Strict schema for k8s objects for values.yaml (#19181)
- Remove unnecessary ``pod_template_file`` defaults (#19690)
- Use built-in ``check-migrations`` command for Airflow>=2 (#19676)

Bug Fixes
^^^^^^^^^

- Fix precedence of ``affinity``, ``nodeSelector``, and ``tolerations`` (#20641)
- Fix chart elasticsearch default port 80 to 9200. (#20616)
- Fix network policy issue for webserver and flower ui (#20199)
- Use local definitions for k8s schema validation (#20544)
- Add custom labels for ingresses/PVCs (#20535)
- Fix extra secrets/configmaps labels (#20464)
- Fix flower restarts on update (#20316)
- Properly quote namespace names (#20266)

Doc only changes
^^^^^^^^^^^^^^^^

- Add ``helm dependency update`` step to chart INSTALL (#20702)
- Reword section covering the envvar secrets (#20566)
- Add "Customizing Workers" page (#20331)
- Include Datadog example in production guide (#17996)
- Update production Helm guide database section to use k8s secret (#19892)
- Fix ``multiNamespaceMode`` docs to also cover KPO (#19879)
- Clarify Helm behaviour when it comes to loading default connections (#19708)

Misc
^^^^

- Use ``2.2.3`` as default Airflow version (#20450)
- Add ArtifactHUB annotations for docs and screenshots (#20558)
- Add kubernetes 1.21 support (#19557)

Airflow Helm Chart 1.3.0 (2021-11-08)
-------------------------------------

Significant changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.2.1``
"""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.2.1`` (which is Python ``3.7``), previously it was ``2.1.4`` (which is Python ``3.6``).

The triggerer component requires Python ``3.7``. If you require Python ``3.6`` and Airflow ``2.2.0`` or later, use a ``3.6`` based image and set ``triggerer.enabled=False`` in your values.

Resources made configurable for ``airflow-run-airflow-migrations`` job
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Now it's possible to set resources requests and limits for migration job through ``migrateDatabaseJob.resources`` value.

New Features
^^^^^^^^^^^^

- Chart: Add resources for ``cleanup`` and ``createuser`` jobs (#19263)
- Chart: Add labels to jobs created by cleanup pods (#19225)
- Add migration job resources (#19175)
- Allow custom pod annotations to all components (#18481)
- Chart: Make PgBouncer cmd/args configurable (#18910)
- Chart: Use python 3.7 by default; support disabling triggerer (#18920)

Improvements
^^^^^^^^^^^^

- Chart: Increase default liveness probe timeout (#19003)
- Chart: Mount DAGs in triggerer (#18753)

Bug Fixes
^^^^^^^^^

- Allow Airflow UI to create worker pod via Clear > Run (#18272)
- Allow Airflow standard images to run in OpenShift utilizing the official Helm chart #18136 (#18147)

Doc only changes
^^^^^^^^^^^^^^^^

- Chart: Fix ``extraEnvFrom`` examples (#19144)
- Chart docs: Update webserver secret key reference configuration (#18595)
- Fix helm chart links in source install guide (#18588)

Misc
^^^^

- Chart: Update default Airflow version to ``2.2.1`` (#19326)
- Modernize dockerfiles builds (#19327)
- Chart: Use strict k8s schemas for template validation (#19379)

Airflow Helm Chart 1.2.0 (2021-09-28)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``ingress.web.host`` and ``ingress.flower.host`` parameters have been renamed and data type changed
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``ingress.web.host`` and ``ingress.flower.host`` parameters have been renamed to ``ingress.web.hosts`` and ``ingress.flower.hosts``, respectively. Their types have been changed from a string to an array of strings.

The old parameter names will continue to work, however support for them will be removed in a future release so please update your values file.

Default Airflow version is updated to ``2.1.4``
"""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow version that is installed with the Chart is now ``2.1.4``, previously it was ``2.1.2``.

Removed ``ingress.flower.precedingPaths`` and ``ingress.flower.succeedingPaths`` parameters
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``ingress.flower.precedingPaths`` and ``ingress.flower.succeedingPaths`` parameters have been removed as they had previously had no effect on rendered YAML output.

Change of default ``path`` on Ingress
"""""""""""""""""""""""""""""""""""""

With the move to support the stable Kubernetes Ingress API the default path has been changed from being unset to ``/``. For most Ingress controllers this should not change the behavior of the resulting Ingress resource.

New Features
^^^^^^^^^^^^

- Add Triggerer to Helm Chart (#17743)
- Chart: warn when webserver secret key isn't set (#18306)
- add ``extraContainers`` for ``migrateDatabaseJob`` (#18379)
- Labels on job templates (#18403)
- Chart: Allow running and waiting for DB Migrations using default image (#18218)
- Chart: Make cleanup cronjob cmd/args configurable (#17970)
- Chart: configurable number of retention days for log groomers (#17764)
- Chart: Add ``loadBalancerSourceRanges`` in webserver and flower services (#17666)
- Chart: Support ``extraContainers`` in k8s workers (#17562)


Improvements
^^^^^^^^^^^^

- Switch to latest version of PGBouncer-Exporter (#18429)
- Chart: Ability to access http k8s via multiple hostnames (#18257)
- Chart: Use stable API versions where available (#17211)
- Chart: Allow ``podTemplate`` to be templated (#17560)

Bug Fixes
^^^^^^^^^

- Chart: Fix applying ``labels`` on Triggerer (#18299)
- Fixes warm shutdown for celery worker. (#18068)
- Chart: Fix minor Triggerer issues (#18105)
- Chart: fix webserver secret key update (#18079)
- Chart: fix running with ``uid`` ``0`` (#17688)
- Chart: use ServiceAccount template for log reader RoleBinding (#17645)
- Chart: Fix elasticsearch-secret template port default function (#17428)
- KEDA task count query should ignore k8s queue (#17433)

Doc only changes
^^^^^^^^^^^^^^^^

- Chart Doc: Delete extra space in adding connections doc (#18424)
- Improves installing from sources pages for all components (#18251)
- Chart docs: Format ``loadBalancerSourceRanges`` using code-block (#17763)
- Doc: Fix a broken link in an ssh-related warning message (#17294)
- Chart: Add instructions to Update Helm Repo before upgrade (#17282)
- Chart docs: better note for logs existing PVC permissions (#17177)

Misc
^^^^

- Chart: Update the default Airflow version to ``2.1.4`` (#18354)

Airflow Helm Chart 1.1.0 (2021-07-26)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Run ``helm repo update`` before upgrading the chart to the latest version.

Default Airflow version is updated to ``2.1.2``
"""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow version that is installed with the Chart is now ``2.1.2``, previously it was ``2.0.2``.

Helm 2 no longer supported
""""""""""""""""""""""""""

This chart has dropped support for `Helm 2 as it has been deprecated <https://helm.sh/blog/helm-v2-deprecation-timeline/>`__ and no longer receiving security updates since November 2020.

``webserver.extraNetworkPolicies`` and ``flower.extraNetworkPolicies`` parameters have been renamed
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``webserver.extraNetworkPolicies`` and ``flower.extraNetworkPolicies`` have been renamed to ``webserver.networkPolicy.ingress.from`` and ``flower.networkPolicy.ingress.from``, respectively. Their values and behavior are the same.

The old parameter names will continue to work, however support for them will be removed in a future release so please update your values file.

Removed ``dags.gitSync.root``, ``dags.gitSync.dest``, and ``dags.gitSync.excludeWebserver`` parameters
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``dags.gitSync.root`` and ``dags.gitSync.dest`` parameters did not provide any useful behaviors to chart users so they have been removed.
If you have them set in your values file you can safely remove them.

The ``dags.gitSync.excludeWebserver`` parameter was mistakenly included in the charts ``values.schema.json``. If you have it set in your values file,
you can safely remove it.

``nodeSelector``, ``affinity`` and ``tolerations`` on ``migrateDatabaseJob`` and ``createUserJob`` jobs
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The ``migrateDatabaseJob`` and ``createUserJob`` jobs were incorrectly using the ``webserver``'s ``nodeSelector``, ``affinity``
and ``tolerations`` (if set). Each job is now configured separately.

New Features
^^^^^^^^^^^^

- Chart: Allow using ``krb5.conf`` with ``CeleryExecutor`` (#16822)
- Chart: Refactor webserver and flower NetworkPolicy (#16619)
- Chart: Apply worker's node assigning settings to Pod Template File (#16663)
- Chart: Support for overriding webserver and flower service ports (#16572)
- Chart: Support ``extraContainers`` and ``extraVolumes`` in flower (#16515)
- Chart: Allow configuration of pod resources in helm chart (#16425)
- Chart: Support job level annotations; fix jobs scheduling config (#16331)
- feat: Helm chart adding ``minReplicaCount`` to the KEDA ``worker-kedaautoscaler.yaml`` (#16262)
- Chart: Adds support for custom command and args (#16153)
- Chart: Add extra ini config to ``pgbouncer`` (#16120)
- Chart: Add ``extraInitContainers`` to scheduler/webserver/workers (#16098)
- Configurable resources for git-sync sidecar (#16080)
- Chart: Template ``airflowLocalSettings`` and ``webserver.webserverConfig`` (#16074)
- Support ``strategy``/``updateStrategy`` on scheduler (#16069)
- Chart: Add both airflow and extra annotations to jobs (#16058)
- ``loadBalancerIP`` and ``annotations`` for both Flower and Webserver (#15972)

Improvements
^^^^^^^^^^^^

- Chart: Update Postgres subchart to 10.5.3 (#17041)
- Chart: Update the default Airflow version to ``2.1.2`` (#17013)
- Update default image as ``2.1.1`` for Helm Chart (#16785)
- Chart: warn when using default logging with ``KubernetesExecutor`` (#16784)
- Drop support for Helm 2 (#16575)
- Chart: ``podAntiAffinity`` for scheduler, webserver, and workers (#16315)
- Chart: Update the default Airflow Version to ``2.1.0`` (#16273)
- Chart: Only mount DAGs in webserver when required (#16229)
- Chart: Remove ``git-sync``: ``root`` and ``dest`` params (#15955)
- Chart: Add warning about missing ``knownHosts`` (#15950)

Bug Fixes
^^^^^^^^^

- Chart: Create a random secret for Webserver's flask secret key (#17142)
- Chart: fix labels on cleanup ServiceAccount (#16722)
- Chart: Fix overriding node assigning settings on Worker Deployment (#16670)
- Chart: Always deploy a ``gitsync`` init container (#16339)
- Chart: Fix updating from ``KubernetesExecutor`` to ``CeleryExecutor`` (#16242)
- Chart: Adds labels to Kubernetes worker pods (#16203)
- Chart: Allow ``webserver.base_url`` to be templated (#16126)
- Chart: Fix ``PgBouncer`` exporter sidecar (#16099)
- Remove ``dags.gitSync.excludeWebserver`` from chart ``values.schema.json`` (#16070)
- Chart: Fix Elasticsearch secret created without Elasticsearch enabled (#16015)
- Handle special characters in passwords for Helm Chart (#16004)
- Fix flower ServiceAccount created without flower enable (#16011)
- Chart: ``gitsync`` Clean Up for ``KubernetesExecutor``  (#15925)
- Mount DAGs read only when using ``gitsync`` (#15953)

Doc only changes
^^^^^^^^^^^^^^^^

- Chart docs: note uid write permissions for existing PVC (#17170)
- Chart Docs: Add single-line description for ``multiNamespaceMode`` (#17147)
- Chart: Update description for Helm chart to include 'official' (#17040)
- Chart: Better comment and example for ``podTemplate`` (#16859)
- Chart: Add more clear docs for setting ``pod_template_file.yaml`` (#16632)
- Fix description on ``scheduler.livenessprobe.periodSeconds`` (#16486)
- Chart docs: Fix ``extrasecrets`` example (#16305)
- Small improvements for ``README.md`` files (#16244)

Misc
^^^^

- Removes pylint from our toolchain (#16682)
- Update link to match what is in pre-commit (#16408)
- Chart: Update the ``appVersion`` to 2.1.0 in ``Chart.yaml`` (#16337)
- Rename the main branch of the Airflow repo to be ``main`` (#16149)
- Update Chart version to ``1.1.0-rc1`` (#16124)
