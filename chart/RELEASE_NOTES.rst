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

Airflow Helm Chart 1.15.0 (2024-07-24)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.9.3`` (#40816)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.9.3``, previously it was ``2.9.2``.

Default PgBouncer Exporter image has been updated (#40318)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The PgBouncer Exporter image has been updated to ``airflow-pgbouncer-exporter-2024.06.18-0.17.0``, which addresses CVE-2024-24786.

New Features
^^^^^^^^^^^^

- Add git-sync container lifecycle hooks (#40369)
- Add init containers for jobs (#40454)
- Add persistent volume claim retention policy (#40271)
- Add annotations for Redis StatefulSet (#40281)
- Add ``dags.gitSync.sshKey``, which allows the git-sync private key to be configured in the values file directly (#39936)
- Add ``extraEnvFrom`` to git-sync containers (#39031)

Improvements
^^^^^^^^^^^^

- Link in ``UIAlert`` to production guide when a dynamic webserver secret is used now opens in a new tab (#40635)
- Support disabling helm hooks on ``extraConfigMaps`` and ``extraSecrets`` (#40294)

Bug Fixes
^^^^^^^^^

- Add git-sync ssh secret to DAG processor (#40691)
- Fix duplicated ``safeToEvict`` annotations (#40554)
- Add missing ``triggerer.keda.usePgbouncer`` to values.yaml (#40614)
- Trim leading ``//`` character using mysql backend (#40401)

Doc only changes
^^^^^^^^^^^^^^^^

- Updating chart download link to use the Apache download CDN (#40618)

Misc
^^^^

- Update PgBouncer exporter image to ``airflow-pgbouncer-exporter-2024.06.18-0.17.0`` (#40318)
- Default airflow version to 2.9.3 (#40816)
- Fix ``startupProbe`` timing comment (#40412)

Airflow Helm Chart 1.14.0 (2024-06-18)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``ClusterRole`` and ``ClusterRoleBinding`` names have been updated to be unique (#37197)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

``ClusterRole``s and ``ClusterRoleBinding``s created when ``multiNamespaceMode`` is enabled have been renamed to ensure unique names:

  * ``{{ include "airflow.fullname" . }}-pod-launcher-role`` has been renamed to ``{{ .Release.Namespace }}-{{ include "airflow.fullname" . }}-pod-launcher-role``
  * ``{{ include "airflow.fullname" . }}-pod-launcher-rolebinding`` has been renamed to ``{{ .Release.Namespace }}-{{ include "airflow.fullname" . }}-pod-launcher-rolebinding``
  * ``{{ include "airflow.fullname" . }}-pod-log-reader-role`` has been renamed to ``{{ .Release.Namespace }}-{{ include "airflow.fullname" . }}-pod-log-reader-role``
  * ``{{ include "airflow.fullname" . }}-pod-log-reader-rolebinding`` has been renamed to ``{{ .Release.Namespace }}-{{ include "airflow.fullname" . }}-pod-log-reader-rolebinding``
  * ``{{ include "airflow.fullname" . }}-scc-rolebinding`` has been renamed to ``{{ .Release.Namespace }}-{{ include "airflow.fullname" . }}-scc-rolebinding``

``workers.safeToEvict`` default changed to False (#40229)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default for ``workers.safeToEvict`` now defaults to False. This is a safer default
as it prevents the nodes workers are running on from being scaled down by the
`K8s Cluster Autoscaler <https://kubernetes.io/docs/concepts/cluster-administration/cluster-autoscaling/#cluster-autoscaler>`_.
If you would like to retain the previous behavior, you can set this config to True.

Default Airflow image is updated to ``2.9.2`` (#40160)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.9.2``, previously it was ``2.8.3``.

Default StatsD image is updated to ``v0.26.1`` (#38416)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default StatsD image that is used with the Chart is now ``v0.26.1``, previously it was ``v0.26.0``.

New Features
^^^^^^^^^^^^

- Enable MySQL KEDA support for triggerer (#37365)
- Allow AWS Executors (#38524)

Improvements
^^^^^^^^^^^^

- Allow ``valueFrom`` in env config of components (#40135)
- Enable templating in ``extraContainers`` and ``extraInitContainers`` (#38507)
- Add safe-to-evict annotation to pod-template-file (#37352)
- Support ``workers.command`` for KubernetesExecutor (#39132)
- Add ``priorityClassName`` to Jobs (#39133)
- Add Kerberos sidecar to pod-template-file (#38815)
- Add templated field support for extra containers (#38510)

Bug Fixes
^^^^^^^^^

- Set ``workers.safeToEvict`` default to False (#40229)

Doc only changes
^^^^^^^^^^^^^^^^

- Document ``extraContainers`` and ``extraInitContainers`` that are templated (#40033)
- Fix typo in HorizontalPodAutoscaling documentation (#39307)
- Fix supported k8s versions in docs (#39172)
- Fix typo in YAML path for ``brokerUrlSecretName`` (#39115)

Misc
^^^^
- Default Airflow version to 2.9.2 (#40160)
- Limit Redis image to 7.2 (#38928)
- Build Helm values schemas with Kubernetes 1.29 resources (#38460)
- Add missing containers to resources docs (#38534)
- Upgrade StatsD Exporter image to 0.26.1 (#38416)
- Remove K8S 1.25 support (#38367)

Airflow Helm Chart 1.13.1 (2024-03-25)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.8.3`` (#38036)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.8.3``, previously it was ``2.8.2``.

Bug Fixes
^^^^^^^^^
- Don't overwrite ``.Values.airflowPodAnnotations`` (#37917)
- Fix cluster-wide RBAC naming clash when using multiple ``multiNamespace`` releases with the same name (#37197)

Misc
^^^^
- Chart: Default airflow version to 2.8.3 (#38036)

Airflow Helm Chart 1.13.0 (2024-03-05)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.8.2`` (#37704)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.8.2``, previously it was ``2.8.1``.


New Features
^^^^^^^^^^^^

- Support labels specific to the database migration objects and pods (#37490)

Improvements
^^^^^^^^^^^^

- Flower K8s Probe config (#37528)

Bug Fixes
^^^^^^^^^
- Remove duplicate ports key in webserver service (#37356)
- Add ``AIRFLOW_HOME`` env var to log groomer sidecar (#37588)
- Skip ``.`` path when preparing reproducible packages (#37402)

Misc
^^^^
- Default airflow version to 2.8.2 (#37704)

Airflow Helm Chart 1.12.0 (2024-02-11)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

The helm chart is now using a newer version of ``bitnami/postgresql`` dependency (#34817)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The version of ``bitnami/postgresql`` subchart upgraded from ``12.10.0`` to ``13.2.24``.
The version of ``PostgreSQL`` binaries upgraded from ``11`` to ``16.1.0``.

The change requires existing ``bitnami/postgresql`` subchart users to perform manual major version upgrade using ``pg_dumpall`` or ``pg_upgrade``.

As a reminder, it is recommended to `set up an external database <https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#database>`_ in production.

Default Airflow image is updated to ``2.8.1`` (#36907)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.8.1``, previously it was ``2.7.1``.

Default PgBouncer and PgBouncer Exporter images have been updated (#36898)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The PgBouncer and PgBouncer Exporter images are based on newer software/os.

  * ``pgbouncer``: 1.21.0 based on alpine 3.14 (``airflow-pgbouncer-2024.01.19-1.21.0``)
  * ``pgbouncer-exporter``: 0.16.0 based on alpine 3.19 (``apache/airflow:airflow-pgbouncer-exporter-2024.01.19-0.16.0``)

Default StatsD image is updated to ``v0.26.0`` (#37187)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default StatsD image that is used with the Chart is now ``v0.26.0``, previously it was ``v0.22.8``.

Default Redis image is updated to ``7-bookworm`` (#37187)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Redis image that is used with the Chart is now ``7-bookworm``, previously it was ``7-bullseye``.

New Features
^^^^^^^^^^^^

- Enable native HPA for Airflow Workers (#36174)
- Add init container + sidecar support for Airflow Kerberos (#35548)
- Support MySQL backend as KEDA trigger (#36167)

Improvements
^^^^^^^^^^^^

- Improve PriorityClass to improve debuggability (#36365)
- Add ``securityContexts`` in dag processors log groomer sidecar (#34499)
- Add support for ``securityContexts`` in dag processors wait-for-migrations container (#35593)
- Add templating for PVC ``storageClassName`` (#35581)
- Add ``volumeClaimTemplate`` for worker (#34986)
- Add support for ``priorityClassName`` on Redis pods (#34879)
- Configurable mount path for DAGs volume (#35083)
- Add support for custom ``emptyDir`` config (#34837)
- Added ability to enable/disable scheduler and webserver  (#36991)

Bug Fixes
^^^^^^^^^

- Fix StatsD host in Airflow config (#35679)
- Set ``AIRFLOW_HOME`` env var with ``airflowHome`` value (#34839)
- Safer worker pod annotations (#35309)
- Set worker ``safeToEvict`` properly (#35130)
- Fix Redis broker URL with ``useStandardNaming`` (#34825)
- Fix metadata DB & port in KEDA connection when ``usePgbouncer`` is false (#34741)
- Fix PgBouncer connection with ``useStandardNaming`` (#34787)

Doc only changes
^^^^^^^^^^^^^^^^

- Add docs about extending the Airflow Helm chart (#36331)
- Add comment for Elasticsearch connection scheme (#35588)
- Add notes about Virtualenvs preventing the need for custom images (#35306)

Misc
^^^^

- Default Airflow version to 2.8.1 (#36907)
- Support git-sync v4 (#34731)
- Upgrade ``bitnami/postgresql`` subchart to ``13.2.24`` (#36156)
- Change git sync container indent to 4 (#35824)
- Remove K8S 1.24 support (#35214)
- Rebuild ``pgbouncer`` and ``pgbouncer-exporter`` images with newer versions (#36898)
- Update ``statsd`` and ``redis`` chart images (#37187)

Airflow Helm Chart 1.11.0 (2023-10-02)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Support naming customization on helm chart resources, some resources may be renamed during upgrade (#31066)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This is a new opt-in switch ``useStandardNaming``, for backwards compatibility, to leverage the standard naming convention, which allows full use of ``fullnameOverride`` and ``nameOverride`` in all resources.

The following resources will be renamed using default of ``useStandardNaming=false`` when upgrading to 1.11.0 or a higher version.

- ConfigMap ``{release}-airflow-config`` to ``{release}-config``
- Secret ``{release}-airflow-metadata`` to ``{release}-metadata``
- Secret ``{release}-airflow-result-backend`` to ``{release}-result-backend``
- Ingress ``{release}-airflow-ingress`` to ``{release}-ingress``

For existing installations, all your resources will be recreated with a new name and Helm will delete the previous resources.

This won't delete existing PVCs for logs used by StatefulSet/Deployments, but it will recreate them with brand new PVCs.
If you do want to preserve logs history you'll need to manually copy the data of these volumes into the new volumes after
deployment. Depending on what storage backend/class you're using this procedure may vary. If you don't mind starting
with fresh logs/redis volumes, you can just delete the old PVCs that will be names, for example:

.. code-block:: bash

    kubectl delete pvc -n airflow logs-gta-triggerer-0
    kubectl delete pvc -n airflow logs-gta-worker-0
    kubectl delete pvc -n airflow redis-db-gta-redis-0

If you do not change ``useStandardNaming`` or ``fullnameOverride`` after upgrade, you can proceed as usual and no unexpected behaviours will be presented.

``bitnami/postgresql`` subchart updated to ``12.10.0`` (#33747)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The PostgreSQL subchart that is used with the Chart is now ``12.10.0``, previously it was ``12.1.9``.

Default git-sync image is updated to ``3.6.9`` (#33748)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default git-sync image that is used with the Chart is now ``3.6.9``, previously it was ``3.6.3``.

Default Airflow image is updated to ``2.7.1`` (#34186)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.7.1``, previously it was ``2.6.2``.

New Features
^^^^^^^^^^^^

- Add support for scheduler name to PODs templates (#33843)
- Support KEDA scaling for triggerer (#32302)
- Add support for container lifecycle hooks (#32349, #34677)
- Support naming customization on helm chart resources (#31066)
- Adding ``startupProbe`` to scheduler and webserver (#33107)
- Allow disabling token mounts using ``automountServiceAccountToken`` (#32808)
- Add support for defining custom priority classes (#31615)
- Add support for ``runtimeClassName`` (#31868)
- Add support for custom query in workers KEDA trigger (#32308)

Improvements
^^^^^^^^^^^^

- Add ``containerSecurityContext`` for cleanup job (#34351)
- Add existing secret support for PGBouncer metrics exporter (#32724)
- Allow templating in webserver ingress hostnames (#33142)
- Allow templating in flower ingress hostnames (#33363)
- Add configmap annotations to StatsD and webserver (#33340)
- Add pod security context to PgBouncer (#32662)
- Add an option to use a direct DB connection in KEDA when PgBouncer is enabled (#32608)
- Allow templating in cleanup.schedule (#32570)
- Template dag processor ``waitformigration`` containers ``extraVolumeMounts`` (#32100)
- Ability to inject extra containers into PgBouncer (#33686)
- Allowing ability to add custom env into PgBouncer container (#33438)
- Add support for env variables in the StatsD container (#33175)

Bug Fixes
^^^^^^^^^

- Add ``airflow db migrate`` command to database migration job (#34178)
- Pass ``workers.terminationGracePeriodSeconds`` into KubeExecutor pod template (#33514)
- CeleryExecutor namespace depends on Airflow version (#32753)
- Fix dag processor not including webserver config volume (#32644)
- Dag processor liveness probe include ``--local`` and ``--job-type`` args (#32426)
- Revising flower_url_prefix considering default value (#33134)

Doc only changes
^^^^^^^^^^^^^^^^

- Add more explicit "embedded postgres" exclusion for production (#33034)
- Update git-sync description (#32181)

Misc
^^^^

- Default Airflow version to 2.7.1 (#34186)
- Update PostgreSQL subchart to 12.10.0 (#33747)
- Update git-sync to 3.6.9 (#33748)
- Remove unnecessary loops to load env from helm values (#33506)
- Replace ``common.tplvalues.render`` with ``tpl`` in ingress template files (#33384)
- Remove K8S 1.23 support (#32899)
- Fix chart named template comments (#32681)
- Remove outdated comment from chart values in the workers KEDA conf section (#32300)
- Remove unnecessary ``or`` function in template files (#34415)

Airflow Helm Chart 1.10.0 (2023-06-26)
--------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.6.2`` (#31979)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.6.2``, previously it was ``2.5.3``.

New Features
^^^^^^^^^^^^

- Add support for container security context (#31043)

Improvements
^^^^^^^^^^^^

- Validate ``executor`` and ``config.core.executor`` match (#30693)
- Support ``minAvailable`` property for PodDisruptionBudget (#30603)
- Add ``volumeMounts`` to dag processor ``waitForMigrations`` (#30990)
- Template extra volumes (#30773)

Bug Fixes
^^^^^^^^^

- Fix webserver probes timeout and period (#30609)
- Add missing ``waitForMigrations`` for workers (#31625)
- Add missing ``priorityClassName`` to K8S worker pod template (#31328)
- Adding log groomer sidecar to dag processor (#30726)
- Do not propagate global security context to statsd and redis (#31865)

Misc
^^^^

- Default Airflow version to 2.6.2 (#31979)
- Use template comments for the chart license header (#30569)
- Align ``apiVersion`` and ``kind`` order in chart templates (#31850)
- Cleanup Kubernetes < 1.23 support (#31847)

Airflow Helm Chart 1.9.0 (2023-04-14)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default PgBouncer and PgBouncer Exporter images have been updated (#29919)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The PgBouncer and PgBouncer Exporter images are based on newer software/os. They are also multi-platform AMD/ARM images:

  * ``pgbouncer``: 1.16.1 based on alpine 3.14 (``airflow-pgbouncer-2023.02.24-1.16.1``)
  * ``pgbouncer-exporter``: 0.14.0 based on alpine 3.17 (``apache/airflow:airflow-pgbouncer-exporter-2023.02.21-0.14.0``)

Default Airflow image is updated to ``2.5.3`` (#30411)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.5.3``, previously it was ``2.5.1``.

New Features
^^^^^^^^^^^^

- Add support for ``hostAliases`` for Airflow webserver and scheduler (#30051)
- Add support for annotations on StatsD Deployment and cleanup CronJob (#30126)
- Add support for annotations in logs PVC (#29270)
- Add support for annotations in extra ConfigMap and Secrets (#30303)
- Add support for pod annotations to PgBouncer (#30168)
- Add support for ``ttlSecondsAfterFinished`` on ``migrateDatabaseJob`` and ``createUserJob`` (#29314)
- Add support for using SHA digest of Docker images (#30214)

Improvements
^^^^^^^^^^^^

- Template extra volumes in Helm Chart (#29357)
- Make Liveness/Readiness Probe timeouts configurable for PgBouncer Exporter (#29752)
- Enable individual trigger logging (#29482)

Bug Fixes
^^^^^^^^^

- Add ``config.kubernetes_executor`` to values (#29818)
- Block extra properties in image config (#30217)
- Remove replicas if KEDA is enabled (#29838)
- Mount ``kerberos.keytab`` to worker when enabled (#29526)
- Fix adding annotations for dag persistence PVC (#29622)
- Fix ``bitnami/postgresql`` default username and password (#29478)
- Add global volumes in pod template file (#29295)
- Add log groomer sidecar to triggerer service (#29392)
- Helm deployment fails when ``postgresql.nameOverride`` is used (#29214)

Doc only changes
^^^^^^^^^^^^^^^^

- Add gitSync optional env description (#29378)
- Add webserver NodePort example (#29460)
- Include Rancher in Helm chart install instructions (#28416)
- Change RSA SSH host key to reflect update from Github (#30286)

Misc
^^^^

- Update Airflow version to 2.5.3 (#30411)
- Switch to newer versions of PgBouncer and PgBouncer Exporter in chart (#29919)
- Reformat chart templates (#29917)
- Reformat chart templates part 2 (#29941)
- Reformat chart templates part 3 (#30312)
- Replace deprecated k8s registry references (#29938)
- Fix ``airflow_dags_mount`` formatting (#29296)
- Fix ``webserver.service.ports`` formatting (#29297)

Airflow Helm Chart 1.8.0 (2023-02-06)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

``bitnami/postgresql`` subchart updated to ``12.1.9`` (#29071)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The version of postgresql installed is still version 11.

If you are upgrading an existing helm release with the built-in postgres database, you will either need to delete your release and reinstall fresh, or manually delete these 2 objects:

.. code-block::

    kubectl delete secret {RELEASE_NAME}-postgresql
    kubectl delete statefulset {RELEASE_NAME}-postgresql

As a reminder, it is recommended to `set up an external database <https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#database>`_ in production.

This version of the chart uses different variable names for setting usernames and passwords in the postgres database.

- ``postgresql.auth.enablePostgresUser`` is used to determine if the "postgres" admin account will be created.
- ``postgresql.auth.postgresPassword`` sets the password for the "postgres" user.
- ``postgresql.auth.username`` and ``postrgesql.auth.password`` are used to set credentials for a non-admin account if desired.
- ``postgresql.postgresqlUsername`` and ``postgresql.postresqlPassword``, which were used in the previous version of the chart, are no longer used.

Users will need to make those changes in their values files if they are changing the Postgres configuration.

Previously the subchart version was ``10.5.3``.

Default ``dags.gitSync.wait`` reduced to ``5`` seconds (#27625)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default for ``dags.gitSync.wait`` has been reduced from ``60`` seconds to ``5`` seconds to reduce the likelihood of DAGs
becoming inconsistent between Airflow components. This will, however, increase traffic to the remote git repository.

Default Airflow image is updated to ``2.5.1`` (#29074)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.5.1``, previously it was ``2.4.1``.

Default git-sync image is updated to ``3.6.3`` (#27848)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default git-sync image that is used with the Chart is now ``3.6.3``, previously it was ``3.4.0``.

Default redis image is updated to ``7-bullseye`` (#27443)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default redis image that is used with the Chart is now ``7-bullseye``, previously it was ``6-bullseye``.

New Features
^^^^^^^^^^^^

- Add annotations on deployments (#28688)
- Add global volume & volumeMounts to the chart (#27781)

Improvements
^^^^^^^^^^^^

- Add support for ``webserverConfigConfigMapName`` (#27419)
- Enhance chart to allow overriding command-line args to statsd exporter (#28041)
- Add support for NodePort in Services (#26945)
- Add worker log-groomer-sidecar enable option (#27178)
- Add HostAliases to Pod template file (#27544)
- Allow PgBouncer replicas to be configurable (#27439)

Bug Fixes
^^^^^^^^^

- Create scheduler service to serve task logs for LocalKubernetesExecutor (#28828)
- Fix NOTES.txt to show correct URL (#28264)
- Add worker service account for LocalKubernetesExecutor (#28813)
- Remove checks for 1.19 api checks (#28461)
- Add airflow_local_settings to all airflow containers (#27779)
- Make custom env vars optional for job templates (#27148)
- Decrease default gitSync wait (#27625)
- Add ``extraVolumeMounts`` to sidecars too (#27420)
- Fix PgBouncer after PostgreSQL subchart upgrade (#29207)

Doc only changes
^^^^^^^^^^^^^^^^

- Enhance production guide with a few Argo specific guidelines (#29078)
- Add doc note about Pod template images (#29032)
- Update production guide db section (#28610)
- Fix to LoadBalancer snippet (#28014)
- Fix gitSync example code (#28083)
- Correct repo example for cloning via ssh (#27671)

Misc
^^^^

- Update Airflow version to 2.5.1 (#29074)
- Update git-sync to 3.6.3 (#27848)
- Upgrade ``bitnami/postgresql`` subchart to 12.1.9 (#29071)
- Update redis to 7 (#27443)
- Replace helm chart icon (#27704)

Airflow Helm Chart 1.7.0 (2022-10-14)
-------------------------------------

Significant Changes
^^^^^^^^^^^^^^^^^^^

Default Airflow image is updated to ``2.4.1`` (#26485)
""""""""""""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.4.1``, previously it was ``2.3.2``.

New Features
^^^^^^^^^^^^

- Make cleanup job history configurable (#26838)
- Added labels to specific Airflow components (#25031)
- Add StatsD ``overrideMappings`` in Helm chart values (#26598)
- Adding ``podAnnotations`` to StatsD deployment template (#25732)
- Container specific extra environment variables (#24784)
- Custom labels for extra Secrets and ConfigMaps (#25283)
- Add ``revisionHistoryLimit`` to all deployments (#25059)
- Adding ``podAnnotations`` to Redis StatefulSet (#23708)
- Provision Standalone Dag Processor (#23711)
- Add configurable scheme for webserver probes (#22815)
- Add support for KEDA HPA config to Helm chart (#24220)

Improvements
^^^^^^^^^^^^

- Add 'executor' label to Airflow scheduler deployment (#25684)
- Add default ``flower_url_prefix`` in Helm chart values (#26415)
- Add liveness probe to Celery workers (#25561)
- Use ``sql_alchemy_conn`` for celery result backend when ``result_backend`` is not set (#24496)

Bug Fixes
^^^^^^^^^

- Fix pod template ``imagePullPolicy`` (#26423)
- Do not declare a volume for ``sshKeySecret`` if dag persistence is enabled (#22913)
- Pass worker annotations to generated pod template (#24647)
- Fix semver compare number for ``jobs check`` command (#24480)
- Use ``--local`` flag for liveness probes in Airflow 2.5+ (#24999)

Doc only changes
^^^^^^^^^^^^^^^^

- Improve documentation on helm hooks disabling (#26747)
- Remove ``ssh://`` prefix from git repo value (#26632)
- Fix ``defaultAirflowRepository`` comment (#26428)
- Baking DAGs into Docker image (#26401)
- Reload pods when using the same DAG tag (#24576)
- Minor clarifications about ``result_backend``, dag processor, and ``helm uninstall`` (#24929)
- Add hyperlinks to GitHub PRs for Release Notes (#24532)
- Terraform should not use Helm hooks for starting jobs (#26604)
- Flux should not use Helm hooks for starting jobs (#24288)
- Provide details on how to pull Airflow image from a private repository (#24394)
- Helm logo no longer a link (#23977)
- Document LocalKubernetesExecutor support in chart (#23876)
- Update Production Guide (#23836)

Misc
^^^^

- Default Airflow version to 2.4.1 (#26485)
- Vendor in the Bitnami chart (#24395)
- Remove kubernetes 1.20 support (#25871)


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
``flower.enabled`` to true in your values file.

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
