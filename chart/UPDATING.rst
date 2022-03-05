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

Updating the Airflow Helm Chart
===============================

The following documents any backwards-incompatible changes in the Airflow Helm chart and
assists users migrating to a new version.

.. I'm glad you want to write a new note. Remember that this note is intended for users.
   Make sure it contains the following information:

.. - [ ] Previous behaviors
   - [ ] New behaviors
   - [ ] If possible, a simple example of how to migrate. This may include a simple code example.
   - [ ] If possible, the benefit for the user after migration e.g. "we want to make these changes to unify class names."
   - [ ] If possible, the reason for the change, which adds more context to that interested, e.g. reference for Airflow Improvement Proposal.

.. More tips can be found in the guide:
   https://developers.google.com/style/inclusive-documentation

Run ``helm repo update`` before upgrading the chart to the latest version.


Airflow Helm Chart 1.5.0
------------------------

Default Airflow image is updated to ``2.2.4``
"""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.2.4``, previously it was ``2.2.3``.

Removed ``config.api``
""""""""""""""""""""""

This section configured the authentication backend for the Airflow API but used the same values as the Airflow default setting, which made it unnecessary to
declare the same again.

Airflow Helm Chart 1.4.0
------------------------

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

Airflow Helm Chart 1.3.0
------------------------

Default Airflow image is updated to ``2.2.1``
"""""""""""""""""""""""""""""""""""""""""""""

The default Airflow image that is used with the Chart is now ``2.2.1`` (which is Python ``3.7``), previously it was ``2.1.4`` (which is Python ``3.6``).

The triggerer component requires Python ``3.7``. If you require Python ``3.6`` and Airflow ``2.2.0`` or later, use a ``3.6`` based image and set ``triggerer.enabled=False`` in your values.

Resources made configurable for ``airflow-run-airflow-migrations`` job
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Now it's possible to set resources requests and limits for migration job through ``migrateDatabaseJob.resources`` value.

Airflow Helm Chart 1.2.0
------------------------

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

Airflow Helm Chart 1.1.0
------------------------

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
