
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

``apache-airflow-providers-openfeature``
========================================

`OpenFeature <https://openfeature.dev>`__ is a CNCF, vendor-neutral API for feature-flag
evaluation. A *provider* adapts it to a concrete backend (flagd, GrowthBook, Unleash, an in-house
system, ...), so the same DAG code evaluates flags against any of them.

This provider gives Airflow:

- :class:`~airflow.providers.openfeature.hooks.openfeature.OpenFeatureHook` — register a provider
  from a connection and evaluate boolean/string flags.
- :class:`~airflow.providers.openfeature.sensors.feature_flag.FeatureFlagSensor` — wait until a flag
  is enabled for a targeting entity.
- :class:`~airflow.providers.openfeature.providers.fractional.FractionalProvider` — a dependency-free
  in-process provider for deterministic percentage rollouts, so you can canary without running a flag
  daemon.

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/openfeature/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-openfeature/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Progressive delivery of the platform (cluster policy)
-----------------------------------------------------

Beyond gating a single task, the hook can back an Airflow cluster policy to roll a *platform* change
out to a cohort with no core change. Register a provider and consult it from ``task_policy`` in
``airflow_local_settings.py`` to flip a cohort's ``pool``/``queue`` and ramp it like a canary:

.. code-block:: python

    # airflow_local_settings.py
    from openfeature import api

    from airflow.providers.openfeature.hooks.openfeature import OpenFeatureHook
    from airflow.providers.openfeature.providers.fractional import BoolFlag, FractionalProvider

    api.set_provider(FractionalProvider(bool_flags={"airflow.task.canary": BoolFlag(rollout_pct=5)}))


    def task_policy(task):
        hook = OpenFeatureHook()
        if hook.is_enabled("airflow.task.canary", entity=f"{task.dag_id}:{task.task_id}"):
            task.pool = "canary_pool"
            task.queue = "canary"
