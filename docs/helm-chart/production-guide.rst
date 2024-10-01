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

Production Guide
================

The following are things to consider when using this Helm chart in a production environment.

Database
--------

It is advised to set up an external database for the Airflow metastore. The default Helm chart deploys a
Postgres database running in a container. For production usage, a database running on a dedicated machine or
leveraging a cloud provider's database service such as AWS RDS should be used because the embedded Postgres
lacks stability, monitoring and persistence features that you need for a production database. It is only there to
make it easier to test the Helm Chart in a "standalone" version but you might experience data loss when you
are using it. Supported databases and versions can be found at :doc:`Set up a Database Backend <apache-airflow:howto/set-up-database>`.


.. note::

    When using the helm chart, you do not need to initialize the db with ``airflow db migrate``
    as outlined in :doc:`Set up a Database Backend <apache-airflow:howto/set-up-database>`.

First disable Postgres so the chart won't deploy its own Postgres container:

.. code-block:: yaml

  postgresql:
    enabled: false

To provide the database credentials to Airflow, you have 2 options - in your values file or in a Kubernetes Secret.

Values file
^^^^^^^^^^^

This is the simpler options, as the chart will create a Kubernetes Secret for you. However, keep in mind your credentials will be in your values file.

.. code-block:: yaml

  data:
    metadataConnection:
      user: <username>
      pass: <password>
      protocol: postgresql
      host: <hostname>
      port: 5432
      db: <database name>


Kubernetes Secret
^^^^^^^^^^^^^^^^^

You can also store the credentials in a Kubernetes Secret you create. Note that
special characters in the username/password must be URL encoded.

.. code-block:: bash

  kubectl create secret generic mydatabase --from-literal=connection=postgresql://user:pass@host:5432/db

Finally, configure the chart to use the secret you created:

.. code-block:: yaml

  data:
    metadataSecretName: mydatabase

.. warning::
  If you use ``CeleryExecutor`` and Airflow version < ``2.4``, keep in mind that ``resultBackendSecretName`` expects a url that starts with ``db+postgresql://``, while ``metadataSecretName`` expects ``postgresql://`` and won't work with ``db+postgresql://``. You'll need to create separate secrets with the correct scheme. For Airflow version >= ``2.4`` it is possible to omit the result backend secret, as Airflow will use ``sql_alchemy_conn`` (specified in ``metadataSecret``) with a db+ scheme prefix by default.

.. _production-guide:pgbouncer:

PgBouncer
---------

If you are using PostgreSQL as your database, you will likely want to enable `PgBouncer <https://www.pgbouncer.org/>`_ as well.
Airflow can open a lot of database connections due to its distributed nature and using a connection pooler can significantly
reduce the number of open connections on the database.

.. code-block:: yaml

  pgbouncer:
    enabled: true

Depending on the size of your Airflow instance, you may want to adjust the following as well (defaults are shown):

.. code-block:: yaml

  pgbouncer:
    # The maximum number of connections to PgBouncer
    maxClientConn: 100
    # The maximum number of server connections to the metadata database from PgBouncer
    metadataPoolSize: 10
    # The maximum number of server connections to the result backend database from PgBouncer
    resultBackendPoolSize: 5

Webserver Secret Key
--------------------

You should set a static webserver secret key when deploying with this chart as it will help ensure
your Airflow components only restart when necessary.

.. warning::
  You should use a different secret key for every instance you run, as this key is used to sign
  session cookies and perform other security related functions!

First, generate a strong secret key:

.. code-block:: bash

    python3 -c 'import secrets; print(secrets.token_hex(16))'

Now add the secret to your values file:

.. code-block:: yaml

    webserverSecretKey: <secret_key>

Alternatively, create a Kubernetes Secret and use ``webserverSecretKeySecretName``:

.. code-block:: yaml

    webserverSecretKeySecretName: my-webserver-secret
    # where the random key is under `webserver-secret-key` in the k8s Secret

Example to create a Kubernetes Secret from ``kubectl``:

.. code-block:: bash

    kubectl create secret generic my-webserver-secret --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')"

The webserver key is also used to authorize requests to Celery workers when logs are retrieved. The token
generated using the secret key has a short expiry time though - make sure that time on ALL the machines
that you run airflow components on is synchronized (for example using ntpd) otherwise you might get
"forbidden" errors when the logs are accessed.

Eviction configuration
----------------------
When running Airflow along with the `Kubernetes Cluster Autoscaler <https://github.com/kubernetes/autoscaler>`_, it is important to configure whether pods can be safely evicted.
This setting can be configured in the Airflow chart at different levels:

.. code-block:: yaml

  workers:
    safeToEvict: true
  scheduler:
    safeToEvict: true
  webserver:
    safeToEvict: true

When using ``KubernetesExecutor``, ``workers.safeToEvict`` should be set to ``false`` to avoid them being removed before finishing.

Extending and customizing Airflow Image
---------------------------------------

The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
However, Airflow has more than 60 community managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers, or add custom tools and binaries that are needed in
your deployment.

In Kubernetes and Docker terms this means that you need another image with your specific requirements.
This is why you should learn how to build your own ``Docker`` (or more properly ``Container``) image.

Typical scenarios where you would like to use your custom image:

* Adding ``apt`` packages
* Adding ``PyPI`` packages
* Adding binary resources necessary for your deployment
* Adding custom tools needed in your deployment

See `Building the image <https://airflow.apache.org/docs/docker-stack/build.html>`_ for more
details on how you can extend and customize the Airflow image.

Managing DAG Files
------------------

See :doc:`manage-dags-files`.

.. _production-guide:knownhosts:

knownHosts
^^^^^^^^^^

If you are using ``dags.gitSync.sshKeySecret``, you should also set ``dags.gitSync.knownHosts``. Here we will show the process
for GitHub, but the same can be done for any provider:

Grab GitHub's public key:

.. code-block:: bash

    ssh-keyscan -t rsa github.com > github_public_key

Next, print the fingerprint for the public key:

.. code-block:: bash

    ssh-keygen -lf github_public_key

Compare that output with `GitHub's SSH key fingerprints <https://docs.github.com/en/github/authenticating-to-github/githubs-ssh-key-fingerprints>`_.

They match, right? Good. Now, add the public key to your values. It'll look something like this:

.. code-block:: yaml

    dags:
      gitSync:
        knownHosts: |
          github.com ssh-rsa AAAA...1/wsjk=


External Scheduler
^^^^^^^^^^^^^^^^^^

To use an external Scheduler instance:

.. code-block:: yaml

  scheduler:
    enabled: false

Ensure that your external webserver/scheduler is connected to the same redis host. This will ensure the scheduler is aware of the workers deployed in the helm-chart.

Accessing the Airflow UI
------------------------

How you access the Airflow UI will depend on your environment; however, the chart does support various options:

External Webserver
^^^^^^^^^^^^^^^^^^

To use an external Webserver:

.. code-block:: yaml

  webserver:
    enabled: false

Ensure that your external webserver/scheduler is connected to the same redis host. This will ensure the scheduler is aware of the workers deployed in the helm-chart.

Ingress
^^^^^^^

You can create and configure ``Ingress`` objects. See the :ref:`Ingress chart parameters <parameters:ingress>`.
For more information on ``Ingress``, see the
`Kubernetes Ingress documentation <https://kubernetes.io/docs/concepts/services-networking/ingress/>`_.

LoadBalancer Service
^^^^^^^^^^^^^^^^^^^^

You can change the Service type for the webserver to be ``LoadBalancer``, and set any necessary annotations:

.. code-block:: yaml

  webserver:
    service:
      type: LoadBalancer

For more information on ``LoadBalancer`` Services, see the `Kubernetes LoadBalancer Service Documentation
<https://kubernetes.io/docs/concepts/services-networking/service/#loadbalancer>`_.

Logging
-------

Depending on your choice of executor, task logs may not work out of the box. All logging choices can be found
at :doc:`manage-logs`.

Metrics
-------

The chart can support sending metrics to an existing StatsD instance or provide a Prometheus endpoint.

Prometheus
^^^^^^^^^^

The metrics endpoint is available at ``svc/{{ .Release.Name }}-statsd:9102/metrics``.

External StatsD
^^^^^^^^^^^^^^^

To use an external StatsD instance:

.. code-block:: yaml

  statsd:
    enabled: false
  config:
    metrics:  # or 'scheduler' for Airflow 1
      statsd_on: true
      statsd_host: ...
      statsd_port: ...

IPv6 StatsD
^^^^^^^^^^^^^^^

To use an StatsD instance with IPv6 address. Example with Kubernetes with IPv6 enabled:

.. code-block:: yaml
  statsd:
    enabled: true
  config:
    metrics:  # or 'scheduler' for Airflow 1
      statsd_on: 'True'
      statsd_host: ...
      statsd_ipv6: 'True'
      statsd_port: ...
      statsd_prefix: airflow

Datadog
^^^^^^^
If you are using a Datadog agent in your environment, this will enable Airflow to export metrics to the Datadog agent.

.. code-block:: yaml

  statsd:
    enabled: false
  config:
    metrics: # or 'scheduler' for Airflow 1
      statsd_on: true
      statsd_port: 8125
  extraEnv: |-
    - name: AIRFLOW__METRICS__STATSD_HOST
      valueFrom:
        fieldRef:
          fieldPath: status.hostIP

Celery Backend
--------------

If you are using ``CeleryExecutor`` or ``CeleryKubernetesExecutor``, you can bring your own Celery backend.

By default, the chart will deploy Redis. However, you can use any supported Celery backend instead:

.. code-block:: yaml

  redis:
    enabled: false
  data:
    brokerUrl: redis://redis-user:password@redis-host:6379/0

For more information about setting up a Celery broker, refer to the
exhaustive `Celery documentation on the topic <http://docs.celeryproject.org/en/latest/getting-started/>`_.

Security Context Constraints
-----------------------------

A ``Security Context Constraint`` (SCC) is a OpenShift construct that works as a RBAC rule; however, it targets Pods instead of users.
When defining a SCC, one can control actions and resources a POD can perform or access during startup and runtime.

The SCCs are split into different levels or categories with the ``restricted`` SCC being the default one assigned to Pods.
When deploying Airflow to OpenShift, one can leverage the SCCs and allow the Pods to start containers utilizing the ``anyuid`` SCC.

In order to enable the usage of SCCs, one must set the parameter :ref:`rbac.createSCCRoleBinding <parameters:Kubernetes>` to ``true`` as shown below:

.. code-block:: yaml

  rbac:
    create: true
    createSCCRoleBinding: true

In this chart, SCCs are bound to the Pods via RoleBindings meaning that the option ``rbac.create`` must also be set to ``true`` in order to fully enable the SCC usage.

For more information about SCCs and what can be achieved with this construct, please refer to `Managing security context constraints <https://docs.openshift.com/container-platform/latest/authentication/managing-security-context-constraints.html#scc-prioritization_configuring-internal-oauth/>`_.

Security Context
----------------

In Kubernetes a ``securityContext`` can be used to define user ids, group ids and capabilities such as running a container in privileged mode.

When deploying an application to Kubernetes, it is recommended to give the least privilege to containers so as
to reduce access and protect the host where the container is running.

In the Airflow Helm chart, the ``securityContext`` can be configured in several ways:

  * :ref:`uid <parameters:Airflow>` (configures the global uid or RunAsUser)
  * :ref:`gid <parameters:Airflow>` (configures the global gid or fsGroup)
  * :ref:`securityContexts <parameters:Kubernetes>` (same as ``uid`` but allows for setting all `Pod securityContext options <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podsecuritycontext-v1-core>`_ and `Container securityContext options <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#securitycontext-v1-core>`_)

The same way one can configure the global :ref:`securityContexts <parameters:Kubernetes>`, it is also possible to configure different values for specific workloads by setting their local ``securityContexts`` as follows:

.. code-block:: yaml

  workers:
    securityContexts:
      pod:
        runAsUser: 5000
        fsGroup: 0
      containers:
        allowPrivilegeEscalation: false


In the example above, the workers Pod ``securityContexts`` will be set to ``runAsUser: 5000`` and ``fsGroup: 0``.  The containers pod will be set to ``allowPrivilegeEscalation: false``.

As one can see, the local setting will take precedence over the global setting when defined. The following explains the precedence rule for ``securityContexts`` options in this chart:

.. code-block:: yaml

  uid: 40000
  gid: 0

  securityContexts:
    pod:
      runAsUser: 50000
      fsGroup: 0

  workers:
    securityContexts:
      pod:
        runAsUser: 1001
        fsGroup: 0

This will generate the following worker deployment:

.. code-block:: yaml

  kind: StatefulSet
  apiVersion: apps/v1
  metadata:
    name: airflow-worker
  spec:
    serviceName: airflow-worker
    template:
      spec:
        securityContext:    # As the securityContexts was defined in ``workers``, its value will take priority
          runAsUser: 1001
          fsGroup: 0

If we remove both the ``securityContexts`` and ``workers.securityContexts`` from the example above, the output will be the following:

.. code-block:: yaml

  uid: 40000
  gid: 0

  securityContexts: {}

  workers:
    securityContexts: {}

This will generate the following worker deployment:

.. code-block:: yaml

  kind: StatefulSet
  apiVersion: apps/v1
  metadata:
    name: airflow-worker
  spec:
    serviceName: airflow-worker
    template:
      spec:
        securityContext:
          runAsUser: 40000   # As the securityContext was not defined in ``workers`` or ``podSecurity``, the value from uid will be used
          fsGroup: 0         # As the securityContext was not defined in ``workers`` or ``podSecurity``, the value from gid will be used
        initContainers:
          - name: wait-for-airflow-migrations
        ...
        containers:
          - name: worker
        ...

And finally if we set ``securityContexts`` but not ``workers.securityContexts``:

.. code-block:: yaml

  uid: 40000
  gid: 0

  securityContexts:
    pod:
      runAsUser: 50000
      fsGroup: 0

  workers:
    securityContexts: {}

This will generate the following worker deployment:

.. code-block:: yaml

  kind: StatefulSet
  apiVersion: apps/v1
  metadata:
    name: airflow-worker
  spec:
    serviceName: airflow-worker
    template:
      spec:
        securityContext:     # As the securityContexts was not defined in ``workers``, the values from securityContexts will take priority
          runAsUser: 50000
          fsGroup: 0
        initContainers:
          - name: wait-for-airflow-migrations
        ...
        containers:
          - name: worker
        ...

Built-in secrets and environment variables
------------------------------------------

The Helm Chart by default uses Kubernetes Secrets to store secrets that are needed by Airflow.
The contents of those secrets are by default turned into environment variables that are read by
Airflow (some of the environment variables have several variants to support older versions of Airflow).

By default, the secret names are determined from the Release Name used when the Helm Chart is deployed,
but you can also use a different secret to set the variables or disable using secrets
entirely and rely on environment variables (specifically if you want to use ``_CMD`` or ``__SECRET`` variant
of the environment variable.

However, Airflow supports other variants of setting secret configuration - you can specify a system
command to retrieve and automatically rotate the secret (by defining variable with ``_CMD`` suffix) or
to retrieve a variable from secret backed (by defining the variable with ``_SECRET`` suffix).

If the ``<VARIABLE_NAME>>`` is set, it takes precedence over the ``_CMD`` and ``_SECRET`` variant, so
if you want to set one of the ``_CMD`` or ``_SECRET`` variants, you MUST disable the built in
variables retrieved from Kubernetes secrets, by setting ``.Values.enableBuiltInSecretEnvVars.<VARIABLE_NAME>``
to false.

For example in order to use a command to retrieve the DB connection you should (in your ``values.yaml``
file) specify:

.. code-block:: yaml

  extraEnv:
    AIRFLOW_CONN_AIRFLOW_DB_CMD: "/usr/local/bin/retrieve_connection_url"
  enableBuiltInSecretEnvVars:
    AIRFLOW_CONN_AIRFLOW_DB: false

Here is the full list of secrets that can be disabled and replaced by ``_CMD`` and ``_SECRET`` variants:

+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| Default secret name if secret name not specified      | Use a different Kubernetes Secret        | Airflow Environment Variable                     |
+=======================================================+==========================================+==================================================+
| ``<RELEASE_NAME>-airflow-metadata``                   | ``.Values.data.metadataSecretName``      | | ``AIRFLOW_CONN_AIRFLOW_DB``                    |
|                                                       |                                          | | ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``        |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| ``<RELEASE_NAME>-fernet-key``                         | ``.Values.fernetKeySecretName``          | ``AIRFLOW__CORE__FERNET_KEY``                    |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| ``<RELEASE_NAME>-webserver-secret-key``               | ``.Values.webserverSecretKeySecretName`` | ``AIRFLOW__WEBSERVER__SECRET_KEY``               |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| ``<RELEASE_NAME>-airflow-result-backend``             | ``.Values.data.resultBackendSecretName`` | | ``AIRFLOW__CELERY__CELERY_RESULT_BACKEND``     |
|                                                       |                                          | | ``AIRFLOW__CELERY__RESULT_BACKEND``            |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| ``<RELEASE_NAME>-airflow-brokerUrl``                  | ``.Values.data.brokerUrlSecretName``     | ``AIRFLOW__CELERY__BROKER_URL``                  |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+
| ``<RELEASE_NAME>-elasticsearch``                      | ``.Values.elasticsearch.secretName``     | | ``AIRFLOW__ELASTICSEARCH__HOST``               |
|                                                       |                                          | | ``AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST`` |
+-------------------------------------------------------+------------------------------------------+--------------------------------------------------+

There are also a number of secrets, which names are also determined from the release name, that do not need to
be disabled. This is because either they do not follow the ``_CMD`` or ``_SECRET`` pattern, are variables
which do not start with ``AIRFLOW__``, or they do not have a corresponding variable.

There is also one ``_AIRFLOW__*`` variable, ``AIRFLOW__CELERY__FLOWER_BASIC_AUTH``, that does not need to be disabled,
even if you want set the ``_CMD`` and ``_SECRET`` variant. This variable is not set by default. It is only set
when ``.Values.flower.secretName`` is set or when ``.Values.flower.user`` and ``.Values.flower.password``
are set. So if you do not set any of the ``.Values.flower.*`` variables, you can freely configure
flower Basic Auth using the ``_CMD`` or ``_SECRET`` variant without disabling the basic variant.

+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| Default secret name if secret name not specified      | Use a different Kubernetes Secret        | Airflow Environment Variable                   |
+=======================================================+==========================================+================================================+
| ``<RELEASE_NAME>-redis-password``                     | ``.Values.redis.passwordSecretName``     | ``REDIS_PASSWORD``                             |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| ``<RELEASE_NAME>-pgbouncer-config``                   | ``.Values.pgbouncer.configSecretName``   |                                                |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| ``<RELEASE_NAME>-pgbouncer-certificates``             |                                          |                                                |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| ``<RELEASE_NAME>-registry``                           | ``.Values.registry.secretName``          |                                                |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| ``<RELEASE_NAME>-kerberos-keytab``                    |                                          |                                                |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+
| ``<RELEASE_NAME>-flower``                             | ``.Values.flower.secretName``            | ``AIRFLOW__CELERY__FLOWER_BASIC_AUTH``         |
+-------------------------------------------------------+------------------------------------------+------------------------------------------------+

You can read more about advanced ways of setting configuration variables in the
:doc:`apache-airflow:howto/set-config`.
