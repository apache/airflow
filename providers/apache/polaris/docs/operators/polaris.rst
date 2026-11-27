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

Apache Polaris Operators
========================

`Apache Polaris <https://polaris.apache.org/>`__ is an Apache Iceberg REST catalog that adds a
Management API (catalogs, principals, roles, grants) and Polaris-only catalog APIs such as
policies. These operators manage everything a Polaris catalog defines: catalogs, namespaces,
table definitions, access and policies. Reading and writing table data stays with pyiceberg, the
Iceberg provider and the engine providers.

All operators:

* take ``polaris_conn_id`` (default ``polaris_default``, see :ref:`howto/connection:polaris`);
* are **idempotent** by default: creating something that exists, or deleting something that is
  gone, succeeds (``if_exists="skip"``, ``if_missing="skip"``; pass ``"fail"`` to raise instead),
  so tasks are safe to retry and Dags safe to re-run;
* **fail without retrying** on errors a retry cannot fix: a misconfigured connection, rejected
  credentials, missing privileges (HTTP 403), a bad request or a missing entity. They raise
  ``AirflowFailException``, so the task's ``retries`` are not spent waiting on a fix that needs a
  person. An entity that already exists (with ``if_exists="fail"``) and a conflicting policy
  attachment count as such errors too. Transient errors (network failures, server errors, rate
  limiting, concurrent updates) are raised as usual and retried. Code calling the hooks directly
  can check the same classification through the ``retryable`` attribute of
  :class:`~airflow.providers.apache.polaris.exceptions.PolarisError`;
* accept Polaris entities as plain dicts in Polaris's JSON shape. ``polaris_conn_id`` and the
  names and definitions they act on (catalog, namespace, role, principal, policy, grants,
  properties, targets) are templated, so one Dag can target several environments through
  ``params``; the behaviour switches (``if_exists``, ``if_missing``, ``cascade``, ...) are not.
  Fields that take a dict or list need ``render_template_as_native_obj=True`` on the Dag when
  they come from a template;
* return only JSON-serializable, non-secret values.

Catalogs
--------

.. _howto/operator:PolarisCreateCatalogOperator:

PolarisCreateCatalogOperator
""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.catalog.PolarisCreateCatalogOperator` creates
an internal catalog, or a federated (``"type": "EXTERNAL"``) one. An existing catalog is returned
unchanged, not updated; use ``PolarisUpdateCatalogOperator`` for changes.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_create_catalog]
    :end-before: [END howto_operator_polaris_create_catalog]

.. _howto/operator:PolarisUpdateCatalogOperator:

PolarisUpdateCatalogOperator
""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.catalog.PolarisUpdateCatalogOperator` merges
``properties`` into the catalog's existing properties (or replaces them with
``replace_properties=True``) and can change ``storage_config_info``. It reads the catalog's
current entity version first, as Polaris requires, and starts again from the latest version, after
a short random pause, if the catalog changes in the meantime (Polaris then answers HTTP 409).

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_update_catalog]
    :end-before: [END howto_operator_polaris_update_catalog]

.. _howto/operator:PolarisDeleteCatalogOperator:

PolarisDeleteCatalogOperator
""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.catalog.PolarisDeleteCatalogOperator` deletes a
catalog. Polaris refuses to delete a catalog that still contains namespaces or catalog roles
other than ``catalog_admin``.

Namespaces
----------

Namespaces are given as ``"a.b"`` or ``["a", "b"]`` (use the list form for names that contain a
dot), and ``catalog`` defaults to the connection's ``default_catalog``.

.. _howto/operator:PolarisCreateNamespaceOperator:

PolarisCreateNamespaceOperator
""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.namespace.PolarisCreateNamespaceOperator`
creates a namespace, optionally with properties; its parent must exist. An existing namespace is
returned unchanged. Polaris adds a ``location`` property.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_create_namespace]
    :end-before: [END howto_operator_polaris_create_namespace]

.. _howto/operator:PolarisUpdateNamespacePropertiesOperator:

PolarisUpdateNamespacePropertiesOperator
""""""""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.namespace.PolarisUpdateNamespacePropertiesOperator`
sets and/or removes properties and returns which keys were updated, removed or already missing.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_update_namespace_properties]
    :end-before: [END howto_operator_polaris_update_namespace_properties]

.. _howto/operator:PolarisDropNamespaceOperator:

PolarisDropNamespaceOperator
""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.namespace.PolarisDropNamespaceOperator` drops an
empty namespace. Polaris refuses to drop one that still contains tables, views, namespaces or
policies; the task then fails without retrying.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_drop_namespace]
    :end-before: [END howto_operator_polaris_drop_namespace]

Tables
------

These operators manage Iceberg table *definitions*. They never request storage credentials from
Polaris, and the tables they return contain only the name, UUID, locations and format version.

.. _howto/operator:PolarisCreateTableOperator:

PolarisCreateTableOperator
""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.table.PolarisCreateTableOperator` creates a
table. The schema, partition spec and sort order use the JSON shapes of the Iceberg REST
specification and are sent as given, so newer features such as format v3 field defaults work;
Polaris validates them. ``spec-id`` and ``order-id`` may be omitted, and property values are
strings. An existing table is returned unchanged, without comparing its definition.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_create_table]
    :end-before: [END howto_operator_polaris_create_table]

.. _howto/operator:PolarisUpdateTablePropertiesOperator:

PolarisUpdateTablePropertiesOperator
""""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.table.PolarisUpdateTablePropertiesOperator`
sets and/or removes table properties. It reads the table first and commits only what would
change, so re-running it does nothing. Like pyiceberg, it commits only if the table's UUID is
unchanged, so a table dropped and re-created in the meantime is left alone (the task fails and
its retries reload the table).

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_update_table_properties]
    :end-before: [END howto_operator_polaris_update_table_properties]

.. _howto/operator:PolarisRenameTableOperator:

PolarisRenameTableOperator
""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.table.PolarisRenameTableOperator` renames a
table, optionally into another namespace of the same catalog. When re-run after a successful
rename (the table is gone, its new name exists) it succeeds and returns ``False``.

Since Polaris 1.9.0, a move into another namespace is refused (HTTP 403) unless the table's
files lie within the destination namespace's location, which is not the case for a table created
in its source namespace, or the catalog enables ``ALLOW_UNSTRUCTURED_TABLE_LOCATION``.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_rename_table]
    :end-before: [END howto_operator_polaris_rename_table]

.. _howto/operator:PolarisDropTableOperator:

PolarisDropTableOperator
""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.table.PolarisDropTableOperator` drops a table.
By default only the catalog entry is removed and the data and metadata files stay in storage;
``purge=True`` also deletes them, which Polaris allows only when the catalog property
``polaris.config.drop-with-purge.enabled`` is ``"true"`` (or the server enables the
``DROP_WITH_PURGE_ENABLED`` feature).

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_drop_table]
    :end-before: [END howto_operator_polaris_drop_table]

.. _howto/operator:PolarisRegisterTableOperator:

PolarisRegisterTableOperator
""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.table.PolarisRegisterTableOperator` registers an
existing Iceberg table from its current ``metadata.json`` file, for example a table moved from
another catalog or dropped without purging. The file must lie within the catalog's allowed
locations.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_register_table]
    :end-before: [END howto_operator_polaris_register_table]

Principals
----------

.. _howto/operator:PolarisCreatePrincipalOperator:

PolarisCreatePrincipalOperator
""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.principal.PolarisCreatePrincipalOperator`
creates a principal and returns its name, client ID and properties.

.. important::

    Polaris returns a new principal's client secret only once, in the create response. This
    operator never returns, logs or stores it. It suits principals that authenticate through an
    external identity provider. For Polaris-issued credentials, obtain them out of band (for
    example with ``polaris principals reset`` in the Polaris CLI) and put them in your secrets
    backend. For the same reason the provider has no operator to rotate credentials.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_create_principal]
    :end-before: [END howto_operator_polaris_create_principal]

.. _howto/operator:PolarisDeletePrincipalOperator:

PolarisDeletePrincipalOperator
""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.principal.PolarisDeletePrincipalOperator`
deletes a principal. The provisioning example removes everything it created in teardown tasks,
which run even after a failure:

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_teardown]
    :end-before: [END howto_operator_polaris_teardown]

Roles and grants
----------------

Polaris access control has two levels: a principal is assigned **principal roles**, a principal
role is assigned **catalog roles**, and a catalog role is granted **privileges** on the catalog,
a namespace, a table, a view or a policy.

.. _howto/operator:PolarisCreatePrincipalRoleOperator:

.. _howto/operator:PolarisCreateCatalogRoleOperator:

.. _howto/operator:PolarisAssignPrincipalRoleOperator:

.. _howto/operator:PolarisAssignCatalogRoleOperator:

.. _howto/operator:PolarisDeletePrincipalRoleOperator:

.. _howto/operator:PolarisDeleteCatalogRoleOperator:

.. _howto/operator:PolarisRevokePrincipalRoleOperator:

.. _howto/operator:PolarisRevokeCatalogRoleOperator:

Roles and assignments
"""""""""""""""""""""

* :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisCreatePrincipalRoleOperator`
  and :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisCreateCatalogRoleOperator`
  create roles; :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisDeletePrincipalRoleOperator` and
  :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisDeleteCatalogRoleOperator` remove them.
* :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisAssignPrincipalRoleOperator`
  and :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisAssignCatalogRoleOperator`
  assign them; :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisRevokePrincipalRoleOperator` and
  :class:`~airflow.providers.apache.polaris.operators.rbac.PolarisRevokeCatalogRoleOperator` undo that. Assigning an already-assigned role is a
  no-op, and revoking an assignment that does not exist succeeds.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_roles]
    :end-before: [END howto_operator_polaris_roles]

.. _howto/operator:PolarisGrantPrivilegeOperator:

.. _howto/operator:PolarisRevokePrivilegeOperator:

PolarisGrantPrivilegeOperator and PolarisRevokePrivilegeOperator
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.rbac.PolarisGrantPrivilegeOperator` grants one
privilege or a list of them to a catalog role.
:class:`~airflow.providers.apache.polaris.operators.rbac.PolarisRevokePrivilegeOperator` revokes
them, optionally with ``cascade=True``. Grant types are ``catalog``, ``namespace`` (with
``namespace``), ``table`` (adds ``tableName``), ``view`` (``viewName``) and ``policy``
(``policyName``); newer Polaris versions add beta types such as ``semantic-model``.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_grant]
    :end-before: [END howto_operator_polaris_grant]

Revoking undoes the grant and the role assignments:

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_provisioning.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_revoke]
    :end-before: [END howto_operator_polaris_revoke]

Policies
--------

Polaris stores policies, attaches them to catalogs, namespaces and tables, and resolves which ones
are in effect on a table through inheritance (a table-level policy overrides a namespace-level
one of the same type). It **does not execute** them: the table-maintenance types
``system.snapshot-expiry``, ``system.data-compaction``, ``system.metadata-compaction`` and
``system.orphan-file-removal`` only take effect when something runs the matching Iceberg
procedures. Airflow can.

.. _howto/operator:PolarisCreatePolicyOperator:

PolarisCreatePolicyOperator
"""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.policy.PolarisCreatePolicyOperator` creates a
policy in a namespace. By default (``if_exists="update"``) an existing policy is updated when its
content or description differ, so the task declares the desired state; an argument left out keeps
its current value. An existing policy of a different type fails the task.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_policies.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_create_policy]
    :end-before: [END howto_operator_polaris_create_policy]

.. _howto/operator:PolarisAttachPolicyOperator:

.. _howto/operator:PolarisDetachPolicyOperator:

.. _howto/operator:PolarisDropPolicyOperator:

Attaching, detaching and dropping policies
""""""""""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.policy.PolarisAttachPolicyOperator` attaches a
policy to ``{"type": "catalog"}``, ``{"type": "namespace", "path": [...]}`` or
``{"type": "table-like", "path": [..., "<table>"]}``;
:class:`~airflow.providers.apache.polaris.operators.policy.PolarisDetachPolicyOperator` detaches
it. :class:`~airflow.providers.apache.polaris.operators.policy.PolarisDropPolicyOperator` drops a
policy (``detach_all=True`` detaches it everywhere first). Re-attaching a policy is a no-op, but a
target holds at most one inheritable policy of each type: attaching a second one fails the task,
so detach the old policy first.

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_policies.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_attach_policy]
    :end-before: [END howto_operator_polaris_attach_policy]

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_policies.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_detach_and_drop_policy]
    :end-before: [END howto_operator_polaris_detach_and_drop_policy]

.. _howto/operator:PolarisGetApplicablePoliciesOperator:

PolarisGetApplicablePoliciesOperator
""""""""""""""""""""""""""""""""""""

:class:`~airflow.providers.apache.polaris.operators.policy.PolarisGetApplicablePoliciesOperator`
returns the policies in effect on the catalog, a namespace or one table, with ``content`` parsed
from JSON. Disabled policies (``"enable": false``) are dropped unless ``enabled_only=False``;
``policy_types`` narrows the result.

With ``per_table=True`` it resolves policies for every table in ``namespace`` and returns one item
per (table, policy), ready for dynamic task mapping:

.. code-block:: python

    {
        "catalog": "analytics",
        "namespace": ["sales"],
        "table": "orders",
        "policy": {
            "name": "...",
            "type": "system.snapshot-expiry",
            "inherited": True,
            "content": {...},
        },
        # only for system.orphan-file-removal: the table's base location, so a task can check
        # that the policy's "locations" stay inside it
        "table_location": "s3://lake/sales/orders",
    }

.. exampleinclude:: /../tests/system/apache/polaris/example_polaris_policies.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_polaris_get_applicable_policies]
    :end-before: [END howto_operator_polaris_get_applicable_policies]

.. _howto/polaris:maintenance:

Running maintenance on Spark
""""""""""""""""""""""""""""

:func:`~airflow.providers.apache.polaris.utils.maintenance.to_spark_sql` turns one item of
``PolarisGetApplicablePoliciesOperator(per_table=True)`` into the Iceberg Spark procedure call
that applies the policy. It makes no network calls; a Spark task runs the result:

.. code-block:: python

    from airflow.providers.apache.polaris.utils.maintenance import to_spark_sql
    from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

    SparkSqlOperator.partial(task_id="run_maintenance", conn_id="spark_default").expand(
        sql=resolve_policies.output.map(to_spark_sql)
    )

The result is a ``CALL`` of one of Iceberg's
`Spark procedures <https://iceberg.apache.org/docs/latest/spark-procedures/>`__, for example:

.. code-block:: sql

    CALL analytics.system.expire_snapshots(table => 'analytics.sales.orders',
        older_than => TIMESTAMP '2026-10-20 12:00:00+00:00', retain_last => 10)

These are not queries: Iceberg adds ``CALL`` to Spark SQL for table maintenance, with named
arguments (``name => value``). The SQL is built when ``to_spark_sql`` runs, so with ``.map``
the age cutoff is taken when the Spark task starts, and a retry recomputes it.

The Spark session that runs the SQL needs:

* the Iceberg Spark runtime (and ``iceberg-aws-bundle`` or the matching bundle for your
  storage) and Iceberg's SQL extensions,
  ``spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions``;
  without them Spark does not know ``CALL``;
* a Spark catalog pointing at the Polaris catalog through Iceberg's REST catalog. Its name is
  the one in the SQL: the Polaris catalog name, or ``spark_catalog=`` when Spark names it
  differently.
  :meth:`~airflow.providers.apache.polaris.hooks.polaris.PolarisHook.get_iceberg_rest_properties`
  returns its settings for a Polaris connection;
* access to the table's files (credentials vended by Polaris, or the storage's own), and a
  principal allowed to change the table, for example with ``TABLE_WRITE_DATA``;
* for orphan-file removal on object storage, either Hadoop support for the storage scheme
  (``hadoop-aws`` for ``s3://``), or ``"prefix_listing": true`` in the policy's ``config`` so
  Iceberg lists files itself.

Polaris's policy schemas define only ``enable``, and for orphan-file removal
``max_orphan_file_age_in_days`` and ``locations``; everything else is a free-form ``config``.
``to_spark_sql`` reads these settings:

==============================  =======================  =====================================================
Policy type                     Procedure                Settings
==============================  =======================  =====================================================
``system.snapshot-expiry``      ``expire_snapshots``     ``max_snapshot_age_days`` -> ``older_than``,
                                                         ``min_snapshot_to_keep`` -> ``retain_last``;
                                                         ``max_concurrent_deletes``, ``stream_results``,
                                                         ``clean_expired_metadata``
``system.data-compaction``      ``rewrite_data_files``   ``compaction_strategy`` (``bin-pack`` or ``sort``) ->
                                                         ``strategy``; ``target_file_size_bytes`` and every
                                                         hyphenated key (an Iceberg rewrite option such as
                                                         ``min-input-files``) -> ``options``; ``sort_order``,
                                                         ``where``
``system.metadata-compaction``  ``rewrite_manifests``    ``spec_id``, ``use_caching``
``system.orphan-file-removal``  ``remove_orphan_files``  ``max_orphan_file_age_in_days`` (at least 1) ->
                                                         ``older_than``; one statement per entry of
                                                         ``locations``, each inside the table's location;
                                                         ``max_concurrent_deletes``, ``prefix_mismatch_mode``,
                                                         ``prefix_listing``
==============================  =======================  =====================================================

Ages are counted back from the moment the SQL is built and written as UTC timestamps. Keys
not in the table are dropped with a warning; with ``strict=True`` they raise instead.
``dry_run=True`` only lists orphan files and raises for the other types, which have no dry run.
A disabled policy, an unknown policy type or schema version, and invalid values raise
:class:`~airflow.providers.apache.polaris.exceptions.PolarisInvalidArgumentError`.
``spark_catalog`` names the Polaris catalog in Spark, in the ``CALL`` and in its ``table``
argument, when it differs from the Polaris name.

The generated statements were run against Iceberg 1.10 on Spark 3.5 and Iceberg 1.11 on Spark 4.1. Iceberg's procedure
arguments change between versions, so check the ones you use against your Iceberg release.

Reference
---------

For further information, look at:

* `Apache Polaris documentation <https://polaris.apache.org/>`__
* `Polaris access control <https://polaris.apache.org/releases/1.9.0/managing-security/access-control/>`__
* `Polaris policies <https://polaris.apache.org/releases/1.9.0/policy/>`__
* `Polaris identity providers <https://polaris.apache.org/releases/1.9.0/managing-security/external-idp/>`__
