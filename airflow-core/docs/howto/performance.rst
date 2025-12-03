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

Performance tuning (API and UI)
===============================

This guide collects pragmatic tips that improve Airflow performance for API and UI workloads.

Configurable metadata indexes
-----------------------------

Airflow can create additional database indexes on API startup to accelerate common queries used by the API and UI.
This is helpful in larger deployments where filtering and lookups on high‑cardinality columns become hot paths.

Use :ref:`config:database__metadata_indexes` to declare a list of index specifications.
Each entry must follow the ``table(column1, column2, ...)`` syntax (no schema qualification).

- Indexes are created at API server startup. Existing indexes are detected and skipped.
- On PostgreSQL, indexes are created ``CONCURRENTLY`` to avoid blocking writes during creation.
- On other databases (e.g. MySQL, SQLite), a best‑effort non‑blocking creation is attempted; if not supported,
  a standard index creation is used.
- Only Airflow metadata tables should be targeted. Do not include schema qualifiers.

When to use:

- Slow API list/detail endpoints caused by frequent scans or lookups on columns like ``dag_id``, ``task_id``,
  ``run_id``, timestamps (e.g. ``dttm``), or status fields.
- UI pages that load large lists or perform heavy filtering on metadata tables.

Additional Notes:

- Review query plans (e.g. via ``EXPLAIN``) to choose effective column sets and ordering for your workload.
- Composite indexes should list columns in selectivity order appropriate to your most common predicates.
- Indexes incur write overhead; add only those that materially improve your read paths.
