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

.. _howto/toolset:object_storage:

Files on object storage: ``ObjectStorageToolset``
=================================================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

Give an agent the files under one location in S3, GCS, Azure Blob Storage or any other
store Airflow's :class:`~airflow.sdk.ObjectStoragePath` can open, and let it find and
read what it needs: a month's reports, the config files of a failing job, the first
rows of a Parquet extract. The agent can only read, and only under the path you give
it.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_object_storage_toolset.py
    :language: python
    :start-after: [START howto_toolset_object_storage]
    :end-before: [END howto_toolset_object_storage]

The credentials come from ``conn_id``, the same connection your other tasks use for
that store. With ``conn_id=None`` the store's default credentials apply, such as the
worker's AWS role.

The three tools
---------------

``list_files``
    Lists one directory, sorted by name: files with their size in bytes, and
    subdirectories with a trailing slash. The model goes deeper by listing a
    subdirectory. A directory with more than ``max_files`` entries is listed a page at a
    time, with a note giving the ``offset`` to continue from. Every call lists the whole
    directory from storage, so a very large one is slow to page through.

``get_file_info``
    The size and last-modified time of one file, so the model can decide whether it is
    worth reading.

``read_file``
    A text file a window of lines at a time. The model passes ``offset`` and ``limit``
    as line numbers, and a result that stops early says which ``offset`` to continue
    from, the same shape as the sandbox's ``read_file``. A Parquet or Avro file comes
    back as its row count, its schema and its first 20 rows; ``offset`` and ``limit`` do
    not apply to it. To query one, such as summing a column, use :doc:`datafusion`. Text
    compressed as ``.gz``, ``.bz2`` or ``.xz`` is decompressed first.

Every path the model supplies is relative to the root. An absolute path, a path with a
scheme such as ``s3://``, and a path that climbs out with ``..`` are refused, and on a
local root a symbolic link that points outside the root is refused too, and left out of
listings.

What the model cannot read is refused with a message it can act on, rather than failing
the task: an image or PDF, a binary file, a file larger than ``max_read_bytes`` (10 MiB
by default), a corrupt file, a path that does not exist, or one the connection may not
read.

.. _object-storage-toolset-restricted:

Restricting the agent
---------------------

This agent can read only under ``s3://acme-reports/finance/``, and every list and read
is bounded:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_object_storage_toolset.py
    :language: python
    :start-after: [START howto_toolset_object_storage_restricted]
    :end-before: [END howto_toolset_object_storage_restricted]

Run against an S3 endpoint where an ``acme-payroll`` bucket sits beside the reports,
these reads were refused. Each refusal comes back to the model as the tool's result,
so it does not use ``max_retries``, and the run carries on:

``../../acme-payroll/salaries.csv``
    ``'../../acme-payroll/salaries.csv' leaves the storage root; '..' is not allowed.``

``s3://acme-payroll/salaries.csv``
    ``'s3://acme-payroll/salaries.csv' is not a relative path. Name files relative to
    the storage root.``

``/etc/passwd``
    ``'/etc/passwd' is not a relative path. Name files relative to the storage root.``

``2026-09/chart.png``
    ``'2026-09/chart.png' is a png file, which this tool cannot read as text.``

``2026-09/ledger.csv``, a 1.5 MB file
    ``'2026-09/ledger.csv' is larger than the 1.0MB this tool reads.``

Because refused reads cost nothing from ``max_retries``, bound a run that keeps
asking with the operator's ``usage_limits``. The path check is the toolset's only
boundary on location; credentials that can read only ``s3://acme-reports/finance/`` keep that limit
if the check has a gap.

Parameters
----------

``path``
    The root, for example ``"s3://acme-reports/finance/"``. Templated when the toolset
    is passed to ``AgentOperator`` or ``@task.agent``.
``conn_id``
    The connection for the store. Templated like ``path``.
``max_files``
    The most entries one ``list_files`` result holds. Default ``200``.
``max_read_bytes``
    The largest file ``read_file`` opens, after decompression. Default 10 MiB.
``max_output_bytes``
    The most bytes one ``read_file`` result holds. Default 50 KiB.
``tool_prefix``
    A prefix for the tool names, needed when the agent has another toolset with the same
    tool names: a second ``ObjectStorageToolset``, or a ``SandboxToolset``, which has a
    ``read_file`` of its own.
``max_retries``
    How many times the model may correct a call with invalid arguments. A failed read does
    not count. Default ``None``, the agent's ``retries``. See :ref:`toolset-retry-budget`.

When to choose it
-----------------

**Choose it when** the agent should read files the way a person would: browse a
directory, open the report that matters, look at a file's first lines or rows. Text
files need nothing beyond the filesystem package for your store, which Airflow's object
storage already uses; Parquet and Avro need this provider's ``parquet`` or ``avro``
extra. To hand the model a known file rather than let it find one, ``@task.llm_file_analysis``
reads the file for it. For read-only access through a hook's own methods,
``HookToolset(S3Hook(), allowed_methods=["list_keys", "read_key"])`` works too, but the
model then chooses the bucket and key. Pinning ``bucket_name`` with ``pinned_arguments``
fixes the bucket and still leaves the model any key in it, where this toolset keeps the
model under one root.

**What it cannot do**

- It cannot write, delete, move or copy anything.
- It does not search inside files. The model finds a file by listing directories, so a
  store with thousands of files per directory is slow to explore. Give it a narrower root.
- It reads one file at a time. To ask a question across many files, such as a sum over
  a month of Parquet files, use :doc:`datafusion`, which runs SQL over them.
- The root is the boundary only as far as the path check goes. The connection's own
  permissions are the real limit on what can be read, so scope its role or key to the
  prefix you pass as ``path``.

Using it with other agent frameworks
------------------------------------

``ObjectStorageToolset`` implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, so the same three tools work in
a Strands or Google ADK agent, or the Anthropic SDK's tool runner, through
``AirflowTools``; see :doc:`../frameworks/index`.
Outside ``AgentOperator``, ``path`` and ``conn_id`` are used as given: they are not
rendered as templates.
