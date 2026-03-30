====================================================
apache-airflow-providers-grafana
====================================================

Content
-------

This package provides integrations with Grafana ecosystem, specifically Loki for remote task logging in Airflow.

Architecture and Design Decisions
---------------------------------

Loki Logging Backend
~~~~~~~~~~~~~~~~~~~~

The Loki logging backend for Apache Airflow is engineered to scale massively without degrading the performance of your Grafana Loki cluster. It achieves this by carefully sidestepping **cardinality explosions** and leveraging modern TSDB (Time Series Database) features like **Bloom filters**.

The Cardinality Trap
^^^^^^^^^^^^^^^^^^^^

Unlike Elasticsearch, which indexes every field by default, Grafana Loki relies on a minimalistic index. In Loki, an *Index Stream* is created for every unique combination of labels. 

- **Good Labels**: ``{job="airflow_tasks", dag_id="my_dag"}`` (Yields a small, stable number of streams—typically one per DAG).
- **Bad Labels**: ``{job="airflow_tasks", dag_id="my_dag", task_id="extract", run_id="scheduled__2023...", try_number="1"}``

If high-cardinality metadata like ``run_id``, ``task_id``, and ``try_number`` are used as Loki labels, Airflow will generate an infinitely growing number of unique index streams. This triggers a cardinality explosion: Loki's ingesters run out of memory tracking millions of short-lived streams, the global index inflates, and search performance collapses.

The Solution: JSON payloads and TSDB Bloom Filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To keep the index small and performant, the Airflow Loki Provider drops high-cardinality identifiers from the labels entirely. Instead, it embeds them directly into the JSON log payload prior to uploading chunks to the Loki Push API:

.. code-block:: json

    {"event": "Fetching data...", "task_id": "extract", "run_id": "scheduled__...", "try_number": "1", "map_index": "-1"}

When the Airflow UI requests logs for a specific task run, the ``LokiTaskHandler`` constructs an optimized LogQL query utilizing the ``| json`` parser:

.. code-block:: text

    {job="airflow_tasks", dag_id="my_dag"} | json | task_id="extract" | try_number="1"

Behind the scenes, Loki's modern TSDB engine automatically builds mathematical **Bloom filters** for the structured data inside the log chunks during ingestion. 

When executing the query, Loki instantly resolves the ``{job="airflow_tasks", dag_id="my_dag"}`` stream. Then, before downloading or decompressing any log chunk from object storage, Loki consults the chunk's Bloom filter:

1. *Are the strings ``"extract"`` and ``"1"`` anywhere within this block?*
2. The Bloom filter answers "No" with 100% certainty in microseconds, allowing Loki to skip the entire block of data.
3. Loki only decompresses and evaluates the JSON parser on the specific chunks that mathematically *must* contain the target task's logs.

**Operating Philosophy**: By embedding dynamic metadata into the JSON payload rather than stream labels, this provider guarantees full-text indexing performance while keeping Grafana Loki infrastructure costs near zero, ensuring stability regardless of how many millions of tasks Airflow executes.
