Apache Airflow Hugging Face Provider
====================================

The Hugging Face provider connects Airflow tasks to Hugging Face Datasets and
Hub repositories. It supplies a shared ``HuggingFaceHook`` and four operators
to load a dataset, map a Python callable, stream directly to the Hub, and
publish a durable artifact. Durable data moves between tasks as Parquet objects
and a small JSON manifest, so tasks can run on separate workers when their
artifact store is shared.

Supported capabilities
----------------------

* Airflow connection based Hub authentication, with Hub SDK fallback when the
  default connection is absent.
* Revision pinning and source provenance for Hub dataset loads.
* Split aware Parquet handoff with features metadata and content fingerprints.
* Synchronous publication of materialized or streamed datasets with the Hub
  commit SHA and URL in results.
* ``hf://datasets/<repo_id>`` assets and OpenLineage conversion.

``StreamDatasetOperator`` lazily transforms and filters an ``IterableDataset``
and publishes it directly to the Hub. Datasets uploads Parquet shards
progressively; the operator does not retain all rows or create an intermediate
artifact. Standalone filtering and split assembly operators are not included
in the v1 public API.

Installation
------------

Install into an Airflow environment with Python 3.11 through 3.14:

.. code-block:: bash

   pip install apache-airflow-providers-huggingface

The provider requires Apache Airflow 3.0 or later, ``datasets>=4.0.0,<5``,
and ``huggingface-hub>=0.24,<2``. For ``s3://`` and ``gs://`` artifacts,
install the corresponding storage provider and filesystem dependency. Select
the matching Airflow connection in the URI's user-info section, for example
``s3://aws_default@my-bucket/path/data.json`` or
``gs://google_cloud_default@my-bucket/path/data.json``. Without a connection
ID in the URI, the filesystem uses its default credential chain; configuring
an Airflow connection alone does not select it.

Guides
------

.. toctree::
   :maxdepth: 2

   connections
   operators
   artifacts
   security
   changelog

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/huggingface/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/huggingface/index>

The system-test DAGs under ``tests/system/huggingface/`` also serve as runnable
examples and are collected with pytest through Airflow's ``get_test_run(dag)``
system-test helper. Run them with the repository's system-test pytest setup.
The read-only workflow uses a pinned public dataset. The publishing workflow
uses the ``airflow-hf`` organization by default and requires a write-scoped
``huggingface_default`` Airflow connection. Set ``HF_TEST_HUB_ORG`` or
``HF_TEST_HUB_CONN_ID`` to override those defaults. ``HF_TEST_WRITE_REPO_ID``
and ``HF_TEST_DURABLE_REPO_ID`` can override the base repository names. Each
run appends ``SYSTEM_TESTS_ENV_ID`` (or a generated UUID) to its repository
names to avoid collisions. Cleanup tasks use ``ALL_DONE`` so they are attempted
after failures. On Airflow 3.3 and later, the publishing DAG also checks retry
reuse of a successful Hub commit.

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-huggingface package
------------------------------------------------------

Load, transform, and publish Hugging Face Datasets.

Release: 0.1.0

Provider package
----------------

This package is for the ``huggingface`` provider.
All classes for this package are included in the ``airflow.providers.huggingface`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-huggingface``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0,<4``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``datasets``                                ``>=4.0.0``
``huggingface-hub``                         ``>=0.24``
==========================================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-huggingface 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-huggingface 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_huggingface-0.1.0-py3-none-any.whl.sha512>`__)
