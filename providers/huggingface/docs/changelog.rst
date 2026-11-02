Changelog
=========

Version 0.1.0
------------

Initial public release of the Hugging Face provider for Apache Airflow.

* Add ``HuggingFaceHook`` and the ``huggingface`` connection.
* Add ``LoadDatasetOperator``, ``MapDatasetOperator``,
  ``StreamDatasetOperator``, and ``PushToHubOperator``.
* Add revision pinned loads, Parquet artifacts, fingerprints, Hub publication
  commit metadata, and Hugging Face dataset assets.
* Set the artifact contract to JSON manifests and per-split Parquet objects.
  Direct streaming publication is included; standalone filtering and split
  assembly operators are not included in the v1 public API.
* Documented the minimum supported "datasets>=3.4.0,<5" version for Hub commit metadata support.
