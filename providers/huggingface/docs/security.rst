Security
========

Credentials
-----------

Store a Hugging Face access token in the password field of the ``huggingface``
Airflow connection. Airflow connection secret masking applies to the password;
the provider does not log token values or copy tokens into artifact manifests,
XCom results, asset event metadata, or commit markers. Keep tokens out of DAG
source, task arguments, and transform code. When the default connection is
absent, ``huggingface_hub`` may use ``HF_TOKEN`` or a cached login on the worker.
An explicitly named but missing connection is an error.

Use separate read and write tokens where possible. Grant read access for load
only workflows and write access only to the repositories that publication
tasks need. Set ``private=True`` when a publication must create a private
repository; this setting does not change the visibility of an existing
repository.

Dataset and artifact trust
--------------------------

Datasets and transform callables execute as Python code on Airflow workers.
Review remote dataset scripts, custom builder code, and transform dependencies
before allowing them to run. Pin Hub sources to commit SHAs for repeatable
loads. Treat input artifacts and serialized feature metadata as untrusted when
they come from outside the DAG's controlled storage path.

Artifact files can contain the full dataset. Restrict object store access to
the task roles that need it, use encryption and retention settings appropriate
to the data, and remove abandoned split objects after failed writes. Manifests
are published only after split writes complete, and per split checksums detect
corruption; an interrupted write can still leave partial objects behind.
