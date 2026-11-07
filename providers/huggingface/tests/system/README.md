# Hugging Face system tests

Run the DAG system tests with pytest from the repository root in an Airflow
system-test environment:

```bash
pytest tests/system/huggingface
```

The read-only test needs no Hugging Face credentials. The publishing test
publishes temporary private datasets under the ``airflow-hf`` organization by
default, so configure an Airflow connection named ``huggingface_default`` with
a write-scoped Hub token before running it. Set ``HF_TEST_HUB_ORG`` or
``HF_TEST_HUB_CONN_ID`` to override the defaults. The publishing DAG also
checks retry reuse of a successful Hub commit on Airflow 3.3 or later; that
branch is omitted on earlier versions.

The system-test DAGs use ``get_test_run(dag)`` so pytest can execute their task
graphs as Airflow system tests. Repository ID and run ID environment overrides
are documented in the provider docs.
