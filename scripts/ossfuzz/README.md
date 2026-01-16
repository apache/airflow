# Airflow OSS-Fuzz fuzzers

This directory contains the upstream-owned fuzz targets used by OSS-Fuzz for
Apache Airflow.

## Security Model Alignment

These fuzzers target code paths with **clear security boundaries** per
Airflow's [security model](../../airflow-core/docs/security/security_model.rst):

- **DAG Serialization/Deserialization**: Used by Scheduler and API Server with
  schema validation. Input comes from DAG parsing and caching.
- **Connection URI Parsing**: Used when creating/updating connections via API.

We explicitly **avoid** fuzzing code paths in the "DAG author trust zone"
where Airflow's policy is that DAG authors can execute arbitrary code.

## What's here

- `src/ossfuzz/*_fuzz.py`: Atheris fuzz targets.
- `*.dict`: Optional libFuzzer dictionaries for structured inputs.
- `*.options`: libFuzzer options (e.g. `max_len`) tuned per target.
- `seed_corpus/<fuzzer>/...`: Small seed corpora that get zipped and uploaded to
  OSS-Fuzz for each target.

## Fuzzers

| Fuzzer | Target | Security Boundary |
|--------|--------|-------------------|
| `serialized_dag_fuzz` | `DagSerialization.from_dict()` | Schema validation |
| `connection_uri_fuzz` | `Connection._parse_from_uri()` | API input validation |

## Supported engines / sanitizers (Python constraints)

Airflow is fuzzed as a **Python** OSS-Fuzz project. Practically, this means:

- **Fuzzing engine**: `libfuzzer` (Atheris). Other engines (AFL/honggfuzz) are
  not typically used/supported for Python targets in OSS-Fuzz.
- **Sanitizers**: `address`, `undefined`, `coverage`, `introspector` are the
  relevant modes. **MSan (`memory`) is not supported** for Python OSS-Fuzz
  projects.

## Running locally

From the repository root, use `uv run` which will automatically set up the
virtual environment and install dependencies:

```bash
# Run serialized_dag_fuzz (quick test, 60 seconds)
uv run --package apache-airflow-ossfuzz serialized_dag_fuzz -max_total_time=60

# Run connection_uri_fuzz
uv run --package apache-airflow-ossfuzz connection_uri_fuzz -max_total_time=60

# Run with a corpus directory
mkdir -p corpus/serialized_dag_fuzz
uv run --package apache-airflow-ossfuzz serialized_dag_fuzz corpus/serialized_dag_fuzz -max_total_time=300
```

## Running with OSS-Fuzz helper

From a checkout of `google/oss-fuzz`:

```bash
# Build + basic validation:
python3 infra/helper.py build_fuzzers --clean --sanitizer address airflow /path/to/airflow
python3 infra/helper.py check_build --sanitizer address airflow

# Coverage build + validation:
python3 infra/helper.py build_fuzzers --clean --sanitizer coverage airflow /path/to/airflow
python3 infra/helper.py check_build --sanitizer coverage airflow
```
