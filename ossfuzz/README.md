# Airflow OSS-Fuzz fuzzers

This directory contains the upstream-owned fuzz targets used by OSS-Fuzz for
Apache Airflow.

## What’s here

- `*_fuzz.py`: Atheris fuzz targets (packaged by OSS-Fuzz via `pyinstaller`).
- `*.dict`: Optional libFuzzer dictionaries for structured inputs.
- `*.options`: libFuzzer options (e.g. `max_len`) tuned per target.
- `seed_corpus/<fuzzer>/...`: Small seed corpora that get zipped and uploaded to
  OSS-Fuzz for each target.

## Supported engines / sanitizers (Python constraints)

Airflow is fuzzed as a **Python** OSS-Fuzz project. Practically, this means:

- **Fuzzing engine**: `libfuzzer` (Atheris). Other engines (AFL/honggfuzz) are
  not typically used/supported for Python targets in OSS-Fuzz.
- **Sanitizers**: `address`, `undefined`, `coverage`, `introspector` are the
  relevant modes. **MSan (`memory`) is not supported** for Python OSS-Fuzz
  projects.

## Running locally with OSS-Fuzz helper

From a checkout of `google/oss-fuzz` (examples assume the Airflow repo is at
`/home/sky/tmp/airflow`):

- Build + basic validation:
  - `python3 infra/helper.py build_fuzzers --clean --sanitizer address airflow /home/sky/tmp/airflow`
  - `python3 infra/helper.py check_build --sanitizer address airflow`
  - `python3 infra/helper.py build_fuzzers --clean --sanitizer undefined airflow /home/sky/tmp/airflow`
  - `python3 infra/helper.py check_build --sanitizer undefined airflow`

- Coverage build + validation:
  - `python3 infra/helper.py build_fuzzers --clean --sanitizer coverage airflow /home/sky/tmp/airflow`
  - `python3 infra/helper.py check_build --sanitizer coverage airflow`

- Introspector report (fast path):
  - `python3 infra/helper.py build_fuzzers --clean --sanitizer introspector airflow /home/sky/tmp/airflow`
  - Open the report at `oss-fuzz/build/out/airflow/inspector/fuzz_report.html`

Note: `python3 infra/helper.py introspector ...` is an end-to-end pipeline that
also builds/runs coverage and can take a long time for a large project like
Airflow. For most PR validation, the “fast path” above is sufficient.

