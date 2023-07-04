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

Suspending providers
====================

As of April 2023, we have the possibility to suspend individual providers, so that they are not holding
back dependencies for Airflow and other providers. The process of suspending providers is described
in `description of the process <https://github.com/apache/airflow/blob/main/README.md#suspending-releases-for-providers>`_

Technically, suspending a provider is done by setting ``suspended : true``, in the provider.yaml of the
provider. This should be followed by committing the change and either automatically or manually running
pre-commit checks that will either update derived configuration files or ask you to update them manually.
Note that you might need to run pre-commit several times until all the static checks pass,
because modification from one pre-commit might impact other pre-commits.

If you have pre-commit installed, pre-commit will be run automatically on commit. If you want to run it
manually after commit, you can run it via ``breeze static-checks --last-commit`` some of the tests might fail
because suspension of the provider might cause changes in the dependencies, so if you see errors about
missing dependencies imports, non-usable classes etc., you will need to build the CI image locally
via ``breeze build-image --python 3.8 --upgrade-to-newer-dependencies`` after the first pre-commit run
and then run the static checks again.

If you want to be absolutely sure to run all static checks you can always do this via
``pre-commit run --all-files`` or ``breeze static-checks --all-files``.

Some of the manual modifications you will have to do (in both cases ``pre-commit`` will guide you on what
to do.

* You will have to run  ``breeze setup regenerate-command-images`` to regenerate breeze help files
* you will need to update ``extra-packages-ref.rst`` and in some cases - when mentioned there explicitly -
  ``setup.py`` to remove the provider from list of dependencies.

What happens under-the-hood as the result, is that ``generated/providers.json`` file is updated with
the information about available providers and their dependencies and it is used by our tooling to
exclude suspended providers from all relevant parts of the build and CI system (such as building CI image
with dependencies, building documentation, running tests, etc.)


Additional changes needed for cross-dependent providers
=======================================================

Those steps above are usually enough for most providers that are "standalone" and not imported or used by
other providers (in most cases we will not suspend such providers). However some extra steps might be needed
for providers that are used by other providers, or that are part of the default PROD Dockerfile:

* Most of the tests for the suspended provider, will be automatically excluded by pytest collection. However,
  in case a provider is dependent on by another provider, the relevant tests might fail to be collected or
  run by ``pytest``. In such cases you should skip the whole test module failing to be collected by
  adding ``pytest.importorskip`` at the top of the test module.
  For example if your tests fail because they need to import ``apache.airflow.providers.google``
  and you have suspended it, you should add this line at the top of the test module that fails.

Example failing collection after ``google`` provider has been suspended:

  .. code-block:: txt

    _____ ERROR collecting tests/providers/apache/beam/operators/test_beam.py ______
    ImportError while importing test module '/opt/airflow/tests/providers/apache/beam/operators/test_beam.py'.
    Hint: make sure your test modules/packages have valid Python names.
    Traceback:
    /usr/local/lib/python3.8/importlib/__init__.py:127: in import_module
        return _bootstrap._gcd_import(name[level:], package, level)
    tests/providers/apache/beam/operators/test_beam.py:25: in <module>
        from airflow.providers.apache.beam.operators.beam import (
    airflow/providers/apache/beam/operators/beam.py:35: in <module>
        from airflow.providers.google.cloud.hooks.dataflow import (
    airflow/providers/google/cloud/hooks/dataflow.py:32: in <module>
        from google.cloud.dataflow_v1beta3 import GetJobRequest, Job, JobState, JobsV1Beta3AsyncClient, JobView
    E   ModuleNotFoundError: No module named 'google.cloud.dataflow_v1beta3'
    _ ERROR collecting tests/providers/microsoft/azure/transfers/test_azure_blob_to_gcs.py _


The fix is to add this line at the top of the ``tests/providers/apache/beam/operators/test_beam.py`` module:

  .. code-block:: python

    pytest.importorskip("apache.airflow.providers.google")


* Some of the other providers might also just import unconditionally the suspended provider and they will
  fail during provider verification step in CI. In this case you should turn the provider imports
  into conditional imports. For example when import fails after ``amazon`` provider has been suspended:

  .. code-block:: txt

      Traceback (most recent call last):
        File "/opt/airflow/scripts/in_container/verify_providers.py", line 266, in import_all_classes
          _module = importlib.import_module(modinfo.name)
        File "/usr/local/lib/python3.8/importlib/__init__.py", line 127, in import_module
          return _bootstrap._gcd_import(name, package, level)
        File "<frozen importlib._bootstrap>", line 1006, in _gcd_import
        File "<frozen importlib._bootstrap>", line 983, in _find_and_load
        File "<frozen importlib._bootstrap>", line 967, in _find_and_load_unlocked
        File "<frozen importlib._bootstrap>", line 677, in _load_unlocked
        File "<frozen importlib._bootstrap_external>", line 728, in exec_module
        File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
        File "/usr/local/lib/python3.8/site-packages/airflow/providers/mysql/transfers/s3_to_mysql.py", line 23, in <module>
          from airflow.providers.amazon.aws.hooks.s3 import S3Hook
      ModuleNotFoundError: No module named 'airflow.providers.amazon'

or:

  .. code-block:: txt

  Error: The ``airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs`` object in transfers list in
  airflow/providers/microsoft/azure/provider.yaml does not exist or is not a module:
  No module named 'gcloud.aio.storage'

The fix for that is to turn the feature into an optional provider feature (in the place where the excluded
``airflow.providers`` import happens:

  .. code-block:: python

    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    except ImportError as e:
        from airflow.exceptions import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)


* In case we suspend an important provider, which is part of the default Dockerfile you might want to
  update the tests for PROD docker image in ``docker_tests/test_prod_image.py``.

* Some of the suspended providers might also fail ``breeze`` unit tests that expect a fixed set of providers.
  Those tests should be adjusted (but this is not very likely to happen, because the tests are using only
  the most common providers that we will not be likely to suspend).


Resuming providers
==================

Resuming providers is done by reverting the original change that suspended it. In case there are changes
needed to fix problems in the reverted provider, our CI will detect them and you will have to fix them
as part of the PR reverting the suspension.
