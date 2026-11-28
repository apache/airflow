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

Deprecation policy for apache-airflow-providers-amazon package
----------------------------------------------------------------

Versioning of the package
`````````````````````````

The Amazon provider package follows SemVer, as described in
`Airflow's release process and version policy <https://airflow.apache.org/docs/apache-airflow/stable/release-process.html#airflow-s-release-process-and-version-policy>`__.
Breaking changes are released only together with a major version bump of the package.
A change is breaking if a Dag that worked before stops working after the change.

A fix for behaviour that was a bug, for example a parameter that was silently ignored or a result that
contradicted the documentation, is not a breaking change even if a Dag relied on the old behaviour. Such
a fix can be released in a minor or patch version and is called out in the changelog.

Deprecation procedure
`````````````````````

A class, method, parameter or behaviour is deprecated before it is removed or changed:

- The old form keeps working and emits ``AirflowProviderDeprecationWarning`` that says what is changing
  and how to adapt. For a renamed parameter, for example:

  .. code-block:: python

      from airflow.providers.amazon.aws.utils.deprecation import NEXT_MAJOR_VERSION, warn_deprecated

      if region is not None:
          warn_deprecated(
              "Parameter `region` is deprecated. Use the parameter `region_name` instead.",
              removal_version=NEXT_MAJOR_VERSION,
          )
          kwargs["region_name"] = region

- The deprecation is released in a minor version of the package and appears in its :doc:`changelog <changelog>`.
- The removal or behaviour change is released in a later major version. The changelog of that version lists
  every removal or change and how to migrate.

Additional considerations
`````````````````````````

- There is no fixed deprecation period. Deprecated code is removed in a major version, and at least two
  versions that emit the warning are released before it.
- When there is nothing to warn on, for example because AWS already removed the underlying API, the change
  goes straight into a major version with a changelog entry that describes the migration.
