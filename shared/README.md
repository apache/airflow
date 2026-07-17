<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Shared Python Code for Airflow Components

This folder contains code shared across two or more of the Airflow distributions, such as
airflow-core and task-sdk.

## Be Thoughtful about what you add under here

Not every piece of code used in two distributions should be automatically placed in one of the
shared libraries, and sometimes "just duplicate it" is the right approach to take. For example,
if it's just a 5 or 10 line function and it's used in two places, it might be easier for future
developers to understand if the function is in two places.

There is no hard rule about what should or shouldn't be in these libraries, so try to apply your
best judgement.

## Mechanics of sharing

The primary mechanism by which we share code is to use in-repo symlinks. This means that there is
a single copy of the code existing in the repo (no need for prek hook to update other copies,
resulting in larger diffs).

The primary reason we are using this sharing approach, rather than the perhaps more traditional
approach of depending on a shared distribution from PyPI is around compatibility. Let's say that
we have a library to parse AirflowConfig (including all the sources, env, `_cmd` handling, etc
etc.). If that code is used by two distributions that are installed in the same Python
environment, then either we introduce another source of "version hell" or we have to make every
change both backwards and forwards compatible.

Instead, by using an approach similar to vendoring (or if you compare it to C libraries - static
linking), where each dist ships with a copy of the code of the version it knows it works with,
it allows them to co-exist in the same env without problems, and also reduces the cognitive load
on developers when making changes to the shared libs -- as long as it passes the CI it should be
good (and we don't have to test an ever increasing matrix of versions to ensure compatibility.)

### Shared libraries _must_ use relative imports for other shared libraries

The one caveat to this is due to the side-effect that these shared modules are going to be
imported from different locations (for example `airflow._shared.timezones.timezone` and
`airflow.sdk._shared.timezones.timezone`) then any imports inside the shared code to other parts
of shared libraries _must_ make use of relative imports (but only in sources - tests are still
required to use absolute imports).

For example, to use the shared timezone library from another shared library, let's say
`shared/logging/src/airflow_shared/logging/config.py` you would have

```python
from ..timezones.timezone import is_naive  # noqa: TID252
```

This is different to the rest of the airflow codebase where relative imports are not allowed.

### Shared libraries should be grouped one level beneath `airflow_shared/`

In order to make both the relative imports above function and to provide simple structure, we
want to arrange things in single named chunks, where the folder name
is also the "shared library name". For example:

```
shared
├── README.md
└── timezones/
    ├── pyproject.toml
    ├── src/
    │   └── airflow_shared
    │       └── timezones/
    │           ├── __init__.py
    └── tests/
└── logging/
    ├── pyproject.toml
    ├── src/
    │   └── airflow_shared
    │       └── logging/
    │           ├── __init__.py
    └── tests/
```

## Use In Editable installs/Git checkouts

In the editable checkout (which is what you get by default when using `uv sync` etc), the shared
libraries are symlinked directly in place, for example `task-sdk/src/airflow/sdk/_shared/timezones` is
symlinked to `shared/timezones/src/airflow_shared/timezones`. This means that python automatically finds
the files underneath the module as if they lived directly under `airflow.sdk._shared.timezones.X`.


### Including In built distributions

Symlinks will not work when we build the main distributions for PyPI and other channels (since the symlinks
would be to files outside the distribution otherwise). This is specific to sdist; building wheel
resolves the symlinks.

We have a convention that when you include a shared code in your distribution, you should such shared
distributions (full name of the form `apache-airflow-shared-<name>` it to the `tool.airflow` section of
your pyproject.toml:

```toml
[tool.airflow]

shared_distributions = [
     "apache-airflow-shared-timezones",
]
```

This allows `prek` check to automatically verify if the shared distributio is properly configured in
your source tree and `pyproject.toml`.

You should also have `_shared` folder created in your source tree to be able to symlink the shared
distributions code.

Once you do it, prek hook will automatically check and either correct the distribution using the shared one,
but you can also do it manually (for each shared distribution used):

* creating a symlink of the sub-folder of `airflow_shared` in the `shared` folder to the
  corresponding shared distribution, e.g.
   `ln -s ../../shared/timezones/src/airflow_shared/timezones src/airflow/sdk/_shared/timezones`

* adding force-include in your `pyproject.toml` to have hatchling copy the source files instead of
  symling when `.sdist` distribution is built:

```toml
[tool.hatch.build.targets.sdist.force-include]
"../shared/timezones/src/airflow_shared/timezones" = "src/airflow/sdk/_shared/timezones"
```

* prek hook will automatically synchronize all dependencies of the shared distributions to
  your pyproject.toml with appropriate comments allowing it to be automatically synced
  when shared distribution dependencies are updated in the future.
