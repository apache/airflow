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

This folder contains code that is shared across two or more of the Airflow distributions.

## Be Thoughtful about what you add under here

Not every piece of code that is used in two distributions should be automatically placed in one of the shared libraries, and sometimes "just duplicate it" is the right approach to take. For example if it's just a 5 or 10 line function and it's used in two places, it might be easier to future developers to understand if the function is in two places.

There is no hard rules about what should or shouldn't be in these libraries, so try to apply your best judgement.

## Vendoring process

i.e. how this code ends up in distributions

One of the desires we had when setting up this shared code process was to only have one copy of the source in the repo; while we could have used a pre-commit check to ensure that other copies were kept up to date, and that approach would have worked, it is "a bit messier" and makes PRs look scarier, so we have an process that has only a single copy of the code in the repo.

### In built distributions

We make use of [hatch-build-time-vendoring] to automatically run [vendoring] for us when the `sdist` is built (and then the wheel is automatically built using that already vendored sdist).

`vendoring` is a project from the pip maintainers and is how pip vendors code into it's tree. Now the eagle-eyed amongst you will notice that the project description essentially says "do not use this if you aren't pip", but in this specific case (of vendoring in _our own code_) none of the specific warnings or caveats apply to us.

The main thing the vendoring tool does for us is that it will copy all of the python code out of the source distribution (i.e. `shared/core-and-tasksdk/src/airlfow_shared/*.py`), place it in the target namespace (i.e. `task-sdk/src/airflow/sdk/_vendor/airflow_shared/`) and then most usefully, it will automatically re-write the imports so they use the new names.

### At dev time/in an editable checkout

When you are running from your Airflow checkout, or within a breeze shell, the vendored code does not exist (in fact, almost the entire content of the `_vendor/` is ignored from git), so in order for Python to load the shared modules we make use of a [meta path finder].

Importing `airflow.sdk._vendor` (and similarly in `airflow._vendor`) will have the side-effect of registering a new MetaPathFinder that will look for the known sub-modules, and telling the rest of the python import machinery that `$reop/shared/core-and-tasksdk/src/airflow_shared/__init__.py` is where to find that module. The main thing to note here is that that finder is only called if there the shared library as it exists in the repo is already in the sys.path, so this will only be hit in development.

One extra thing to note here, is that in order for both IDEs and type checkers to find the "other" copy of the shared library, we commit the typestub files that vendoring generates.

[hatch-build-time-vendoring]: https://pypi.org/project/hatch-build-time-vendoring/
[vendoring]: https://pypi.org/project/vendoring/
[meta path finder]: https://docs.python.org/3/glossary.html#term-meta-path-finder
