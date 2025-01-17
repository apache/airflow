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

# Keeping the API of common.sql provider backwards compatible

The API of the `common.sql` provider should be kept backwards compatible with previously released versions.
The reason is - there are already released providers that use `common.sql` provider and rely on the API and
behaviour of the `common.sql` provider, and any updates in the API or behaviour of it, might potentially
break those released providers.

Therefore, we should keep an extra care when changing the APIs.

The approach we take is similar to one that has been applied by Android OS team to keep their API in check,
and it is based on storing the current version of API and flagging changes that are potentially breaking.
This is done by comparing the previous API (store in stub files) and the upcoming API from the PR.
The upcoming API is automatically extracted from `common.sql` Python files using `update-common-sql-api-stubs`
pre-commit using mypy `stubgen` and stored as `.pyi` files in the `airflow.providers.common.sql` package.
We also post-process the `.pyi` files to add some historically exposed methods that should be also
considered as public API.

If the comparison determines that the change is potentially breaking, the contributor is asked
to review the changes and manually regenerate the stub files.

The details of the workflow are as follows:

1) The previous API is stored in the (committed to repository) stub files.
2) Every time when common.sql Python files are modified the `update-common-sql-api-stubs` pre-commit
  regenerates the stubs (including post-processing it) and looks for potentially breaking changes
   (removals or updates of the existing classes/methods).
3) If the check reveals there are no changes to the API, nothing happens, pre-commit succeeds.
4) If there are only additions, the pre-commit automatically updates the stub files,
   asks the contributor to commit resulting updates and fails the pre-commit. This is very similar to
   other static checks that automatically modify/fix source code.
5) If the pre-commit detects potentially breaking changes, the process is a bit more involved for the
   contributor. The pre-commit flags such changes to the contributor by failing the pre-commit and
   asks the contributor to review the change looking specifically for breaking compatibility with previous
   providers (and fix any backwards compatibility). Once this is completed, the contributor is asked to
   manually and explicitly regenerate and commit the new version of the stubs by running the pre-commit
   with manually added environment variable:

```shell
UPDATE_COMMON_SQL_API=1 pre-commit run update-common-sql-api-stubs
```

# Verifying other providers to use only public API of the `common.sql` provider

MyPy automatically checks if only the methods available in the stubs are used. This gives enough protection
for most "static" usages of the `common.sql` provider by any other provider.

Of course, this is never 100% fool-proof, due to the dynamic Python behaviours and due to possible internal
changes in behaviours of the common.sql APIs, but at the very least any of "interface" changes will be
flagged in any change to contributor making a change and also reviewers will be notified whenever any
API change happens.

# External usage, tracking API changes

The `api.txt` file might be used by any external provider developer to make sure that only official
APIs are used,  which will also help those external providers to use the `common.sql` provider
functionality without the fear of using methods and classes that are deemed non-public.

Since this file is stored in the repository, we can automatically generate the API differences between
future released version of `common.sql` provider, and we will be able to easily reason about changes of the
API between versions.

# Possible future work

This mechanism providing the "official" API of the `common.sql` provider is just the first attempt to
standardise APIs provided by various Airflow's components. In the future, we might decide to extend
after experimenting and testing its behaviour for the `common.sql`.

## Other providers

Keeping a formal, automatically generated and maintained stubs is a good thing. We might simply decide
to generate the stub files for all our providers - so that our users and cross-provider dependencies might
start relying on stability of the APIs in the provider. This will be just a simple extension of the current
approach - we will not limit stub generation to `common.sql` but we will add it to all providers.

## Airflow Core

We should be able to use very similar mechanism for exposing and flagging the public API of Airflow's core
and flag any misuse of methods in the Community providers

As a follow-up of those two - we should be able to produce more "formal" API that providers written
externally could utilise MyPy as "compatibility tool" that will be able to verify if the provider
written externally is compatible (on API level) with certain version of Airflow, or in case of the
cross-provider dependencies whether it is compatible (on API level) with certain versions of the released
providers.
