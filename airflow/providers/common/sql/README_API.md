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
and it is based on storing the current varsion of API and flagging changes that are potentially breaking.
This is done by comparing the previous API (store in [api.txt](api.txt) and the upcoming API from the PR.
The upcoming API is automatically extracted from `common.sql` Python files using `update-common-sql-api`
pre-commit. If the comparison determines that the change is potentially breaking, the contributor is asked
to review the changes and manually regenerate the [api.txt](api.txt).

The details of the workflow are as follows:

1) The previous API is stored in the (committed to repository) [api.txt](api.txt) file in this folder.
2) Every time when common.sql Python files are modified the `update-common-sql-api` pre-commit regenerates
   the API file content and looks for potentially breaking changes are detected (removals or updates of
   the existing classes/methods.
3) If the check reveals there are no changes to the API, nothing happens.
4) If there are only additions, the pre-commit automatically updates the [api.txt](api.txt) file,
   asks the contributor to commit resulting update to the [api.txt](api.txt) and fails the pre-commit. This
   is very similar to other static checks that automatically modify/fix source code.
5) If the pre-commit detects potentially breaking changes, the process is a bit more involved for the
   contributor. The pre-commit flags such changes to the contributor by failing the pre-commit and
   asks the contributor to review the change looking specifically for breaking compatibility with previous
   providers (and fix any backwards compatibility). Once this is completed, the contributor is asked to
   manually and explicitly regenerate and commit the new version of the API by running the pre-commit
   with manually added environment variable: `UPDATE_COMMON_SQL_API=1 pre-commit run update-common-sql-api`.

# Verifying other providers to use only public API of the `common.sql` provider

The file which holds the current API is used by a second pre-commit - `check-common-sql-api-usage`.

This pre-commit does not do any update - it only checks that any of the providers uses only the
public API stored in the [api.txt](api.txt) when accessing `common.sql` provider. The reason is that some
methods or classes in the common.sql are not supposed to be part of the API - there are helper methods
and classes. Those methods and classes are usually starting with `_` (protected methods) and they are
excluded.

However - historically - some protected methods have been used, so until we determine it is
safe to remove them, the pre-commit will continue to manually select them as part of the API and they
are protected by this mechanism. This way we can be more certain that the community providers that use
`common.sql` provider only use the APIs that are intended to be used as public.

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

Keeping a formal, automatically generated and maintained list of official classes and methods that
providers can use. Once we get used to it and confirm that it is a good mechanism, we will be able
to extend this approach to more cases:

* for other cross-provider-dependencies, we will be able to flag potentially breaking changes between them
* we should be able to use very similar mechanism for exposing and flagging the public API of Airflow's core
  and flag any misuse of methods in the Community providers
* as a follow-up of those two - we should be able to produce more "formal" API that providers written
  externally could use and write a "compatibility tool" that will be able to verify if the provider
  written externally is compatible (on API level) with certain version of Airflow, or in case of the
  cross-provider dependencies whether it is compatible (on API level) with certain versions of the released
  providers.
