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

## Why symbolic links here ?

The sub-folders you see in this folder are symbolic links to the actual code in the `shared` folder.
The reason we are doing it is that we want to be able to use the shared libraries in different
distributions potentially in different versions - when several packages are using the same shared library.

Python - unlike for example npm - does not have a way to install different versions of the same distribution,
so what we effectively have to do is to vendor-in those shared libraries in different packages that are
using those libraries.

By employing symbolic links we can avoid code duplication (single source of current version of the shared
library code is stored in "shared" folder) - and at the same time we can have different versions of the
same shared library in different packages when for example `airflow-core` and `task-sdk` package are
installed together in different version.

You can read about it in [the shared README.md](../../../../shared/README.md) document.
