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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Moving providers to new structure](#moving-providers-to-new-structure)
  - [How to use the script](#how-to-use-the-script)
  - [Options](#options)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Moving providers to new structure

We are moving providers to a new structure, where each provider has a separate sub-project in
"providers" sub-folder.

This means that we need to migrate some 90+ providers to the new structure. This is a big task and while we
could do it in one huge PR, it would be disruptive and likely take some time to review and fix some individual
edge-cases - even if we have automated most of the work.

This directory contains a script that contributors can use to move a provider (or a few providers to the
new structure as a starting point for their PR. Most of the work is automated, but there will be likely
some manual adjustments needed in more complex cases.

## How to use the script

The script follows https://peps.python.org/pep-0723/ and uses inlined dependencies - so it can be run as-is
by modern tools without creating dedicated virtualenv - the virtualenv with dependencies is
created on-the-fly by PEP 723 compatible tools. For example one can use uv to run it:

```shell
uv run dev/moving_providers/move_providers.py alibaba
```

## Options


> [!NOTE]
> You can see all the options by running the script with `--help` option:
>
> ```shell
> uv run dev/moving_providers/move_providers.py --help
> ```

By default the script runs in `--dry-run` mode, which means it will not make any changes to the file system,
but will print what it would do. To actually move the files, you need to pass `--no-dry-run` option and you
will be asked to commit the code and create a PR:

```shell
uv run dev/moving_providers/move_providers.py alibaba --no-dry-run
```

You can specify multiple providers to move in one go:

```shell
uv run dev/moving_providers/move_providers.py alibaba amazon  microsoft.azure
```

You can specify `--verbose` option to see more details about what the script is doing:

```shell
uv run dev/moving_providers/move_providers.py alibaba --verbose
```

You can also specify `--quiet` option to see less output:

```shell
uv run dev/moving_providers/move_providers.py alibaba --quiet
```
