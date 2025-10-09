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

- [10. Use pipx to install breeze](#10-use-pipx-to-install-breeze)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 10. Use pipx to install breeze

Date: 2022-04-04

## Status

Accepted

Supersedes [3. Bootstrapping virtual environment](0003-bootstrapping-virtual-environment.md)

## Context

We are close to release the new Breeze to the users, and we need to make
final decision on how the users will be running it. While initially,
during development it was really nice to have bootstrapping script and the
script was similar to what we had in the original Shell Breeze, this has
one negative consequence - click-complete only works when package is
locally installed via entrypoint and present in the path. This means that if
the package is not installed and not available in the PATH, autocomplete will
not work.

There could be a workaround of modifying the autocomplete scripts (to inject the
breeze bootstrap script on the path for example).

The new Breeze already supports running from different repositories even if it
is installed from another repository. The only real difference between `pipx`
installed Breeze and bootstrapped are:

1. you can use different versions of Breeze in different repos
2. you can automatically install new version of dependencies when Breeze starts

The 1. is only for Power Users who clone multiple versions of repos and want to
use them interchangeably. Arguably we do not expect Breeze in the future to
differ significantly in the basic functions. As a possible solution for Power
users it's also ok to print them warning in case it turns out that Breeze they
are using is run in a different source tree - including information on how they
can update it - this can always be done with

`pipx install -e ./dev/breeze/ --force`.

The 2. is a two-sided sword. In some cases user might not be able to upgrade
some dependencies, or they might break Breeze when installed, but they might
want to be able to run Breeze regardless, even if dependencies are out-dated. On
the other hand if we do not upgrade, strange error messages might appear, and
we need to explain the user to also reinstall Breeze with `--force`. As a
possible solution we could also inform the users that they need to update in
case we detect that the installation config has changed - warning the
user and telling the exact command to run in order to fix the problem.

There is another risk, that the user will install Breeze without
the `-e` flag - then the installed version of Breeze will not be updated
when Airflow source code changes. This can be mitigated by checking if
the installed sources of `breeze.py` are actually installed from Airflow
source tree. If not - we can fail with appropriate message and instructions
for the user.

## Decision

It seems that we should be able to detect both situation which we currently
make use of the bootstrapping script for, and since we are entering a more
stable stage of Breeze usage, using `pipx` as the only solution to install
Breeze is a better approach (if accompanied by the detection and user
guidance). We can also provide helpful guidance for the current users
of breeze, by simply offering them a way to install breeze via pipx and
redirecting the command to breeze on path if we detect breeze is already
installed. Later we can remove even that redirection and replace it with
instructions how to run breeze from the path.

## Consequences

There will be one and only one way of installing Breeze. User will get less
confused which version to use. However, they will have to install one more
tool (pipx) to install Breeze. This will be an extra step of installation,
but we can automate it when the current Breeze users will continue use
the old ./breeze for a while until we deprecate and finally remove it.
