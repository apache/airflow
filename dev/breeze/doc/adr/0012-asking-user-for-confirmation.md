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

- [12. Asking user for confirmation](#12-asking-user-for-confirmation)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 12. Asking user for confirmation

Date: 2022-04-28

## Status

Accepted

## Context

When we run a number of breeze commands, we sometimes ask the user for confirmation. This happens in a few
cases:

1. When the user attempts to run a potentially disruptive operation (cleanup, delete, modifying startup
   scripts)
2. When the user attempts to run a potentially long operation or operation that needs fast internet access
3. When an action generates an auxiliary action that we determined we need to run in order to make sure
   we use the latest version of the environment (like upgrading breeze dependencies or pulling and building
   latest image).

In case 1) - we should present the user with information on what is going to change and the user should
not be surprised with the effect of it.

In case 2) - we should not break the workflow of the user, unless this is really necessary or the user
chooses to break the workflow. This is important in situation when the user needs to perform an action
quickly and disrupting the workflow would be annoying and undesired. In this case the confirmation of
the "long" action should not be "necessary" - when user does not expect the action to wait for confirmation.

In case 3) - we want to gently nudge the user to run an action when we guess it is advised. We should not
break the regular workflows, but we want the user to "eventually" run the upgrade to use the same
environment as others (and avoiding the problem where thing "work for me but not the others".

Breeze is also used for unattended actions - and then any questions would prevent the actions from running,
so it should be possible to disable the questions and assume an answer in those cases.

## Decision

We take the following approach with the confirmation:

1) If there is a potentially disruptive command - we always ask the user for confirmation and explicit
confirmation is necessary. Default action in this case is "quit" so if the user just presses enter
the operation should quit without making any change.

2) If there is a long-running (but not disruptive) operation that user directly requested (for example
building and image by `ci-image build` command), there should be no question asked unless there is some
condition that could change the length of the action (for example when we realise that the user have not
rebased to the latest main and rebuilding the image might take far longer than initially anticipated by
the user). In case of such unexpected condition the default answer should be "quit" as well without timeout.

3) If there is an auxiliary actions (for example building image) that would prevent (or delay) another action
of the user - the default action should be "no" (no upgrade) and there should be a short timeout (few seconds)
so that the original action can be executed without too much waiting.
In all cases an upgrade needs an explicit 'y' answer by the user.

Pressing "ENTER" when asked triggers the default action if it is specified.

User can "force" any answer by adding `--answer <ANSWER>` command line switch to any command line that
might ask a question - and it will force `<ANSWER>` as an immediate action to all questions.

Default action (if used) is capitalised in allowed answers. Timeout is displayed when present.

## Consequences

As a consequence, the user will not accidentally make any disruptive changes without confirming it. The user
will not have a broken workflow even if upgrade is recommended - they will just get a short annoying delay and
question asked whether to upgrade, but the upgrade will not run without the user's consent. The short
delay will introduce a gentle "nagging" of the user to upgrade their breeze or image which will lead to
"eventually consistent" versions of both for the users.
