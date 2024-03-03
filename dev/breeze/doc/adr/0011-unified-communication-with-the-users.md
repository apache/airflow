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

- [11. Unified communication with the users](#11-unified-communication-with-the-users)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 11. Unified communication with the users

Date: 2022-04-27

## Status

Accepted

## Context

We communicate with the user via messages printed in get_console() And we should
make sure we communicate the msssages in a consistent way so that the users would
understand:

* whether verification of a condition succeeded
* whether a message is just informational that might be used to understand what's going on
* whether there is some important warning to potentially act on
* whether there is a definite error that the user should fix
* instructions how to proceed

The messages should be:

* short
* informative
* readable
* actionable

## Decision

We are using rich and colours to communicate the type of messages:

* [success] - to show verification success (green)
* [info] - to present inform users (bright_blue)
* [warning] - to print warning that the user probably should read and act on (bright_yellow)
* [error] - when there is an error that the user should fix (red)
* instructions should be printed without style and default rich rendering should be used

By default, we map those styles to those colors, but we can change configuration of Breeze to
be colour-blind-friendly by disabling colours in communication via `breeze setup config --no-colour`.

When signalling warning or error The communication should not be confusing and
should not contain "noise". It should contain simple - usually one-liner -
information on what's wrong or what the user is warned against, possibly
followed by instructions telling the user what to do. When we are able to
foresee that error will happen, we should print the message only with
explanation, and we should suppress any stack-traces as they are not really
useful for the users. When an error is unexpected, full stack trace should
be written to allow easier investigation of the problem.

We should also support `--verbose` flag to display all the commands being executed, and
the --dry-run flag to only display commands and not execute the commands. This will allow to
diagnose any problems that the user might have.

Whenever we print a logically separated message we should add EOL to separate the logical part of the
message as well as when you want to separate the header description from bullet lists (very similar to
what .rst rules are for readability). Whitespace improves readability of communication and messages
separated in sections separated by whitespace and marked with colors are much more readable than the
wall of text without visible structure.

## Consequences

Users will get consistent approach when it comes to context and expectations of their reaction for the
messages. Developers will use consistent styles.
