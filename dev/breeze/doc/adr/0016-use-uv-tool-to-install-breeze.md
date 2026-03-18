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

- [10. Use uv tool to install breeze](#10-use-uv-tool-to-install-breeze)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 10. Use uv tool to install breeze

Date: 2024-11-11

## Status

Accepted

Supersedes [10. Use pipx to install breeze](0010-use-pipx-to-install-breeze.md)

## Context

The ``uv`` tools is a new modern python development environment management tool
and we adopt it in ``Airflow``  as recommended way to manage airflow local virtualenv and development
setup. It's much faster to install dependencies with ``uv`` than with ``pip`` and it has many
more features - including managing python interpreters, workspaces, syncing virtualenv and more.

## Decision

While it is still possible to install breeze using ``pipx``, we are now recommending ``uv`` and specifically
``uv tool`` as the way to install breeze. Contributors should use ``uv tool`` to install breeze.

## Consequences

Those who used ``pipx``, should clean-up and reinstall their environment with ``uv``.
