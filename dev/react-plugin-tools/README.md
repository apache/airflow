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

- [React Plugin Development Tools](#react-plugin-development-tools)
  - [Overview](#overview)
  - [Files](#files)
  - [Quick Start](#quick-start)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# React Plugin Development Tools

This directory contains tools for developing React-based Airflow plugins that can be dynamically loaded into the Airflow UI.

## Overview

These tools help you create React plugin projects that:

- Build as libraries compatible with dynamic imports
- Share React instances with the host Airflow application
- Follow Airflow's UI development patterns and standards
- Include proper TypeScript configuration and build setup

## Files

- `bootstrap.py` - CLI tool to create new React plugin projects
- `react_plugin_template/` - Template directory with all the necessary files

## Quick Start

### Create a New Plugin Project

```bash
# From the dev/react-plugin-tools directory
python bootstrap.py my-awesome-plugin

# Or specify a custom directory
python bootstrap.py my-awesome-plugin --dir /path/to/my-projects/my-awesome-plugin
```
