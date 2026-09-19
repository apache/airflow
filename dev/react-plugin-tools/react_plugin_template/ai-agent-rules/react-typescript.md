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

- [React and TypeScript](#react-and-typescript)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# React and TypeScript

- Write function components and hooks. Do not introduce class components.
- Keep TypeScript strict. Define explicit component props and domain types; do not use `any` or
  suppress type errors.
- Use the configured `src` path alias instead of deep relative imports.
- Calculate derived values while rendering instead of synchronizing them with `useState` and
  `useEffect`.
- Use `useEffect` only to synchronize with an external system. Keep dependencies complete and make
  cleanup symmetrical with setup.
- Handle loading, error, empty, and success states for asynchronous work.
- Preserve keyboard behavior, focus management, accessible names, and semantic elements when adding
  or changing interactions.
- Keep reusable components focused and place shared application code under `src/` rather than in the
  library entry point.
