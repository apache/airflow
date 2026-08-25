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

- [Testing and validation](#testing-and-validation)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Testing and validation

- Add a colocated `.test.tsx` file for new components and changed behavior.
- Use Vitest and Testing Library to test behavior visible to users. Prefer accessible queries and
  user interactions over implementation details.
- Cover relevant loading, error, empty, and success states as well as important interactions.
- Do not test React, Chakra UI, React Router, or other third-party internals.
- Keep tests deterministic and restore mocks between tests.
- Before finishing a change, run `pnpm lint`, `pnpm test`, and `pnpm build`. Run `pnpm format` when
  source formatting changes.
