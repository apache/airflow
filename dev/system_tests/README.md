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
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Small tool to update status of all AIP-47 issues.

Simply:

1) Set `GITHUB_TOKEN` to a repo-writeable token.

2) Run the script via `uv` (it pulls in the few third-party deps it
   needs — `PyGithub`, `rich-click`, `rich`):

```bash
uv run --with PyGithub --with rich-click --with rich python dev/system_tests/update_issue_status.py
```


It will automatically update status of all the AIP-47 issues based on the presence of files in the repo.

You can also run it with ``--dry-run`` mode - useful when you have no write permission to the repository.
You can generate a read-only token (or hope you will not hit rate limits if you do not have one)
In this case it will only show what would have happened if you had  the access and print stats.
