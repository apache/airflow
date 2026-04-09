 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [IDE Setup](#ide-setup)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# IDE Setup

When the user wants to open the project in IntelliJ IDEA or PyCharm (e.g. to review
a PR, inspect code, or work on changes), run:

```bash
uv run dev/ide_setup/setup_idea.py --confirm --open-ide
```

This regenerates the IDE configuration files without prompts (`--confirm`) and
opens the IDE in the project directory automatically (`--open-ide`).
