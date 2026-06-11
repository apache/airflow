<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [dev/ directory guidelines](#dev-directory-guidelines)
  - [Scripts](#scripts)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# dev/ directory guidelines

## Scripts

New scripts in `dev/` must be standalone Python scripts (not bash). Each script must include
[inline script metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/)
placed **after** the Apache License header, so that `uv run` can execute it without any prior
installation:

```python
#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) ...
#   http://www.apache.org/licenses/LICENSE-2.0
# ...
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "some-package",
# ]
# ///
```

If the script only uses the standard library, omit the `dependencies` key but keep the
`requires-python` line.

Run scripts with:

```shell
uv run dev/my_script.py [args...]
```

Document `uv run` (not `python`) as the invocation method in READMEs and instructions.
