#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Script to update secrets_masker.py with should_hide_value_for_key method."""

from __future__ import annotations

import re

# Read the file
with open("src/airflow_shared/secrets_masker/secrets_masker.py") as f:
    content = f.read()

# 1. Add the method to the class (before add_mask)
method_to_add = '''    def should_hide_value_for_key(self, name):
        """
        Return if the value for this given name should be hidden.

        Name might be a Variable name, or key in conn.extra_dejson, for example.
        """
        from airflow import settings

        if isinstance(name, str) and settings.HIDE_SENSITIVE_VAR_CONN_FIELDS:
            name = name.strip().lower()
            return any(s in name for s in self.sensitive_variables_fields)
        return False

'''

# Find the add_mask method and add the new method before it
content = re.sub(
    r"(\s+def add_mask\(self, secret: JsonValue, name: str \| None = None\):)", method_to_add + r"\1", content
)

# 2. Update the module-level function to use the global singleton
content = re.sub(
    r'def should_hide_value_for_key\(name\):\s*\n\s*""".*?""".*?\n\s*from airflow import settings\s*\n\s*if isinstance\(name, str\) and settings\.HIDE_SENSITIVE_VAR_CONN_FIELDS:\s*\n\s*name = name\.strip\(\)\.lower\(\)\s*\n\s*return any\(s in name for s in _secrets_masker\(\)\.sensitive_variables_fields\)\s*\n\s*return False',
    '''def should_hide_value_for_key(name):
    """
    Return if the value for this given name should be hidden.

    Name might be a Variable name, or key in conn.extra_dejson, for example.
    """
    return _secrets_masker().should_hide_value_for_key(name)''',
    content,
    flags=re.DOTALL,
)

# 3. Update add_mask method to use self.should_hide_value_for_key
content = re.sub(
    r"if pattern not in self\.patterns and \(not name or should_hide_value_for_key\(name\)\):",
    "if pattern not in self.patterns and (not name or self.should_hide_value_for_key(name)):",
    content,
)

# 4. Update _redact method to use self.should_hide_value_for_key
content = re.sub(
    r"if name and should_hide_value_for_key\(name\):",
    "if name and self.should_hide_value_for_key(name):",
    content,
)

# 5. Update _redact method for V1EnvVar case
content = re.sub(
    r'if should_hide_value_for_key\(tmp\.get\("name", ""\)\) and "value" in tmp:',
    'if self.should_hide_value_for_key(tmp.get("name", "")) and "value" in tmp:',
    content,
)

# 6. Update _merge method to use self.should_hide_value_for_key
content = re.sub(
    r"is_sensitive = force_sensitive or \(name is not None and should_hide_value_for_key\(name\)\)",
    "is_sensitive = force_sensitive or (name is not None and self.should_hide_value_for_key(name))",
    content,
)

# Write the updated content
with open("src/airflow_shared/secrets_masker/secrets_masker.py", "w") as f:
    f.write(content)

print("Updated secrets_masker.py successfully!")
