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
from __future__ import annotations

import re
from os.path import dirname, join


def remove_license_header(content: str) -> str:
    """Remove license header from the given content."""
    # Define the pattern to match both block and single-line comments
    pattern = r"(/\*.*?\*/)|(--.*?(\r?\n|\r))|(#.*?(\r?\n|\r))"

    # Check if there is a license header at the beginning of the file
    if re.match(pattern, content, flags=re.DOTALL):
        # Use re.DOTALL to allow .* to match newline characters in block comments
        return re.sub(pattern, "", content, flags=re.DOTALL).strip()
    return content.strip()


def load_file(*args: str, mode="r", encoding="utf-8"):
    with open(join(dirname(__file__), *args), mode=mode, encoding=encoding) as file:
        if mode == "r":
            return remove_license_header(file.read())
        return file.read()
