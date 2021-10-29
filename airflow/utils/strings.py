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
"""Common utility functions with strings"""

import string
from random import choice
from typing import Optional


def get_random_string(length=8, choices=string.ascii_letters + string.digits):
    """Generate random string"""
    return ''.join(choice(choices) for _ in range(length))


TRUE_LIKE_VALUES = {"on", "t", "true", "y", "yes", "1"}


def to_boolean(astring: Optional[str]) -> bool:
    """Convert a string to a boolean."""
    if astring is None:
        return False
    if astring.lower() in TRUE_LIKE_VALUES:
        return True
    return False
