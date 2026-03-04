#
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

from tests_common.test_utils.fernet import generate_fernet_key_string


class TestFernetUtils:
    """Test utils for Fernet encryption."""

    def test_generate_fernet_key_string(self):
        """Test generating a Fernet key."""
        key = generate_fernet_key_string("TEST_KEY")
        assert key == "NBJC_zYX6NWNek9v7tVv64YZz4K5sAgpoC4WGkQYv6I="
        default_key = generate_fernet_key_string()
        assert default_key == "BMsag_V7iplH1SIxzrTIbhLRZYOAYd6p0_nPtGdmuxo="
