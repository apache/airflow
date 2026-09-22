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

import pytest
from ci.prek.check_provider_yaml_exception_rationales import main


@pytest.mark.parametrize(
    "key",
    ["min-python-version", "excluded-python-versions", "excluded-platforms"],
)
@pytest.mark.parametrize("has_comment", [True, False])
def test_exception_key_requires_rationale(tmp_path, capsys, key, has_comment):
    provider_yaml = tmp_path / "provider.yaml"
    comment = "# The dependency does not support this environment.\n" if has_comment else ""
    provider_yaml.write_text(f"{comment}{key}: value\n")

    result = main([provider_yaml])

    assert result == (0 if has_comment else 1)
    if not has_comment:
        error = capsys.readouterr().err
        assert str(provider_yaml) in error
        assert key in error
        assert "comment block directly above the key explaining why" in error
