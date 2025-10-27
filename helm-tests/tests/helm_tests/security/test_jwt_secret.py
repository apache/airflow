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

import base64

import jmespath
from chart_utils.helm_template_generator import render_chart


class TestJwtSecret:
    """Tests jwt secret."""

    def test_should_add_annotations_to_jwt_secret(self):
        """Test that annotations are properly added to JWT secret and the secret is correctly encoded.

        This test verifies that:
        1. Custom annotations can be added to the JWT secret
        2. The JWT secret is properly base64 encoded in the template
        3. The decoded secret matches the input value
        """
        # Create JWT secret with custom annotation
        docs = render_chart(
            values={
                "jwtSecret": "verysecret",
                "jwtSecretAnnotations": {"test_annotation": "test_annotation_value"},
            },
            show_only=["templates/secrets/jwt-secret.yaml"],
        )[0]
        assert "annotations" in jmespath.search("metadata", docs)
        assert jmespath.search("metadata.annotations", docs)["test_annotation"] == "test_annotation_value"

        # Extract and decode the JWT secret
        jwt_secret_b64 = jmespath.search('data."jwt-secret"', docs).strip('"')
        jwt_secret = base64.b64decode(jwt_secret_b64).decode()

        # Verify the decoded secret matches original input
        assert jwt_secret == "verysecret"
