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

import unittest

import jmespath
from tests.helm_template_generator import render_chart


class DagsPersistentVolumeClaimTest(unittest.TestCase):
    def test_should_not_generate_a_document_if_persistence_is_disabled(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": False}}},
            show_only=["templates/dags-persistent-volume-claim.yaml"],
        )

        self.assertEqual(0, len(docs))

    def test_should_not_generate_a_document_when_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}}},
            show_only=["templates/dags-persistent-volume-claim.yaml"],
        )

        self.assertEqual(0, len(docs))

    def test_should_generate_a_document_if_persistence_is_enabled_and_not_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": None}}},
            show_only=["templates/dags-persistent-volume-claim.yaml"],
        )

        self.assertEqual(1, len(docs))

    def test_should_set_pvc_details_correctly(self):
        docs = render_chart(
            values={
                "dags": {
                    "persistence": {
                        "enabled": True,
                        "size": "1G",
                        "existingClaim": None,
                        "storageClassName": "MyStorageClass",
                        "accessMode": "ReadWriteMany",
                    }
                }
            },
            show_only=["templates/dags-persistent-volume-claim.yaml"],
        )

        self.assertEqual(
            {
                "accessModes": ["ReadWriteMany"],
                "resources": {"requests": {"storage": "1G"}},
                "storageClassName": "MyStorageClass",
            },
            jmespath.search("spec", docs[0]),
        )
