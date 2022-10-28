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


def test_incorrect_endpoint_should_return_json(minimal_app_for_api):
    client = minimal_app_for_api.test_client()

    # Given we have application with Connexion added
    # When we are hitting incorrect endpoint in API path

    resp = client.get("/api/v1/incorrect_endpoint")

    # Then we have parsable JSON as output

    assert "Not Found" == resp.json["title"]
    assert 404 == resp.json["status"]
    assert 404 == resp.status_code


def test_incorrect_endpoint_should_return_html(minimal_app_for_api):
    client = minimal_app_for_api.test_client()

    # When we are hitting non-api incorrect endpoint

    resp = client.get("/incorrect_endpoint")

    # Then we do not have JSON as response, rather standard HTML

    assert resp.json is None
    assert resp.mimetype == "text/html"
    assert resp.status_code == 404


def test_incorrect_method_should_return_json(minimal_app_for_api):
    client = minimal_app_for_api.test_client()

    # Given we have application with Connexion added
    # When we are hitting incorrect HTTP method in API path

    resp = client.put("/api/v1/version")

    # Then we have parsable JSON as output

    assert "Method Not Allowed" == resp.json["title"]
    assert 405 == resp.json["status"]
    assert 405 == resp.status_code


def test_incorrect_method_should_return_html(minimal_app_for_api):
    client = minimal_app_for_api.test_client()

    # When we are hitting non-api incorrect HTTP method

    resp = client.put("/")

    # Then we do not have JSON as response, rather standard HTML

    assert resp.json is None
    assert resp.mimetype == "text/html"
    assert resp.status_code == 405
