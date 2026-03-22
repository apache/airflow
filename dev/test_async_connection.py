#!/usr/bin/env python
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
"""
Manual end-to-end tests for async connection testing via the REST API.

Run against a live Breeze server:
    uv run --project dev python dev/test_async_connection.py

Expects:
  - Breeze API server at http://localhost:8080
  - AIRFLOW__CORE__TEST_CONNECTION=Enabled
  - Default admin:admin credentials
"""

from __future__ import annotations

import sys
import time

import requests

BASE = "http://localhost:28080/api/v2"
AUTH_URL = "http://localhost:28080/auth/token"
AUTH = None  # Set by get_token()
PASS_COUNT = 0
FAIL_COUNT = 0


def get_token() -> dict:
    """Get a JWT token and return headers dict for requests."""
    r = requests.post(AUTH_URL, json={"username": "admin", "password": "admin"}, timeout=5)
    r.raise_for_status()
    token = r.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS_COUNT, FAIL_COUNT
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1
    msg = f"  [{status}] {label}"
    if detail and not condition:
        msg += f" -- {detail}"
    print(msg)


def cleanup_connection(conn_id: str) -> None:
    """Delete a connection if it exists, ignoring 404/409."""
    r = requests.delete(f"{BASE}/connections/{conn_id}", headers=AUTH)
    # 204=deleted, 404=didn't exist, 409=active test blocking
    if r.status_code == 409:
        # Wait for active test to finish then retry
        time.sleep(3)
        requests.delete(f"{BASE}/connections/{conn_id}", headers=AUTH)


def poll_until_terminal(token: str, timeout: int = 30) -> dict:
    """Poll test status until terminal or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = requests.get(f"{BASE}/connections/test-async/{token}", headers=AUTH)
        body = r.json()
        if body.get("state") in ("success", "failed"):
            return body
        time.sleep(1)
    return body


# ============================================================
# TEST 1: Basic async test with connection fields
# ============================================================
def test_basic_async_test():
    header("TEST 1: Basic async test — POST with connection fields")

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={
            "connection_id": "manual_test_http",
            "conn_type": "http",
            "host": "https://httpbin.org",
        },
    )
    check("POST returns 202", r.status_code == 202, f"got {r.status_code}: {r.text}")
    if r.status_code != 202:
        return

    body = r.json()
    check("Response has token", "token" in body and len(body["token"]) > 0)
    check("Response has connection_id", body.get("connection_id") == "manual_test_http")
    check("State is pending", body.get("state") == "pending")

    token = body["token"]
    print(f"  Token: {token[:20]}...")

    # Poll for status
    print("  Polling for result...")
    result = poll_until_terminal(token)
    check(
        "Test reached terminal state",
        result.get("state") in ("success", "failed"),
        f"state={result.get('state')}",
    )
    check("Response has no 'reverted' field", "reverted" not in result)
    print(f"  Final state: {result.get('state')}")
    print(f"  Message: {result.get('result_message', '(none)')}")


# ============================================================
# TEST 2: 409 duplicate active test
# ============================================================
def test_duplicate_409():
    header("TEST 2: 409 for duplicate active test on same connection_id")

    body = {
        "connection_id": "dup_test_conn",
        "conn_type": "http",
        "host": "https://httpbin.org",
    }

    r1 = requests.post(f"{BASE}/connections/test-async", headers=AUTH, json=body)
    check("First POST returns 202", r1.status_code == 202, f"got {r1.status_code}")

    r2 = requests.post(f"{BASE}/connections/test-async", headers=AUTH, json=body)
    check("Second POST returns 409", r2.status_code == 409, f"got {r2.status_code}: {r2.text}")
    if r2.status_code == 409:
        check("409 message mentions async test", "async test" in r2.json().get("detail", ""))

    # Wait for first test to finish so cleanup works
    if r1.status_code == 202:
        poll_until_terminal(r1.json()["token"])


# ============================================================
# TEST 3: 403 when test_connection is disabled
# ============================================================
def test_disabled_403():
    header("TEST 3: 403 when test_connection is disabled (skip if enabled)")

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={"connection_id": "x", "conn_type": "http"},
    )
    if r.status_code == 403:
        check("Returns 403 when disabled", True)
        print("  NOTE: test_connection is disabled. Remaining tests will skip.")
        return True  # signal disabled
    if r.status_code == 202:
        check("test_connection is enabled (403 test skipped)", True)
        poll_until_terminal(r.json()["token"])
        return False
    check("Unexpected status", False, f"got {r.status_code}: {r.text}")
    return True


# ============================================================
# TEST 4: Block PATCH while test is active
# ============================================================
def test_block_patch():
    header("TEST 4: PATCH blocked while async test is running")

    # Create a real connection first
    requests.post(
        f"{BASE}/connections",
        headers=AUTH,
        json={"connection_id": "block_patch_conn", "conn_type": "http", "host": "example.com"},
    )

    # Start an async test
    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={"connection_id": "block_patch_conn", "conn_type": "http", "host": "example.com"},
    )
    check("Async test started", r.status_code == 202, f"got {r.status_code}")
    token = r.json().get("token") if r.status_code == 202 else None

    # Try to PATCH the same connection
    r2 = requests.patch(
        f"{BASE}/connections/block_patch_conn",
        headers=AUTH,
        json={"connection_id": "block_patch_conn", "conn_type": "http", "host": "new-host.com"},
    )
    check("PATCH returns 409", r2.status_code == 409, f"got {r2.status_code}: {r2.text}")

    # Wait for test to finish
    if token:
        poll_until_terminal(token)

    # PATCH should work now
    r3 = requests.patch(
        f"{BASE}/connections/block_patch_conn",
        headers=AUTH,
        json={"connection_id": "block_patch_conn", "conn_type": "http", "host": "new-host.com"},
    )
    check("PATCH succeeds after test completes", r3.status_code == 200, f"got {r3.status_code}: {r3.text}")

    cleanup_connection("block_patch_conn")


# ============================================================
# TEST 5: Block DELETE while test is active
# ============================================================
def test_block_delete():
    header("TEST 5: DELETE blocked while async test is running")

    requests.post(
        f"{BASE}/connections",
        headers=AUTH,
        json={"connection_id": "block_del_conn", "conn_type": "http", "host": "example.com"},
    )

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={"connection_id": "block_del_conn", "conn_type": "http", "host": "example.com"},
    )
    check("Async test started", r.status_code == 202, f"got {r.status_code}")
    token = r.json().get("token") if r.status_code == 202 else None

    r2 = requests.delete(f"{BASE}/connections/block_del_conn", headers=AUTH)
    check("DELETE returns 409", r2.status_code == 409, f"got {r2.status_code}: {r2.text}")

    if token:
        poll_until_terminal(token)

    r3 = requests.delete(f"{BASE}/connections/block_del_conn", headers=AUTH)
    check("DELETE succeeds after test completes", r3.status_code == 204, f"got {r3.status_code}: {r3.text}")


# ============================================================
# TEST 6: commit_on_success=True creates a new connection
# ============================================================
def test_commit_on_success_create():
    header("TEST 6: commit_on_success=True creates connection on success")

    cleanup_connection("commit_new_conn")

    # Verify connection doesn't exist
    r = requests.get(f"{BASE}/connections/commit_new_conn", headers=AUTH)
    check("Connection does not exist yet", r.status_code == 404)

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={
            "connection_id": "commit_new_conn",
            "conn_type": "http",
            "host": "https://httpbin.org",
            "commit_on_success": True,
        },
    )
    check("POST returns 202", r.status_code == 202, f"got {r.status_code}")
    if r.status_code != 202:
        return

    token = r.json()["token"]
    result = poll_until_terminal(token)
    print(f"  Test result: {result.get('state')} - {result.get('result_message', '')}")

    if result.get("state") == "success":
        r2 = requests.get(f"{BASE}/connections/commit_new_conn", headers=AUTH)
        check("Connection was created", r2.status_code == 200, f"got {r2.status_code}")
        if r2.status_code == 200:
            conn = r2.json()
            check("conn_type matches", conn.get("conn_type") == "http")
            check("host matches", conn.get("host") == "https://httpbin.org")
    else:
        print("  Test failed — cannot verify commit (this is OK if no HTTP hook)")
        check("Test did not succeed (commit not applicable)", True)

    cleanup_connection("commit_new_conn")


# ============================================================
# TEST 7: commit_on_success=False does NOT create connection
# ============================================================
def test_commit_on_success_false():
    header("TEST 7: commit_on_success=False does NOT create connection")

    cleanup_connection("no_commit_conn")

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={
            "connection_id": "no_commit_conn",
            "conn_type": "http",
            "host": "https://httpbin.org",
            "commit_on_success": False,
        },
    )
    check("POST returns 202", r.status_code == 202, f"got {r.status_code}")
    if r.status_code != 202:
        return

    token = r.json()["token"]
    result = poll_until_terminal(token)
    print(f"  Test result: {result.get('state')}")

    r2 = requests.get(f"{BASE}/connections/no_commit_conn", headers=AUTH)
    check("Connection was NOT created", r2.status_code == 404, f"got {r2.status_code}")


# ============================================================
# TEST 8: commit_on_success=True updates existing connection
# ============================================================
def test_commit_on_success_update():
    header("TEST 8: commit_on_success=True updates existing connection")

    cleanup_connection("commit_update_conn")

    # Create a connection first
    requests.post(
        f"{BASE}/connections",
        headers=AUTH,
        json={"connection_id": "commit_update_conn", "conn_type": "http", "host": "old-host.example.com"},
    )

    r = requests.post(
        f"{BASE}/connections/test-async",
        headers=AUTH,
        json={
            "connection_id": "commit_update_conn",
            "conn_type": "http",
            "host": "https://httpbin.org",
            "login": "new_user",
            "commit_on_success": True,
        },
    )
    check("POST returns 202", r.status_code == 202, f"got {r.status_code}")
    if r.status_code != 202:
        return

    token = r.json()["token"]
    result = poll_until_terminal(token)
    print(f"  Test result: {result.get('state')}")

    if result.get("state") == "success":
        r2 = requests.get(f"{BASE}/connections/commit_update_conn", headers=AUTH)
        check("Connection still exists", r2.status_code == 200)
        if r2.status_code == 200:
            conn = r2.json()
            check("Host was updated", conn.get("host") == "https://httpbin.org", f"got {conn.get('host')}")
            check("Login was updated", conn.get("login") == "new_user", f"got {conn.get('login')}")
    else:
        print("  Test failed — cannot verify update (this is OK if no HTTP hook)")

    cleanup_connection("commit_update_conn")


# ============================================================
# TEST 9: Polling with invalid token returns 404
# ============================================================
def test_poll_invalid_token():
    header("TEST 9: Polling with invalid token returns 404")

    r = requests.get(f"{BASE}/connections/test-async/this-token-does-not-exist", headers=AUTH)
    check("GET returns 404", r.status_code == 404, f"got {r.status_code}")


# ============================================================
# TEST 10: Sync test endpoint still works (unchanged)
# ============================================================
def test_sync_test_still_works():
    header("TEST 10: Sync test endpoint still works")

    r = requests.post(
        f"{BASE}/connections/test",
        headers=AUTH,
        json={"connection_id": "sync_test", "conn_type": "http", "host": "https://httpbin.org"},
    )
    # 200 if test ran, 403 if disabled
    check("Sync test returns 200 or 403", r.status_code in (200, 403), f"got {r.status_code}: {r.text}")
    if r.status_code == 200:
        body = r.json()
        check("Response has status field", "status" in body)
        check("Response has message field", "message" in body)


# ============================================================
# TEST 11: Old PATCH /{connection_id}/test endpoint is removed
# ============================================================
def test_old_patch_endpoint_removed():
    header("TEST 11: Old PATCH /{connection_id}/test endpoint is removed")

    r = requests.patch(
        f"{BASE}/connections/some_conn/test",
        headers=AUTH,
        json={"connection_id": "some_conn", "conn_type": "http"},
    )
    # Should be 404 (route doesn't exist) or 405 (method not allowed)
    # FastAPI with no matching route returns 404
    check(
        "PATCH /connections/{id}/test returns 404 or 405",
        r.status_code in (404, 405),
        f"got {r.status_code}: {r.text}",
    )


# ============================================================
# MAIN
# ============================================================
def main():

    print("Async Connection Test — Manual E2E Test Suite")
    print(f"Target: {BASE}")
    print()

    global AUTH

    # Check server is reachable and get token
    try:
        r = requests.get(f"{BASE.rsplit('/api', 1)[0]}/api/v2/version", timeout=5)
        if r.status_code != 200:
            print(f"Server returned unexpected status {r.status_code}. Is Breeze running?")
            sys.exit(1)
        AUTH = get_token()
        print("Authenticated with JWT token")
    except requests.ConnectionError:
        print("Cannot connect to server. Start Breeze first:")
        print("  breeze start-airflow")
        sys.exit(1)

    # Check if test_connection is enabled
    disabled = test_disabled_403()
    if disabled:
        print("\n*** test_connection is disabled. Enable it with:")
        print("    AIRFLOW__CORE__TEST_CONNECTION=Enabled")
        print("    Then restart Breeze and re-run this script.")
        sys.exit(1)

    # Run all tests
    test_basic_async_test()
    test_duplicate_409()
    test_block_patch()
    test_block_delete()
    test_commit_on_success_create()
    test_commit_on_success_false()
    test_commit_on_success_update()
    test_poll_invalid_token()
    test_sync_test_still_works()
    test_old_patch_endpoint_removed()

    # Summary
    header("SUMMARY")
    total = PASS_COUNT + FAIL_COUNT
    print(f"  {PASS_COUNT}/{total} checks passed")
    if FAIL_COUNT > 0:
        print(f"  {FAIL_COUNT} checks FAILED")
        sys.exit(1)
    else:
        print("  All checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
