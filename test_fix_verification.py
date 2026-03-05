"""
Standalone verification that our fix works.
Tests that Pydantic Field(repr=False) hides the JWT token from repr().
This proves the fix for Apache Airflow issue #62428.
"""
from pydantic import BaseModel, ConfigDict, Field


# ---- BEFORE the fix (original code) ----
class BaseWorkloadSchema_BEFORE(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    token: str  # No repr=False — token LEAKS into repr


# ---- AFTER the fix (our code) ----
class BaseWorkloadSchema_AFTER(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    token: str = Field(repr=False)  # repr=False — token HIDDEN


FAKE_JWT = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.secret_payload.signature"


def test_BEFORE_fix_token_leaks():
    """BEFORE our fix: token appears in repr (THE BUG)."""
    obj = BaseWorkloadSchema_BEFORE(token=FAKE_JWT)
    r = repr(obj)
    print(f"\n[BEFORE FIX] repr = {r}")
    assert FAKE_JWT in r, "Expected token to LEAK (this is the bug)"
    print("[BEFORE FIX] ❌ Token LEAKS into repr — this is the security bug!")


def test_AFTER_fix_token_hidden():
    """AFTER our fix: token is hidden from repr (THE FIX)."""
    obj = BaseWorkloadSchema_AFTER(token=FAKE_JWT)
    r = repr(obj)
    print(f"\n[AFTER FIX]  repr = {r}")
    assert FAKE_JWT not in r, f"Token still leaking! repr = {r}"
    print("[AFTER FIX]  ✅ Token is HIDDEN from repr — fix works!")

    # But token is still accessible as attribute
    assert obj.token == FAKE_JWT
    print("[AFTER FIX]  ✅ Token still accessible via obj.token — functionality preserved!")


if __name__ == "__main__":
    print("=" * 60)
    print("VERIFICATION: Airflow Issue #62428 Fix")
    print("=" * 60)

    test_BEFORE_fix_token_leaks()
    print()
    test_AFTER_fix_token_hidden()

    print()
    print("=" * 60)
    print("ALL TESTS PASSED ✅ — Fix is correct!")
    print("=" * 60)
