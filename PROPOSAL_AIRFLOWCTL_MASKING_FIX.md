# Proposal: Fix airflowctl Sensitive Data Masking (Issue #59843)

## Problem Statement

**Issue:** #59843 - Remove config sensitive data from airflowctl

The issue requires that **airflowctl should never expose sensitive config data** - all sensitive values should always be masked when retrieved via the API by airflowctl.

## Analysis of Current PR (#59998)

### What the PR Attempts to Fix

1. Modifies `_check_expose_config()` in `airflow-core/src/airflow/api_fastapi/core_api/services/public/config.py`
2. Always returns `False` to mask sensitive data
3. Adds `smtp_password` to the list of sensitive config values in `config.yml`

### Maintainer Feedback

@jscheffl commented: **"The function now lacks a return statement but is declared to return a bool."**

The PR added `display_sensitive` variable initialized to `None`, which causes type checker errors.

### The Deeper Problem

When applying the fix (adding `return False`), the maintainer noted that **"the logic doesn't make sense anymore"**. Here's why:

The API config endpoint is consumed by **multiple clients**, not just airflowctl:

1. **airflowctl** - Should ALWAYS mask sensitive data ✓ (security requirement)
2. **Airflow Web UI** - May legitimately need to display sensitive config values when `expose_config = True` for admin tools
3. **Airflow CLI migration tool** (`airflowctl config lint`) - Uses the API with `display_sensitive=True` to check config changes
4. **Other API consumers** - Rely on `expose_config` setting to control sensitive data exposure

By always returning `False`, we:

- ✅ Fix the security issue for airflowctl
- ❌ Break backward compatibility for Web UI admin tools
- ❌ Break Airflow CLI migration functionality
- ❌ Make the `expose_config` configuration setting meaningless

## Root Cause

The `_check_expose_config()` function cannot distinguish between different API consumers, so it applies the same masking logic to all clients regardless of their intent or security requirements.

## Proposed Solution

**Detect if the API caller is airflowctl via the `User-Agent` header** and only mask sensitive data for airflowctl specifically, while preserving existing behavior for other consumers.

### How airflowctl Identifies Itself

From `airflow-ctl/src/airflowctl/api/client.py:212`:

```python
headers={"user-agent": f"apache-airflow-ctl/{version} (Python/{pyver})"}
```

airflowctl sends a User-Agent header in the format: `apache-airflow-ctl/{version} (Python/{x.y.z})`

## Implementation Plan

### 1. Modify `_check_expose_config()` Function

**File:** `airflow-core/src/airflow/api_fastapi/core_api/services/public/config.py`

```python
from fastapi import HTTPException, Request, status

def _check_expose_config(request: Request | None = None) -> bool:
    """Check if config exposure is allowed and return display_sensitive flag.

    For airflowctl requests, always mask sensitive data regardless of expose_config setting.
    For other requests, respect the expose_config configuration setting.

    Args:
        request: FastAPI Request object to check User-Agent header

    Returns:
        bool: False if sensitive data should be masked, True otherwise

    Raises:
        HTTPException: If expose_config is disabled (403 Forbidden)
    """
    if conf.get("api", "expose_config").lower() == "non-sensitive-only":
        expose_config = True
    else:
        expose_config = conf.getboolean("api", "expose_config")

    if not expose_config:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your Airflow administrator chose not to expose the configuration, most likely for security reasons.",
        )

    # Check if request is from airflowctl via User-Agent header
    if request:
        user_agent = request.headers.get("user-agent", "")
        if "apache-airflow-ctl/" in user_agent:
            # Always mask sensitive data for airflowctl
            return False

    # For other callers, respect the expose_config setting
    if conf.get("api", "expose_config").lower() == "non-sensitive-only":
        return False
    return True
```

**Key Changes:**
- Add optional `request: Request | None = None` parameter
- Check User-Agent header for "apache-airflow-ctl/" string
- Return `False` (mask) for airflowctl
- Return based on `expose_config` setting for other clients

### 2. Update Route Handlers

**File:** `airflow-core/src/airflow/api_fastapi/core_api/routes/public/config.py`

```python
from fastapi import Depends, HTTPException, Request, status

# Update get_config endpoint
def get_config(
    accept: HeaderAcceptJsonOrText,
    section: str | None = None,
    request: Request,  # Add Request parameter
):
    display_sensitive = _check_expose_config(request)

    if section and not conf.has_section(section):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Section {section} not found.",
        )
    conf_dict = conf.as_dict(display_source=False, display_sensitive=display_sensitive)

    # ... rest of function unchanged

# Update get_config_value endpoint
def get_config_value(
    section: str,
    option: str,
    accept: HeaderAcceptJsonOrText,
    request: Request,  # Add Request parameter
):
    _check_expose_config(request)  # Keep call for permission check

    if not conf.has_option(section, option):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Option [{section}/{option}] not found.",
        )

    if (section.lower(), option.lower()) in conf.sensitive_config_values:
        value = "< hidden >"
    else:
        value = conf.get(section, option)

    config = Config(sections=[ConfigSection(name=section, options=[ConfigOption(key=option, value=value)])])
    return _response_based_on_accept(accept, config)
```

**Note:** The `get_config_value` endpoint already masks sensitive values by checking `conf.sensitive_config_values`, so it doesn't use the `display_sensitive` return value from `_check_expose_config()`. This behavior is preserved.

### 3. Keep smtp_password as Sensitive

**File:** `airflow-core/src/airflow/config_templates/config.yml`

Already present in the current PR - no changes needed. The `smtp_password` is correctly marked as `sensitive: true` at line 2061:

```yaml
smtp_password:
  description: |
    The password for the SMTP server, used to authenticate to the server.
  type: string
  example: ~
  default: ""
  sensitive: true
```

The `sensitive_config_values` cached property in `shared/configuration/src/airflow_shared/configuration/parser.py` automatically picks up this marking.

### 4. Add Test Cases

**File:** `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_config.py`

Add new parameterized test to verify airflowctl detection:

```python
@pytest.mark.parametrize(
    ("user_agent", "expose_config", "should_mask_sensitive"),
    [
        ("apache-airflow-ctl/1.0.0", "True", True),  # airflowctl always masks
        ("apache-airflow-ctl/2.0.0", "non-sensitive-only", True),  # airflowctl always masks
        ("apache-airflow-ctl/2.0.0", "False", "should_raise_403"),  # airflowctl respects 403
        ("Mozilla/5.0", "True", False),  # Web browser respects setting
        ("Mozilla/5.0", "non-sensitive-only", True),  # Web browser respects setting
        ("curl/7.68.0", "True", False),  # Other clients respect setting
    ],
)
def test_airflowctl_masks_sensitive_data(
    user_agent, expose_config, should_mask_sensitive, test_client
):
    """Test that airflowctl always masks sensitive data regardless of expose_config setting."""
    config = {("api", "expose_config"): expose_config}

    with conf_vars(config):
        if should_mask_sensitive == "should_raise_403":
            response = test_client.get("/config", headers={"user-agent": user_agent})
            assert response.status_code == 403
        else:
            response = test_client.get("/config", headers={"user-agent": user_agent})
            assert response.status_code == 200

            config_data = response.json()
            smtp_section = next((s for s in config_data["sections"] if s["name"] == "smtp"), None)
            assert smtp_section is not None

            smtp_password_option = next(
                (o for o in smtp_section["options"] if o["key"] == "smtp_password"), None
            )
            assert smtp_password_option is not None

            if should_mask_sensitive:
                assert smtp_password_option["value"] == "< hidden >"
            else:
                # When expose_config=True and not airflowctl, actual value should be shown
                assert smtp_password_option["value"] != "< hidden >"
```

Update existing tests to pass `Request` object or mock it appropriately.

## Testing Strategy

### Unit Tests
1. Test `_check_expose_config()` with various `User-Agent` headers
2. Test with `request=None` (backward compatibility)
3. Test with missing `User-Agent` header
4. Test with malformed `User-Agent` header
5. Verify `expose_config` setting is still respected for non-airflowctl clients

### Integration Tests
1. Test with actual airflowctl client
2. Test with Web UI (simulated via different User-Agent)
3. Test config migration tool functionality

### Regression Tests
1. Ensure all existing tests pass
2. Verify Web UI admin tools still work
3. Verify CLI migration tool (`airflowctl config lint`) still works correctly

### Edge Cases
1. Request without `User-Agent` header
2. Malformed `User-Agent` header
3. `User-Agent` containing "apache-airflow-ctl/" but not actually from airflowctl
4. Concurrent requests from different clients

## Verification Checklist

- [ ] airflowctl requests always mask sensitive data (when `expose_config` is `True`, `False`, or `non-sensitive-only`)
- [ ] Web UI requests respect `expose_config` setting
- [ ] CLI migration tool still works (can display sensitive config when needed)
- [ ] Existing tests pass
- [ ] New tests added for User-Agent detection
- [ ] Type checking passes (mypy, pre-commit hooks)
- [ ] Code follows existing patterns and conventions
- [ ] Documentation updated if needed

## Behavior Matrix

| Client Type | User-Agent | `expose_config` Setting | display_sensitive | Result |
|-------------|-------------|------------------------|------------------|---------|
| airflowctl | `apache-airflow-ctl/*` | `True` | `False` | Masked ✓ |
| airflowctl | `apache-airflow-ctl/*` | `non-sensitive-only` | `False` | Masked ✓ |
| airflowctl | `apache-airflow-ctl/*` | `False` | N/A | 403 Forbidden ✓ |
| Web UI | `Mozilla/*` | `True` | `True` | Not Masked ✓ |
| Web UI | `Mozilla/*` | `non-sensitive-only` | `False` | Masked ✓ |
| Web UI | `Mozilla/*` | `False` | N/A | 403 Forbidden ✓ |
| Other | Any | `True` | `True` | Not Masked ✓ |
| Other | Any | `non-sensitive-only` | `False` | Masked ✓ |
| Other | Any | `False` | N/A | 403 Forbidden ✓ |

## Backward Compatibility

This solution maintains full backward compatibility:

- ✅ Web UI behavior unchanged
- ✅ CLI migration tools continue to work
- ✅ Other API clients unaffected
- ✅ `expose_config` setting remains meaningful for non-airflowctl clients
- ✅ Only airflowctl is affected (new behavior, not a breaking change)

## Open Questions

1. **Newsfragment**: Should we create a newsfragment file (e.g., `59843.significant.rst`) to document this change?

2. **Deprecation Warning**: Should we add a deprecation warning when `expose_config = True` and request is from airflowctl, informing admins that this setting no longer applies to airflowctl?

3. **Explicit Allow**: Is there a need for additional configuration to explicitly allow airflowctl to display sensitive data in certain scenarios (e.g., debugging)?

4. **Security Auditing**: Should we log when airflowctl makes config requests for security auditing purposes?

## Related Files

- `airflow-core/src/airflow/api_fastapi/core_api/services/public/config.py` - Main logic to modify
- `airflow-core/src/airflow/api_fastapi/core_api/routes/public/config.py` - Route handlers to update
- `airflow-core/src/airflow/config_templates/config.yml` - smtp_password already marked sensitive (no change needed)
- `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_config.py` - Tests to add/update
- `shared/configuration/src/airflow_shared/configuration/parser.py` - sensitive_config_values property (no change needed)
- `airflow-ctl/src/airflowctl/api/client.py` - Reference for User-Agent format (no change needed)

## Summary

This proposal addresses the security requirement in issue #59843 (airflowctl must never expose sensitive config data) while preserving backward compatibility and existing functionality for other API consumers. The solution uses the existing `User-Agent` header pattern to detect airflowctl requests and applies appropriate masking logic specifically for airflowctl, maintaining the intended use of the `expose_config` setting for other clients.
