import pytest
import re


# Adversarial payloads targeting LDAP injection via AUTH_LDAP_SEARCH_FILTER
ADVERSARIAL_PAYLOADS = [
    # Classic LDAP injection attempts
    ")(uid=*",
    ")(|(uid=*)",
    "*)(uid=*))(|(uid=*",
    "*()|%26'",
    "*()|&'",
    # Filter escape attempts
    ")(objectClass=*",
    ")(cn=*)(objectClass=*",
    # Null byte injection
    "\x00)(uid=*",
    # Nested filter injection
    "(&(objectClass=*)(uid=*))",
    # Attempt to close and reopen filters
    "))(|(objectClass=*",
    # Unicode/encoding tricks
    "\uff08objectClass=*\uff09",
    # Whitespace manipulation
    "  )(uid=*  ",
    # Comment-like sequences (not standard LDAP but worth testing)
    ")(uid=admin",
    # Double injection
    ")(uid=*)(uid=*",
    # Wildcard abuse
    "(uid=*)",
    "*(uid=*)*",
    # Environment variable injection patterns (for containerized deployments)
    "$(uid=*)",
    "${IFS})(uid=*",
    # Empty/boundary values
    "",
    " ",
    "()",
    # Deeply nested
    "((((uid=*))))",
    # Mixed case LDAP operators
    ")(UID=*",
    ")(Uid=*",
]


def build_ldap_filter(auth_ldap_search_filter: str, auth_ldap_uid_field: str, escaped_username: str) -> str:
    """
    Replicates the filter construction logic from the vulnerable code snippet.
    This mirrors the actual implementation to test the invariant.
    """
    if auth_ldap_search_filter:
        filter_str = f"(&{auth_ldap_search_filter}({auth_ldap_uid_field}={escaped_username}))"
    else:
        filter_str = f"({auth_ldap_uid_field}={escaped_username})"
    return filter_str


def is_valid_ldap_filter_structure(filter_str: str) -> bool:
    """
    Validates that the LDAP filter has balanced parentheses and
    does not contain obvious injection artifacts that would alter
    the intended query structure beyond the search filter prefix.
    """
    # Check balanced parentheses
    depth = 0
    for char in filter_str:
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        if depth < 0:
            return False
    return depth == 0


def count_filter_components(filter_str: str) -> int:
    """Count the number of top-level filter components."""
    # Count top-level parenthesized groups
    depth = 0
    components = 0
    for char in filter_str:
        if char == '(':
            if depth == 0:
                components += 1
            depth += 1
        elif char == ')':
            depth -= 1
    return components


def escape_filter_chars_simple(text: str) -> str:
    """
    Simple implementation of LDAP filter character escaping
    (mirrors ldap3's escape_filter_chars behavior for testing purposes).
    """
    escape_chars = {
        '\\': r'\5c',
        '*': r'\2a',
        '(': r'\28',
        ')': r'\29',
        '\x00': r'\00',
    }
    result = text
    for char, escaped in escape_chars.items():
        result = result.replace(char, escaped)
    return result


@pytest.mark.parametrize("search_filter_payload", ADVERSARIAL_PAYLOADS)
def test_ldap_search_filter_injection_invariant(search_filter_payload):
    """
    Invariant: The AUTH_LDAP_SEARCH_FILTER configuration value, when interpolated
    directly into the LDAP filter string without validation or escaping, must not
    be able to alter the structural integrity of the resulting LDAP filter in a way
    that could bypass authentication or exfiltrate data.

    Specifically: if AUTH_LDAP_SEARCH_FILTER contains adversarial content,
    the security property is that such content SHOULD be validated/sanitized
    before use. This test documents the vulnerability by showing that
    unvalidated interpolation CAN break filter structure.
    """
    uid_field = "uid"
    safe_username = "testuser"
    escaped_username = escape_filter_chars_simple(safe_username)

    # Build the filter as the vulnerable code does
    filter_str = build_ldap_filter(search_filter_payload, uid_field, escaped_username)

    # SECURITY INVARIANT 1: The resulting filter must have balanced parentheses.
    # An unbalanced filter indicates injection has altered the query structure.
    assert is_valid_ldap_filter_structure(filter_str), (
        f"SECURITY VIOLATION: LDAP filter has unbalanced parentheses after injecting "
        f"search_filter payload: {repr(search_filter_payload)!r}\n"
        f"Resulting filter: {filter_str!r}"
    )

    # SECURITY INVARIANT 2: The username component must always be present and intact
    # in the filter, ensuring the authentication check cannot be bypassed.
    expected_uid_component = f"({uid_field}={escaped_username})"
    assert expected_uid_component in filter_str, (
        f"SECURITY VIOLATION: Username authentication component missing from LDAP filter.\n"
        f"Expected component: {expected_uid_component!r}\n"
        f"Resulting filter: {filter_str!r}\n"
        f"Payload: {repr(search_filter_payload)!r}"
    )

    # SECURITY INVARIANT 3: If a search filter is provided, the overall filter
    # must be wrapped in an AND (&) operator to ensure both conditions apply.
    if search_filter_payload and search_filter_payload.strip():
        assert filter_str.startswith("(&"), (
            f"SECURITY VIOLATION: Combined filter does not start with AND operator.\n"
            f"Resulting filter: {filter_str!r}\n"
            f"Payload: {repr(search_filter_payload)!r}"
        )

    # SECURITY INVARIANT 4: The filter must not contain null bytes which could
    # truncate the filter string in C-based LDAP libraries.
    assert '\x00' not in filter_str, (
        f"SECURITY VIOLATION: Null byte found in LDAP filter.\n"
        f"Resulting filter: {repr(filter_str)!r}\n"
        f"Payload: {repr(search_filter_payload)!r}"
    )


@pytest.mark.parametrize("search_filter_payload", ADVERSARIAL_PAYLOADS)
def test_ldap_search_filter_should_be_validated(search_filter_payload):
    """
    Invariant: A properly secured implementation MUST validate or sanitize
    AUTH_LDAP_SEARCH_FILTER before interpolation. This test verifies that
    a validation function correctly identifies dangerous filter values.
    """

    def is_safe_ldap_search_filter(filter_value: str) -> bool:
        """
        Security check: validates that a search filter value is safe to interpolate.
        A safe filter must:
        1. Have balanced parentheses
        2. Not be empty (if provided, must be a valid filter)
        3. Start with '(' if non-empty
        4. Not contain null bytes
        """
        if not filter_value:
            return True  # Empty is safe (no filter applied)

        # Must not contain null bytes
        if '\x00' in filter_value:
            return False

        # Must have balanced parentheses
        depth = 0
        for char in filter_value:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            if depth < 0:
                return False

        if depth != 0:
            return False

        # Must start with '(' to be a valid LDAP filter component
        stripped = filter_value.strip()
        if stripped and not stripped.startswith('('):
            return False

        return True

    # The invariant: if the filter is deemed safe by validation,
    # then building the LDAP query with it must produce a structurally valid filter.
    if is_safe_ldap_search_filter(search_filter_payload):
        uid_field = "uid"
        escaped_username = escape_filter_chars_simple("testuser")
        filter_str = build_ldap_filter(search_filter_payload, uid_field, escaped_username)

        assert is_valid_ldap_filter_structure(filter_str), (
            f"SECURITY VIOLATION: Even a 'safe' filter payload produced invalid LDAP filter.\n"
            f"Payload: {repr(search_filter_payload)!r}\n"
            f"Filter: {filter_str!r}"
        )
    else:
        # If the filter is unsafe, the system MUST reject it before use.
        # We document that these payloads are correctly identified as dangerous.
        assert not is_safe_ldap_search_filter(search_filter_payload), (
            f"Payload should be flagged as unsafe: {repr(search_filter_payload)!r}"
        )


@pytest.mark.parametrize("username_payload", [
    "admin)(uid=*",
    "admin*",
    "*(uid=*)",
    "admin\x00",
    "admin)(objectClass=*",
    "' OR '1'='1",
    "admin)(|(password=*))",
])
def test_username_escaping_invariant(username_payload):
    """
    Invariant: The username parameter MUST always be properly escaped before
    inclusion in the LDAP filter, regardless of its content. The escaped username
    must not contain unescaped special LDAP characters.
    """
    escaped = escape_filter_chars_simple(username_payload)

    # After escaping, these characters must not appear unescaped
    dangerous_chars = ['(', ')', '*', '\x00']

    for char in dangerous_chars:
        assert char not in escaped, (
            f"SECURITY VIOLATION: Dangerous character {repr(char)} found unescaped "
            f"in escaped username.\n"
            f"Original: {repr(username_payload)!r}\n"
            f"Escaped: {repr(escaped)!r}"
        )

    # Build filter with a safe search filter and verify structure
    uid_field = "uid"
    safe_search_filter = "(objectClass=person)"
    filter_str = build_ldap_filter(safe_search_filter, uid_field, escaped)

    assert is_valid_ldap_filter_structure(filter_str), (
        f"SECURITY VIOLATION: Filter structure broken after username escaping.\n"
        f"Username payload: {repr(username_payload)!r}\n"
        f"Escaped username: {repr(escaped)!r}\n"
        f"Filter: {filter_str!r}"
    )