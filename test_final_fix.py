#!/usr/bin/env python3
"""
Final test to verify the fix for issue #59877 according to the expected behavior.
This test validates that the truncation logic prioritizes clear communication to the user.
"""

def test_final_truncation_logic():
    """Test the final fixed truncation logic based on PR discussion."""
    
    def redact(text, name):
        """Mock redact function for testing."""
        return text  # In real implementation, this would mask secrets
    
    def _truncate_rendered_value(rendered: str, max_length: int) -> str:
        """
        Final implementation of the fixed function based on PR discussion
        """
        if max_length <= 0:
            return ""

        prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
        suffix = "..."

        if max_length <= len(suffix):
            return suffix[:max_length]

        if max_length <= len(prefix) + len(suffix):
            # Not enough space for prefix + suffix + content, return truncated prefix + suffix
            return (prefix + suffix)[:max_length]

        # We have enough space for prefix + some content + suffix
        # Need to account for the fact that !r may add quotes, so we need to be more conservative
        available = max_length - len(prefix) - len(suffix)
        # If we're using !r formatting, it may add quotes, so we need to account for that
        # For strings, repr() adds 2 characters (quotes) around the content
        tentative_content = rendered[:available]
        tentative_repr = repr(tentative_content)
        if len(prefix) + len(tentative_repr) + len(suffix) <= max_length:
            return f"{prefix}{tentative_repr}{suffix}"
        else:
            # Need to reduce content length to account for the quotes added by repr()
            # We need to find the right content length so that len(prefix) + len(repr(content)) + len(suffix) <= max_length
            target_repr_length = max_length - len(prefix) - len(suffix)
            # Since repr adds quotes, we need to find content length where len(repr(content)) <= target_repr_length
            # For a string, repr adds 2 quotes, so content length should be target_repr_length - 2
            content_length = max(0, target_repr_length - 2)  # -2 for the quotes
            content_part = rendered[:content_length]
            return f"{prefix}{repr(content_part)}{suffix}"

    print("Testing FINAL fixed truncation logic with expected behavior...")
    
    # Test cases from the PR discussion
    test_cases = [
        (1, 'test', 'Minimum value'),
        (3, 'test', 'At ellipsis length'),
        (5, 'test', 'Very small'),
        (10, 'password123', 'Small'),
        (20, 'secret_value', 'Small with content'),
        (50, 'This is a test string', 'Medium'),
        (83, 'Hello World', 'At prefix+suffix boundary v1'),
        (84, 'Hello World', 'Just above boundary v1'),
        (86, 'Hello World', 'At overhead boundary v2'),
        (90, 'short', 'Normal case - short string'),
        (100, 'This is a longer string', 'Normal case'),
        (100, 'None', "String 'None'"),
        (100, 'True', "String 'True'"),
        (100, "{'key': 'value'}", 'Dict-like string'),
        (100, "test's", 'String with apostrophe'),
        (90, '"quoted"', 'String with quotes')
    ]

    print("Results from our implementation:")
    for max_length, rendered, description in test_cases:
        result = _truncate_rendered_value(rendered, max_length)
        print(f"max_length={max_length}, input='{rendered}' -> output='{result}' (len={len(result)})")
        assert len(result) <= max_length, f"Result length {len(result)} exceeds max_length {max_length}"
    
    # Specific test cases to validate expected behavior
    # For very small max_length values, we should get truncated prefix+suffix
    result1 = _truncate_rendered_value("any_content", 1)
    print(f"\nMax length 1: '{result1}' (length: {len(result1)})")
    assert len(result1) == 1, f"Expected length 1, got {len(result1)}"
    
    result3 = _truncate_rendered_value("any_content", 3)
    print(f"Max length 3: '{result3}' (length: {len(result3)})")
    assert len(result3) == 3, f"Expected length 3, got {len(result3)}"
    
    result5 = _truncate_rendered_value("any_content", 5)
    print(f"Max length 5: '{result5}' (length: {len(result5)})")
    assert len(result5) == 5, f"Expected length 5, got {len(result5)}"
    
    # Test that longer lengths work properly
    result85 = _truncate_rendered_value("Hello World", 85)
    print(f"Max length 85: '{result85}' (length: {len(result85)})")
    assert len(result85) <= 85, f"Expected length <= 85, got {len(result85)}"
    assert "Truncated. You can change this behaviour in [core]max_templated_field_length." in result85
    
    # Test max_length=83 which was problematic before
    result83 = _truncate_rendered_value("Hello World", 83)
    print(f"Max length 83: '{result83}' (length: {len(result83)})")
    assert len(result83) <= 83, f"Expected length <= 83, got {len(result83)}"
    
    # Test zero length
    result0 = _truncate_rendered_value("any_content", 0)
    print(f"Max length 0: '{result0}' (length: {len(result0)})")
    assert result0 == "", f"Expected empty string, got '{result0}'"
    
    print("\nAll tests passed! The fix follows the expected behavior from PR discussion.")
    return True


if __name__ == "__main__":
    test_final_truncation_logic()