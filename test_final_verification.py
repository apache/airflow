#!/usr/bin/env python3
"""
Final test to verify the fix for issue #59877.
This test uses the exact logic that was implemented in the final fix.
"""

def test_final_truncation_logic():
    """Test the final fixed truncation logic."""
    
    def redact(text, name):
        """Mock redact function for testing."""
        return text  # In real implementation, this would mask secrets
    
    def is_jsonable(x):
        import json
        try:
            json.dumps(x)
        except (TypeError, OverflowError):
            return False
        else:
            return True

    def _serialize_template_field(template_field, name, max_length):
        """
        Final implementation of the fixed function from task_runner.py
        """
        if not is_jsonable(template_field):
            try:
                serialized = template_field.serialize()
            except AttributeError:
                serialized = str(template_field)
            if len(serialized) > max_length:
                rendered = redact(serialized, name)
                # Calculate how much space is available for actual content after accounting for prefix and suffix
                prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
                suffix = "... "
                available_content_length = max_length - len(prefix) - len(suffix)
                
                # Ensure we show at least 1 character of actual content if possible
                if available_content_length < 1:
                    # If max_length is too small to show content with full prefix and suffix,
                    # return content that fits within max_length
                    if max_length < len(suffix):
                        # Max length is smaller than suffix - just return first max_length chars
                        return rendered[:max_length] if max_length > 0 else ""
                    elif max_length < len(prefix):
                        # Max length is smaller than prefix - return a truncated prefix
                        return prefix[:max_length]
                    else:
                        # Max length is big enough for prefix but not for prefix+content+suffix
                        remaining_after_prefix = max_length - len(prefix)
                        if remaining_after_prefix >= len(suffix):
                            # We can fit both prefix and suffix, with minimal content
                            content_length = max(0, remaining_after_prefix - len(suffix))
                            content_part = rendered[:content_length]
                            return f"{prefix}{content_part}{suffix}"
                        else:
                            # We can't even fit the full suffix after prefix
                            # Show prefix + partial suffix
                            content_part = ""
                            suffix_part = suffix[:remaining_after_prefix]
                            return f"{prefix}{suffix_part}"
                else:
                    content_part = rendered[:available_content_length]
                    return f"{prefix}{content_part!r}{suffix}"
            return serialized
        # Handle JSON serializable content
        if not template_field and not isinstance(template_field, tuple):
            # Avoid unnecessary serialization steps for empty fields unless they are tuples
            # and need to be converted to lists
            return template_field
        # For this test, just convert to string directly
        template_field = template_field  # This would be processed differently in real code
        serialized = str(template_field)
        if len(serialized) > max_length:
            rendered = redact(serialized, name)
            # Calculate how much space is available for actual content after accounting for prefix and suffix
            prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
            suffix = "... "
            available_content_length = max_length - len(prefix) - len(suffix)
            
            # Ensure we show at least 1 character of actual content if possible
            if available_content_length < 1:
                # If max_length is too small to show content with full prefix and suffix,
                # return content that fits within max_length
                if max_length < len(suffix):
                    # Max length is smaller than suffix - just return first max_length chars
                    return rendered[:max_length] if max_length > 0 else ""
                elif max_length < len(prefix):
                    # Max length is smaller than prefix - return a truncated prefix
                    return prefix[:max_length]
                else:
                    # Max length is big enough for prefix but not for prefix+content+suffix
                    remaining_after_prefix = max_length - len(prefix)
                    if remaining_after_prefix >= len(suffix):
                        # We can fit both prefix and suffix, with minimal content
                        content_length = max(0, remaining_after_prefix - len(suffix))
                        content_part = rendered[:content_length]
                        return f"{prefix}{content_part}{suffix}"
                    else:
                        # We can't even fit the full suffix after prefix
                        # Show prefix + partial suffix
                        content_part = ""
                        suffix_part = suffix[:remaining_after_prefix]
                        return f"{prefix}{suffix_part}"
            else:
                content_part = rendered[:available_content_length]
                return f"{prefix}{content_part!r}{suffix}"
        return template_field

    print("Testing FINAL fixed truncation logic with small max_length values...")
    
    # Test case 1: max_length = 1 (smaller than suffix length 4)
    test_string = "This is a long string that should be truncated"
    result = _serialize_template_field(test_string, "test_field", 1)
    print(f"Max length 1, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected max: <= 1")
    assert len(result) <= 1, f"Result length {len(result)} exceeds max length 1"
    
    # Test case 2: max_length = 3 (smaller than suffix length 4)
    result = _serialize_template_field(test_string, "test_field", 3)
    print(f"Max length 3, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected max: <= 3")
    assert len(result) <= 3, f"Result length {len(result)} exceeds max length 3"
    
    # Test case 3: max_length = 5 (larger than suffix but smaller than prefix)
    result = _serialize_template_field(test_string, "test_field", 5)
    print(f"Max length 5, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected max: <= 5")
    assert len(result) <= 5, f"Result length {len(result)} exceeds max length 5"
    
    # Test case 4: max_length = 10 (larger than suffix but smaller than prefix)
    result = _serialize_template_field(test_string, "test_field", 10)
    print(f"Max length 10, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected max: <= 10")
    assert len(result) <= 10, f"Result length {len(result)} exceeds max length 10"
    
    # Test case 5: max_length = 85 (should have room for prefix + content + suffix)
    result = _serialize_template_field(test_string, "test_field", 85)
    print(f"Max length 85, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected: <= 85")
    assert len(result) <= 85, f"Result length {len(result)} exceeds max length 85"
    
    # Test case 6: input shorter than max_length should not be truncated
    short_string = "Short"
    result = _serialize_template_field(short_string, "test_field", 10)
    print(f"Max length 10, short input: '{short_string}' -> output: '{result}'")
    assert result == short_string, f"Short string was incorrectly truncated: {result}"
    
    # Test case 7: max_length = 0
    result = _serialize_template_field(test_string, "test_field", 0)
    print(f"Max length 0, input: '{test_string}' -> output: '{result}'")
    print(f"  Length: {len(result)}, Expected: <= 0")
    assert len(result) <= 0, f"Result length {len(result)} exceeds max length 0"
    
    print("\nAll tests passed! The final fix works correctly.")
    return True


if __name__ == "__main__":
    test_final_truncation_logic()