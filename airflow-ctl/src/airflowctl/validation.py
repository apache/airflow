"""
Validation utilities for airflowctl
Issue #57721: Improve error messages when required API fields are missing
"""

from typing import Any
from pydantic import BaseModel, ValidationError


class FieldValidationError(Exception):
    """Custom exception for field validation errors."""
    pass


def get_missing_required_fields(model_class: type[BaseModel], data: dict[str, Any]) -> list[str]:
    """
    Identify missing required fields in the provided data.
    
    Args:
        model_class: The Pydantic model class to validate against
        data: Dictionary of field values provided by the user
    
    Returns:
        List of field names that are required but missing from data
    """
    missing_fields = []
    
    for field_name, field_info in model_class.model_fields.items():
        if field_info.is_required() and field_name not in data:
            missing_fields.append(field_name)
    
    return missing_fields


def validate_and_build_model(
    model_class: type[BaseModel], 
    data: dict[str, Any],
    context: str = "request"
) -> BaseModel:
    """
    Validate data and build a Pydantic model with improved error messages.
    
    Args:
        model_class: The Pydantic model class to build
        data: Dictionary of field values
        context: Context string for error messages (e.g., "dags pause")
    
    Returns:
        Instance of the model class
    
    Raises:
        FieldValidationError: If required fields are missing or validation fails
    """
    # Check for missing required fields first
    missing_fields = get_missing_required_fields(model_class, data)
    
    if missing_fields:
        # Format field names for CLI (convert snake_case to --kebab-case)
        cli_fields = [f"--{field.replace('_', '-')}" for field in missing_fields]
        fields_str = ", ".join(cli_fields)
        raise FieldValidationError(
            f"Missing required parameter(s): {fields_str}\n"
            f"Use 'airflowctl {context} --help' for usage information."
        )
    
    # Try to build the model and catch validation errors
    try:
        return model_class(**data)
    except ValidationError as e:
        # Format Pydantic validation errors in a user-friendly way
        error_messages = [f"Validation error in {context}:"]
        
        for error in e.errors():
            loc = ".".join(str(l) for l in error['loc'])
            msg = error['msg']
            error_messages.append(f"  --{loc.replace('_', '-')}: {msg}")
        
        raise FieldValidationError("\n".join(error_messages))