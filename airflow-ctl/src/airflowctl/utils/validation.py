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
"""Utilities for validating required fields in pydantic models."""

from __future__ import annotations

import getpass
import sys
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def get_field_info(model: type[BaseModel], field_name: str) -> Any:
    """Get field info from a pydantic model."""
    return model.model_fields.get(field_name)


def is_required_field(model: type[BaseModel], field_name: str) -> bool:
    """
    Check if a field is required in a pydantic model.

    A field is required if:
    1. It has no default value (is_required() returns True)
    2. The default is None (which means the field must be provided)
    """
    field_info = get_field_info(model, field_name)
    if field_info is None:
        return False
    return field_info.is_required()


def get_missing_required_fields(model: type[BaseModel], data: dict) -> list[str]:
    """
    Get a list of required fields that are missing from the data.

    Args:
        model: The pydantic model class
        data: The data dictionary to check

    Returns:
        List of field names that are required but missing
    """
    missing_fields = []
    for field_name, field_info in model.model_fields.items():
        if field_info.is_required():
            # Check if field is missing entirely OR if it's set to None
            if field_name not in data or data.get(field_name) is None:
                missing_fields.append(field_name)
    return missing_fields


def get_field_description(model: type[BaseModel], field_name: str) -> str:
    """
    Get a human-readable description for a field.

    Uses the field's title if available, otherwise capitalizes the field name.
    """
    field_info = get_field_info(model, field_name)
    if field_info is None:
        return field_name.replace("_", " ").title()

    # Use title from field if available
    if field_info.title:
        return field_info.title

    # Fall back to capitalized field name
    return field_name.replace("_", " ").title()


def prompt_for_field(description: str, is_password: bool = False) -> Any:
    """
    Prompt the user to enter a value for a field.

    Args:
        description: A human-readable description of the field
        is_password: Whether to prompt for a password (hidden input)

    Returns:
        The value entered by the user
    """
    if is_password:
        return getpass.getpass(f"{description}: ")

    # Check if stdin is a TTY to determine if we can prompt interactively
    if not sys.stdin.isatty():
        # Non-interactive mode - return None to indicate missing value
        return None

    return input(f"{description}: ")


def validate_and_prompt_for_required_fields(
    model: type[T],
    data: dict,
    prompt: bool = True,
) -> tuple[T | None, list[str]]:
    """
    Validate that all required fields are present, optionally prompting for missing ones.

    Args:
        model: The pydantic model class to validate against
        data: The data dictionary to validate
        prompt: Whether to prompt for missing required fields (default: True)

    Returns:
        A tuple of (model_instance or None, list of missing fields)
        - If prompt=True and user cancels, returns (None, missing_fields)
        - If all fields are present, returns (model instance, empty list)
    """
    missing_fields = get_missing_required_fields(model, data)

    if not missing_fields:
        # All required fields are present, try to create the model
        try:
            return model(**data), []
        except Exception:
            # If validation fails, return the missing fields
            return None, missing_fields

    if not prompt:
        return None, missing_fields

    # Prompt for missing required fields
    filled_data = data.copy()
    still_missing = []

    for field_name in missing_fields:
        description = get_field_description(model, field_name)
        is_password = field_name == "password"
        value = prompt_for_field(description, is_password=is_password)

        if value is None or (isinstance(value, str) and not value.strip()):
            still_missing.append(field_name)
        else:
            filled_data[field_name] = value

    if still_missing:
        return None, still_missing

    # Try to create the model with the filled data
    try:
        return model(**filled_data), []
    except Exception:
        return None, still_missing


def check_required_fields(
    model: type[T],
    data: dict,
) -> list[str]:
    """
    Check which required fields are missing from the data.

    This is a non-interactive check that just returns the list of missing fields.

    Args:
        model: The pydantic model class to validate against
        data: The data dictionary to check

    Returns:
        List of field names that are required but missing
    """
    return get_missing_required_fields(model, data)


def format_missing_fields_message(
    model: type[BaseModel],
    missing_fields: list[str],
) -> str:
    """
    Format a user-friendly message listing missing required fields.

    Args:
        model: The pydantic model class
        missing_fields: List of missing field names

    Returns:
        A formatted error message
    """
    if not missing_fields:
        return ""

    field_descriptions = [get_field_description(model, f) for f in missing_fields]
    fields_str = ", ".join(field_descriptions)

    return f"Missing required field(s): {fields_str}"


def validate_required(datamodel: type[BaseModel]):
    """
    Validate required fields of a Pydantic model before executing a command.

    This decorator checks that all required fields are present in the datamodel
    before allowing the command function to execute. If required fields are missing,
    it exits with a helpful error message.

    Usage:
        @validate_required(ConnectionBody)
        def my_command(args, api_client):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find the datamodel params - they might be in args or kwargs
            # Usually the datamodel is passed as a dict parameter

            # Get the params dict from args/kwargs
            # Looking for parameter name matching the datamodel key
            model_params = None

            # Check for common parameter names
            possible_names = [
                datamodel.__name__.replace("Body", "").lower() + "_body",
                datamodel.__name__.replace("Body", "").lower(),
                "body",
                "data",
            ]

            for name in possible_names:
                if name in kwargs:
                    model_params = kwargs[name]
                    break

            if model_params is None:
                # Try first positional arg if it's a dict
                if args and isinstance(args[0], dict):
                    model_params = args[0]

            if model_params is not None and isinstance(model_params, dict):
                missing_fields = check_required_fields(datamodel, model_params)
                if missing_fields:
                    msg = format_missing_fields_message(datamodel, missing_fields)
                    raise SystemExit(msg)

            return func(*args, **kwargs)

        return wrapper

    return decorator
