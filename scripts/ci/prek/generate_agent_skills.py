#!/usr/bin/env python3
"""
Generate Agent Skills from Breeze CLI

This script extracts command information from Breeze CLI and generates
machine-readable agent skills definitions.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict


def extract_command_info() -> Dict[str, Any]:
    """Extract command information from Breeze CLI."""
    skills = {
        "version": "1.0",
        "description": "Auto-generated Airflow Breeze Agent Skills from CLI",
        "environment_detection": {
            "script": "dev/detect_breeze_environment.py",
            "markers": {
                "host": {
                    "inside_docker": False,
                    "breeze_container": False
                },
                "breeze_container": {
                    "inside_docker": True,
                    "breeze_container": True
                }
            }
        },
        "skills": {},
        "command_mappings": {}
    }

    # For now, define core skills based on common workflows
    # TODO: Extract from actual CLI when dependencies are available
    skills["skills"] = {
        "static_checks": {
            "name": "Run Static Checks",
            "description": "Run pre-commit hooks and static analysis checks",
            "context": "host",
            "commands": [
                {
                    "description": "Stage changes for commit",
                    "command": "git add .",
                    "optional": True
                },
                {
                    "description": "Run pre-commit checks",
                    "command": "prek run --from-ref main",
                    "environment": "host"
                }
            ],
            "success_criteria": [
                "All checks pass without errors",
                "No linting violations",
                "No type checking errors"
            ]
        },
        "run_unit_tests": {
            "name": "Run Unit Tests",
            "description": "Execute unit tests for specific modules in Breeze environment",
            "context": "breeze_container",
            "parameters": {
                "test_path": {
                    "type": "string",
                    "description": "Path to test file or module",
                    "example": "tests/cli/test_cli_parser.py"
                },
                "project": {
                    "type": "string",
                    "description": "Project name (airflow-core, providers/amazon, etc.)",
                    "default": "airflow-core"
                }
            },
            "commands": [
                {
                    "description": "Enter Breeze shell if not already in container",
                    "command": "breeze shell",
                    "condition": "not is_breeze_container",
                    "environment": "host"
                },
                {
                    "description": "Run pytest on specified test path",
                    "command": "uv run --project {project} pytest {test_path} -xvs",
                    "environment": "breeze_container"
                }
            ]
        },
        "start_airflow": {
            "name": "Start Airflow Services",
            "description": "Start Airflow with all necessary services",
            "context": "host",
            "commands": [
                {
                    "description": "Start Airflow services",
                    "command": "breeze start-airflow",
                    "environment": "host"
                }
            ]
        }
    }

    # Basic command mappings
    skills["command_mappings"] = {
        "breeze_shell": {
            "description": "Enter Breeze development environment",
            "host_command": "breeze shell",
            "context": "host"
        },
        "breeze_start_airflow": {
            "description": "Start Airflow services",
            "host_command": "breeze start-airflow",
            "context": "host"
        },
        "run_pytest": {
            "description": "Run pytest tests",
            "container_command": "uv run --project {project} pytest {args}",
            "context": "breeze_container"
        },
        "run_prek": {
            "description": "Run pre-commit checks",
            "host_command": "prek run --from-ref {target_branch}",
            "context": "host"
        }
    }

    return skills


def main():
    """Main entry point."""
    skills = extract_command_info()

    # Write to the agent skills file
    skills_file = Path(__file__).parent.parent.parent.parent / "dev" / "agent_skills" / "skills.json"
    skills_file.parent.mkdir(exist_ok=True)

    with open(skills_file, 'w') as f:
        json.dump(skills, f, indent=2)

    print(f"Generated agent skills at {skills_file}")


if __name__ == "__main__":
    main()