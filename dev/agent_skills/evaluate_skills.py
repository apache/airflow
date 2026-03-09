#!/usr/bin/env python3
"""
Agent Skills Evaluation Harness

This script tests the agent skills by simulating common contribution workflows.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any


def load_skills() -> Dict[str, Any]:
    """Load the agent skills definition."""
    skills_file = Path(__file__).parent / "skills.json"
    with open(skills_file) as f:
        return json.load(f)


def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command."""
    print(f"Running: {cmd}")
    return subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)


def test_environment_detection():
    """Test environment detection."""
    print("Testing environment detection...")

    # Run the detection script
    result = run_command("python3 dev/detect_breeze_environment.py --json")
    context = json.loads(result.stdout)

    assert context["is_host"] == True
    assert context["is_breeze"] == False
    print("✓ Environment detection works")


def test_static_checks_skill(skills: Dict[str, Any]):
    """Test the static checks skill (dry run)."""
    print("Testing static checks skill...")

    skill = skills["skills"]["static_checks"]
    # Just check that the skill is defined properly
    assert skill["name"] == "Run Static Checks"
    assert "commands" in skill
    assert len(skill["commands"]) > 0
    print("✓ Static checks skill structure is valid")


def test_unit_tests_skill(skills: Dict[str, Any]):
    """Test the unit tests skill structure."""
    print("Testing unit tests skill...")

    skill = skills["skills"]["run_unit_tests"]
    assert skill["name"] == "Run Unit Tests"
    assert "parameters" in skill
    assert "test_path" in skill["parameters"]
    print("✓ Unit tests skill structure is valid")


def main():
    """Main evaluation function."""
    print("Starting Agent Skills Evaluation")
    print("=" * 40)

    try:
        skills = load_skills()
        print(f"Loaded skills version: {skills['version']}")

        test_environment_detection()
        test_static_checks_skill(skills)
        test_unit_tests_skill(skills)

        print("=" * 40)
        print("✓ All evaluations passed!")

    except Exception as e:
        print(f"✗ Evaluation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
