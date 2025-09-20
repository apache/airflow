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
"""Utilities to fix common CI issues and make workflows more robust."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import run_command


def check_required_tool(tool_name: str, install_instructions: str | None = None) -> bool:
    """
    Check if a required tool is available in the system.
    
    :param tool_name: Name of the tool to check
    :param install_instructions: Optional installation instructions
    :return: True if tool exists, False otherwise
    """
    try:
        subprocess.check_output(["which", tool_name], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        console = get_console()
        console.print(f"[red]{tool_name} is required but not found![/]")
        if install_instructions:
            console.print(f"[yellow]Install it with: {install_instructions}[/]")
        else:
            common_instructions = {
                "gh": "sudo apt-get update && sudo apt-get install -y gh",
                "jq": "sudo apt-get update && sudo apt-get install -y jq",
                "git": "sudo apt-get update && sudo apt-get install -y git",
            }
            if tool_name in common_instructions:
                console.print(f"[yellow]Install it with: {common_instructions[tool_name]}[/]")
        return False


def ensure_required_tools(tools: list[str]) -> None:
    """
    Ensure all required tools are available.
    
    :param tools: List of tool names to check
    :raises SystemExit: If any tool is missing
    """
    missing_tools = []
    for tool in tools:
        if not check_required_tool(tool):
            missing_tools.append(tool)
    
    if missing_tools:
        console = get_console()
        console.print(f"[error]Missing required tools: {', '.join(missing_tools)}[/]")
        console.print("\n[info]For GitHub Actions, add this step:[/]")
        console.print("- name: Install dependencies")
        console.print("  run: |")
        console.print("    sudo apt-get update")
        console.print(f"    sudo apt-get install -y {' '.join(missing_tools)}")
        sys.exit(1)


def ensure_git_remote_exists(remote_name: str, remote_url: str) -> None:
    """
    Ensure a git remote exists with the correct URL.
    
    :param remote_name: Name of the remote
    :param remote_url: URL for the remote
    """
    console = get_console()
    
    # Check if remote exists
    try:
        result = run_command(
            ["git", "remote", "get-url", remote_name], 
            text=True, 
            capture_output=True,
            check=False
        )
        if result.returncode == 0:
            # Remote exists, check if URL matches
            current_url = result.stdout.strip()
            if current_url != remote_url:
                console.print(f"[warning]Remote '{remote_name}' exists but with different URL[/]")
                console.print(f"[info]Current: {current_url}[/]")
                console.print(f"[info]Expected: {remote_url}[/]")
                console.print(f"[info]Updating remote URL...[/]")
                run_command(["git", "remote", "set-url", remote_name, remote_url], check=True)
        else:
            # Remote doesn't exist, add it
            console.print(f"[info]Adding remote '{remote_name}' -> {remote_url}[/]")
            run_command(["git", "remote", "add", remote_name, remote_url], check=True)
    except subprocess.CalledProcessError as ex:
        console.print(f"[error]Error checking remote '{remote_name}': {ex}[/]")
        sys.exit(1)


def ensure_git_tags_fetched(remote_name: str = "origin") -> None:
    """
    Ensure all tags are fetched from the remote.
    
    :param remote_name: Name of the remote to fetch from
    """
    console = get_console()
    console.print(f"[info]Fetching all tags from {remote_name}...[/]")
    
    try:
        # First, check if repo is shallow
        result = run_command(
            ["git", "rev-parse", "--is-shallow-repository"],
            check=True,
            capture_output=True,
            text=True,
        )
        is_shallow = result.stdout.strip() == "true"
        
        # Build fetch command
        fetch_cmd = ["git", "fetch", "--tags", "--force", remote_name]
        if is_shallow:
            console.print("[warning]Repository is shallow, unshallowing...[/]")
            fetch_cmd.append("--unshallow")
        
        # Fetch tags
        run_command(fetch_cmd, check=True)
        console.print("[success]Tags fetched successfully[/]")
        
    except subprocess.CalledProcessError as ex:
        console.print(f"[error]Error fetching tags: {ex}[/]")
        console.print("[info]You can manually fetch tags with:[/]")
        console.print(f"  git fetch --tags {remote_name}")
        sys.exit(1)


def validate_git_revision(revision: str, context: str = "") -> bool:
    """
    Validate if a git revision (tag, branch, commit) exists.
    
    :param revision: Git revision to check
    :param context: Optional context for error messages
    :return: True if revision exists, False otherwise
    """
    try:
        run_command(
            ["git", "rev-parse", "--verify", revision],
            capture_output=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        console = get_console()
        console.print(f"[error]Git revision '{revision}' does not exist{f' ({context})' if context else ''}[/]")
        return False


def safe_git_log(
    from_commit: str | None = None,
    to_commit: str | None = None,
    path: str | None = None,
    format_string: str = "%H %h %cd %s",
    date_format: str = "short",
    fallback_on_error: bool = True,
) -> str | None:
    """
    Safely run git log with proper error handling.
    
    :param from_commit: Starting commit/tag
    :param to_commit: Ending commit/tag
    :param path: Optional path to filter
    :param format_string: Git log format string
    :param date_format: Date format
    :param fallback_on_error: If True, return None on error; if False, raise exception
    :return: Git log output or None on error
    """
    console = get_console()
    
    # Build git log command
    cmd = ["git", "log", f"--pretty=format:{format_string}", f"--date={date_format}"]
    
    # Add revision range
    if from_commit and to_commit:
        # Validate revisions exist
        if not validate_git_revision(from_commit, "from_commit"):
            if fallback_on_error:
                console.print(f"[warning]Skipping git log due to missing revision: {from_commit}[/]")
                return None
            raise ValueError(f"Invalid from_commit: {from_commit}")
            
        if not validate_git_revision(to_commit, "to_commit"):
            if fallback_on_error:
                console.print(f"[warning]Skipping git log due to missing revision: {to_commit}[/]")
                return None
            raise ValueError(f"Invalid to_commit: {to_commit}")
            
        cmd.append(f"{from_commit}..{to_commit}")
    elif from_commit:
        if not validate_git_revision(from_commit, "from_commit"):
            if fallback_on_error:
                console.print(f"[warning]Skipping git log due to missing revision: {from_commit}[/]")
                return None
            raise ValueError(f"Invalid from_commit: {from_commit}")
        cmd.append(from_commit)
    
    # Add path filter
    if path:
        cmd.extend(["--", path])
    
    # Run command
    try:
        result = run_command(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as ex:
        if fallback_on_error:
            console.print(f"[warning]Git log failed: {ex}[/]")
            console.print(f"[info]Command was: {' '.join(cmd)}[/]")
            return None
        raise


def setup_git_for_ci(
    github_repository: str = "apache/airflow",
    fetch_tags: bool = True,
    setup_apache_remote: bool = True,
) -> None:
    """
    Set up git environment for CI operations.
    
    :param github_repository: GitHub repository in format owner/repo
    :param fetch_tags: Whether to fetch all tags
    :param setup_apache_remote: Whether to set up apache-https-for-providers remote
    """
    console = get_console()
    console.print("[info]Setting up git environment for CI...[/]")
    
    # Ensure git is available
    ensure_required_tools(["git"])
    
    # Set up apache remote if requested
    if setup_apache_remote:
        remote_url = f"https://github.com/{github_repository}.git"
        ensure_git_remote_exists("apache-https-for-providers", remote_url)
        
        # Fetch tags from the apache remote
        if fetch_tags:
            ensure_git_tags_fetched("apache-https-for-providers")
    
    # Also fetch tags from origin
    if fetch_tags:
        ensure_git_tags_fetched("origin")
    
    console.print("[success]Git environment set up successfully[/]")


# Helper function to fix common git revision issues
def fix_git_revision_range(revision_range: str) -> str:
    """
    Fix common git revision range issues.
    
    :param revision_range: Git revision range (e.g., "tag1...tag2")
    :return: Fixed revision range
    """
    # Fix three dots to two dots
    if "..." in revision_range:
        console = get_console()
        console.print(f"[warning]Fixing revision range: {revision_range} -> {revision_range.replace('...', '..')}[/]")
        return revision_range.replace("...", "..")
    return revision_range
