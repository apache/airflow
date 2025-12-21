#!/usr/bin/env python3
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
"""
Script: Find "DAG" in string literals and comments

This script finds "DAG" in string literals (log messages, docstrings, etc.) and comments
in the codebase, excluding variable names, and organizes them by file.
"""

from __future__ import annotations

import ast
import re
from collections import defaultdict
from pathlib import Path


class DagStringFinder(ast.NodeVisitor):
    """AST visitor that finds DAG in string literals and comments"""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.matches: list[tuple[int, str, str]] = []  # (line, type, content)
        self.source_lines: list[str] = []

    def visit_Str(self, node):
        """String node for Python 3.7 and below"""
        if isinstance(node.s, str) and "DAG" in node.s:
            self.matches.append((node.lineno, "string", node.s))
        self.generic_visit(node)

    def visit_Constant(self, node):
        """Constant node for Python 3.8+ (includes strings)"""
        if isinstance(node.value, str) and "DAG" in node.value:
            self.matches.append((node.lineno, "string", node.value))
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        """Check function definition docstring"""
        if ast.get_docstring(node) and "DAG" in ast.get_docstring(node):
            self.matches.append((node.lineno, "docstring", ast.get_docstring(node)))
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        """Check class definition docstring"""
        if ast.get_docstring(node) and "DAG" in ast.get_docstring(node):
            self.matches.append((node.lineno, "docstring", ast.get_docstring(node)))
        self.generic_visit(node)

    def visit_Module(self, node):
        """Check module-level docstring"""
        if ast.get_docstring(node) and "DAG" in ast.get_docstring(node):
            self.matches.append((1, "docstring", ast.get_docstring(node)))
        self.generic_visit(node)


def find_dag_in_comments(file_path: Path) -> list[tuple[int, str]]:
    """Find DAG in comments (supports Python, JavaScript, TypeScript, RST)"""
    matches = []
    suffix = file_path.suffix.lower()
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                comment_part = None
                # Python: # comment
                if suffix in (".py", ".rst") and "#" in line:
                    comment_part = line.split("#", 1)[1]
                # JavaScript/TypeScript: // comment
                elif suffix in (".js", ".ts", ".tsx") and "//" in line:
                    comment_part = line.split("//", 1)[1]
                # RST: .. comment
                elif suffix == ".rst" and line.strip().startswith(".."):
                    comment_part = line.strip()

                if comment_part and "DAG" in comment_part:
                    matches.append((line_num, comment_part.strip()))
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}")
    return matches


def find_dag_in_strings_ast(file_path: Path) -> list[tuple[int, str, str]]:
    """Find DAG in string literals using AST"""
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            source = f.read()
            tree = ast.parse(source, filename=str(file_path))

        finder = DagStringFinder(file_path)
        finder.source_lines = source.splitlines()
        finder.visit(tree)
        return finder.matches
    except SyntaxError:
        # Skip files that cannot be parsed
        return []
    except Exception as e:
        print(f"Warning: Could not parse {file_path}: {e}")
        return []


def find_dag_in_strings_regex(file_path: Path) -> list[tuple[int, str]]:
    """Find DAG in string literals using regex (supports multiple file types)"""
    matches = []
    suffix = file_path.suffix.lower()
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            content = f.read()
            lines = content.splitlines()

            # String literal patterns for different file types
            # Python, JavaScript, TypeScript: single/double quotes
            # RST: also check for inline code and text
            if suffix in (".py", ".js", ".ts", ".tsx"):
                # Single and double quoted strings
                string_pattern = r'["\']([^"\']*DAG[^"\']*)["\']'
            elif suffix == ".rst":
                # RST: inline code, text, etc.
                string_pattern = r'[`"\']([^`"\']*DAG[^`"\']*)[`"\']'
            else:
                # Default: try common quote patterns
                string_pattern = r'["\']([^"\']*DAG[^"\']*)["\']'

            for line_num, line in enumerate(lines, 1):
                # Remove comments based on file type
                code_part = line
                if suffix in (".py", ".rst") and "#" in line:
                    code_part = line.split("#")[0]
                elif suffix in (".js", ".ts", ".tsx") and "//" in line:
                    code_part = line.split("//")[0]

                # Find string literals
                for match in re.finditer(string_pattern, code_part):
                    matched_text = match.group(1)
                    if "DAG" in matched_text:
                        matches.append((line_num, matched_text))
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}")
    return matches


def scan_file(file_path: Path) -> dict[str, list[tuple[int, str]]]:
    """Scan file to find strings and comments containing DAG"""
    results = {
        "strings": [],
        "comments": [],
    }

    suffix = file_path.suffix.lower()

    # Use AST only for Python files
    if suffix == ".py":
        ast_matches = find_dag_in_strings_ast(file_path)
        for line_num, _match_type, content in ast_matches:
            # Truncate if content is too long
            truncated_content = content
            if len(content) > 100:
                truncated_content = content[:100] + "..."
            results["strings"].append((line_num, truncated_content))

    # Use regex for all file types (fallback for Python, primary for others)
    if not results["strings"] or suffix != ".py":
        regex_matches = find_dag_in_strings_regex(file_path)
        results["strings"] = regex_matches

    # Find comments
    results["comments"] = find_dag_in_comments(file_path)

    return results


def generate_markdown_output(
    results_by_file: dict[Path, dict[str, list[tuple[int, str]]]],
    workspace_root: Path,
) -> list[str]:
    """Generate Markdown output with checkboxes grouped by module"""
    output_lines = []
    output_lines.append("# DAG String Replacement Checklist\n")
    output_lines.append(
        f"Found 'DAG' in {len(results_by_file)} files that need to be changed to 'Dag' or 'Dags'.\n"
    )
    output_lines.append("## Summary\n")
    total_strings = sum(len(r["strings"]) for r in results_by_file.values())
    total_comments = sum(len(r["comments"]) for r in results_by_file.values())
    output_lines.append(f"- **Total files**: {len(results_by_file)}")
    output_lines.append(f"- **String literals**: {total_strings}")
    output_lines.append(f"- **Comments**: {total_comments}\n")
    output_lines.append("---\n")

    # Group files by module (directory)
    modules: dict[str, list[tuple[Path, dict[str, list[tuple[int, str]]]]]] = defaultdict(list)
    for file_path, results in sorted(results_by_file.items()):
        relative_path = file_path.relative_to(workspace_root)
        # Get module (parent directory)
        module_path = str(relative_path.parent) if relative_path.parent != Path(".") else "root"
        modules[module_path].append((relative_path, results))

    # Sort modules
    for module_path in sorted(modules.keys()):
        module_files = modules[module_path]
        module_display = module_path.replace("_", "\\_") if module_path != "root" else "Root"

        # Count items in this module
        module_strings = sum(len(r["strings"]) for _, r in module_files)
        module_comments = sum(len(r["comments"]) for _, r in module_files)
        module_total = module_strings + module_comments

        output_lines.append(f"\n## {module_display}\n")
        output_lines.append(
            f"- [ ] **{len(module_files)} files, {module_total} items** ({module_strings} strings, {module_comments} comments)"
        )

    return output_lines


def generate_text_output(
    results_by_file: dict[Path, dict[str, list[tuple[int, str]]]],
    workspace_root: Path,
) -> list[str]:
    """Generate plain text output"""
    output_lines = []
    output_lines.append(f"\nFound 'DAG' in {len(results_by_file)} files:\n")

    for file_path, results in sorted(results_by_file.items()):
        relative_path = file_path.relative_to(workspace_root)
        output_lines.append(f"\n{'=' * 80}")
        output_lines.append(f"File: {relative_path}")
        output_lines.append(f"{'=' * 80}")

        if results["strings"]:
            output_lines.append("\n[String Literals]")
            for line_num, content in results["strings"]:
                # Highlight only the part containing DAG
                highlighted = content.replace("DAG", ">>>DAG<<<")
                output_lines.append(f"  Line {line_num}: {highlighted}")

        if results["comments"]:
            output_lines.append("\n[Comments]")
            for line_num, content in results["comments"]:
                highlighted = content.replace("DAG", ">>>DAG<<<")
                output_lines.append(f"  Line {line_num}: {highlighted}")

    # Summary statistics
    total_strings = sum(len(r["strings"]) for r in results_by_file.values())
    total_comments = sum(len(r["comments"]) for r in results_by_file.values())

    output_lines.append(f"\n{'=' * 80}")
    output_lines.append("Summary:")
    output_lines.append(f"  - Total files: {len(results_by_file)}")
    output_lines.append(f"  - String literals: {total_strings}")
    output_lines.append(f"  - Comments: {total_comments}")
    output_lines.append(f"{'=' * 80}")

    return output_lines


def main():
    """Main function: scan all Python files"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Find 'DAG' in string literals and comments")
    parser.add_argument(
        "output_file",
        nargs="?",
        help="Output file path (optional, prints to stdout if not specified)",
    )
    parser.add_argument(
        "--markdown",
        "-m",
        action="store_true",
        help="Output in Markdown format with checkboxes",
    )
    parser.add_argument(
        "--file",
        "-f",
        action="append",
        help="Specific file or directory to scan (can be used multiple times)",
    )
    args = parser.parse_args()
    output_file = Path(args.output_file) if args.output_file else None
    markdown_mode = args.markdown
    target_paths = [Path(p) for p in args.file] if args.file else None

    # Find project root (parent of dev directory)
    script_path = Path(__file__).resolve()
    if script_path.parent.name == "dev":
        workspace_root = script_path.parent.parent
    else:
        # Fallback: assume script is in project root
        workspace_root = script_path.parent

    results_by_file: dict[Path, dict[str, list[tuple[int, str]]]] = defaultdict(dict)

    # Directories to exclude
    exclude_dirs = {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        "node_modules",
        "build",
        "dist",
        ".pytest_cache",
        ".mypy_cache",
    }

    # Supported file extensions
    supported_extensions = {".py", ".rst", ".js", ".ts", ".tsx"}

    # Find files to scan
    files_to_scan = []
    if target_paths:
        # Scan only specified files/directories
        for target_path in target_paths:
            target = Path(target_path)
            if not target.is_absolute():
                target = workspace_root / target

            if target.is_file() and target.suffix.lower() in supported_extensions:
                # Single file
                if not any(excluded in target.parts for excluded in exclude_dirs):
                    files_to_scan.append(target)
            elif target.is_dir():
                # Directory - find all supported files in it
                for ext in supported_extensions:
                    for file_path in target.rglob(f"*{ext}"):
                        if not any(excluded in file_path.parts for excluded in exclude_dirs):
                            files_to_scan.append(file_path)
            else:
                print(f"Warning: {target_path} is not a valid file or directory", file=sys.stderr)
    else:
        # Scan all supported files in workspace
        for ext in supported_extensions:
            for file_path in workspace_root.rglob(f"*{ext}"):
                # Check excluded directories
                if any(excluded in file_path.parts for excluded in exclude_dirs):
                    continue
                files_to_scan.append(file_path)

    print(f"Scanning {len(files_to_scan)} files...")
    if output_file:
        format_type = "Markdown" if markdown_mode else "text"
        print(f"Saving results to {output_file} ({format_type})...")
    if not markdown_mode:
        print("=" * 80)

    # Scan each file
    for idx, file_path in enumerate(sorted(files_to_scan), 1):
        if idx % 100 == 0:
            print(f"Progress... {idx}/{len(files_to_scan)}", file=sys.stderr)
        results = scan_file(file_path)
        if results["strings"] or results["comments"]:
            results_by_file[file_path] = results

    # Output results
    if not results_by_file:
        message = "\nNo files found containing 'DAG' in string literals or comments."
        print(message)
        if output_file:
            output_file.write_text(message)
        return

    if markdown_mode:
        output_lines = generate_markdown_output(results_by_file, workspace_root)
    else:
        output_lines = generate_text_output(results_by_file, workspace_root)

    # Output
    output_text = "\n".join(output_lines)
    if output_file:
        output_file.write_text(output_text, encoding="utf-8")
        print(f"\nResults saved to {output_file}.")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
