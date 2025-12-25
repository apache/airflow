#!/usr/bin/env python3
#
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
Benchmark script to measure CLI latency for different Auth Manager and Executor combinations.

This script:
1. Discovers all available Auth Managers and Executors from providers
2. Tests each combination by running 'airflow --help'
3. Measures response time for each combination
4. Generates a markdown report with results
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

# Add airflow to path
AIRFLOW_SOURCES_DIR = Path(__file__).resolve().parents[3] / "airflow-core" / "src"
sys.path.insert(0, str(AIRFLOW_SOURCES_DIR))


def get_available_auth_managers() -> list[str]:
    """Get all available auth manager class names from providers."""
    from airflow.providers_manager import ProvidersManager

    pm = ProvidersManager()
    return pm.auth_managers


def get_available_executors() -> list[str]:
    """Get all available executor class names from providers."""
    from airflow.providers_manager import ProvidersManager

    pm = ProvidersManager()
    # Get executors from providers
    executor_names = pm.executor_class_names
    
    # Add core executors
    core_executors = [
        "airflow.executors.local_executor.LocalExecutor",
        "airflow.executors.sequential_executor.SequentialExecutor",
    ]
    
    all_executors = list(set(core_executors + executor_names))
    return sorted(all_executors)


def measure_cli_latency(auth_manager: str | None, executor: str | None, runs: int = 3) -> tuple[float, float, bool]:
    """
    Measure the latency of 'airflow --help' command.
    
    Args:
        auth_manager: Auth manager class name (None for default)
        executor: Executor class name (None for default)
        runs: Number of runs to average
        
    Returns:
        Tuple of (average_time, min_time, success)
    """
    env = os.environ.copy()
    
    if auth_manager:
        env["AIRFLOW__CORE__AUTH_MANAGER"] = auth_manager
    if executor:
        env["AIRFLOW__CORE__EXECUTOR"] = executor
    
    times = []
    success = True
    
    for _ in range(runs):
        start = time.time()
        try:
            result = subprocess.run(
                ["airflow", "--help"],
                env=env,
                capture_output=True,
                timeout=30,
                check=False,
            )
            elapsed = time.time() - start
            
            # Check if command succeeded
            if result.returncode != 0:
                success = False
                break
                
            times.append(elapsed)
        except (subprocess.TimeoutExpired, Exception) as e:
            print(f"Error running command: {e}", file=sys.stderr)
            success = False
            break
    
    if not times:
        return 0.0, 0.0, False
    
    avg_time = sum(times) / len(times)
    min_time = min(times)
    
    return avg_time, min_time, success


def format_class_name(class_name: str | None) -> str:
    """Format class name for display (show only last part)."""
    if class_name is None:
        return "Default"
    parts = class_name.split(".")
    if len(parts) > 1:
        return parts[-1]
    return class_name


def generate_markdown_report(results: list[dict]) -> str:
    """Generate markdown formatted report."""
    lines = [
        "# Airflow CLI Latency Benchmark",
        "",
        "Benchmark results for `airflow --help` command with different Auth Manager and Executor combinations.",
        "",
        f"Total combinations tested: {len(results)}",
        "",
        "## Results Table",
        "",
        "| Auth Manager | Executor | Avg Time (s) | Min Time (s) | Status |",
        "|--------------|----------|--------------|--------------|--------|",
    ]
    
    for result in results:
        auth_display = format_class_name(result["auth_manager"])
        executor_display = format_class_name(result["executor"])
        avg_time = f"{result['avg_time']:.3f}" if result['success'] else "N/A"
        min_time = f"{result['min_time']:.3f}" if result['success'] else "N/A"
        status = "✅" if result['success'] else "❌"
        
        lines.append(f"| {auth_display} | {executor_display} | {avg_time} | {min_time} | {status} |")
    
    lines.extend([
        "",
        "## Summary Statistics",
        "",
    ])
    
    successful_results = [r for r in results if r['success']]
    if successful_results:
        avg_times = [r['avg_time'] for r in successful_results]
        lines.extend([
            f"- **Successful combinations**: {len(successful_results)}/{len(results)}",
            f"- **Overall average time**: {sum(avg_times) / len(avg_times):.3f}s",
            f"- **Fastest time**: {min(avg_times):.3f}s",
            f"- **Slowest time**: {max(avg_times):.3f}s",
        ])
    else:
        lines.append("- No successful combinations")
    
    lines.extend([
        "",
        "---",
        "",
        "*Note: Each combination was run 3 times and averaged.*",
    ])
    
    return "\n".join(lines)


def main():
    """Main function to run the benchmark."""
    print("=" * 80)
    print("Airflow CLI Latency Benchmark")
    print("=" * 80)
    print()
    
    print("Discovering available Auth Managers and Executors...")
    
    try:
        auth_managers = get_available_auth_managers()
        executors = get_available_executors()
    except Exception as e:
        print(f"Error discovering providers: {e}", file=sys.stderr)
        return 1
    
    print(f"Found {len(auth_managers)} Auth Managers")
    print(f"Found {len(executors)} Executors")
    print()
    
    # Add None to test default configuration
    auth_managers_to_test = [None] + auth_managers
    executors_to_test = [None] + executors
    
    total_combinations = len(auth_managers_to_test) * len(executors_to_test)
    print(f"Testing {total_combinations} combinations...")
    print()
    
    results = []
    count = 0
    
    for auth_manager in auth_managers_to_test:
        for executor in executors_to_test:
            count += 1
            auth_display = format_class_name(auth_manager)
            executor_display = format_class_name(executor)
            
            print(f"[{count}/{total_combinations}] Testing: {auth_display} + {executor_display}...", end=" ", flush=True)
            
            avg_time, min_time, success = measure_cli_latency(auth_manager, executor)
            
            results.append({
                "auth_manager": auth_manager,
                "executor": executor,
                "avg_time": avg_time,
                "min_time": min_time,
                "success": success,
            })
            
            if success:
                print(f"✅ {avg_time:.3f}s (avg) / {min_time:.3f}s (min)")
            else:
                print("❌ Failed")
    
    print()
    print("=" * 80)
    print("Generating report...")
    print("=" * 80)
    print()
    
    report = generate_markdown_report(results)
    print(report)
    
    # Optionally save to file
    output_file = Path("cli_latency_benchmark.md")
    output_file.write_text(report)
    print()
    print(f"Report saved to: {output_file.absolute()}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())