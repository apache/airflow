# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.context import detect_context
from agent_skills_poc.generator.generate_skills import build_skills_payload
from agent_skills_poc.parser.parser import parse_workflows_from_text
from agent_skills_poc.resolver import resolve_command


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Aggregated benchmark output for one workflow-count scenario."""

    workflows: int
    parse_ms: float
    generate_ms: float
    context_detect_ms: float
    command_resolve_ms: float
    peak_memory_mb: float


def _build_rst(workflow_count: int) -> str:
    header = "PoC Workflows\n=============\n\n"
    blocks = []
    for idx in range(workflow_count):
        blocks.append(
            "\n".join(
                [
                    ".. agent-skill::",
                    f"   :id: skill_{idx}",
                    f"   :description: Workflow {idx}",
                    f"   :local: uv run --project airflow-core pytest tests/test_{idx}.py",
                    "   :fallback: breeze exec pytest",
                    "",
                ]
            )
        )
    return header + "\n".join(blocks)


def _mean_time_ms(callable_obj, iterations: int) -> tuple[float, object]:
    result = None
    start = time.perf_counter()
    for _ in range(iterations):
        result = callable_obj()
    elapsed = time.perf_counter() - start
    return (elapsed / iterations) * 1000.0, result


def run_benchmark(workflow_count: int, iterations: int = 20) -> BenchmarkResult:
    rst_text = _build_rst(workflow_count)

    tracemalloc.start()
    parse_ms, workflows = _mean_time_ms(
        lambda: parse_workflows_from_text(rst_text, source_path=f"<bench-{workflow_count}>"),
        iterations=iterations,
    )
    generate_ms, _ = _mean_time_ms(lambda: build_skills_payload(workflows), iterations=iterations)

    # Benchmark context detection
    context_detect_ms, _ = _mean_time_ms(
        lambda: detect_context(),
        iterations=iterations,
    )

    # Benchmark command resolution (requires skills.json)
    skills_payload = build_skills_payload(workflows)
    skills_json_path = Path(__file__).resolve().parents[1] / "test_skills.json"
    try:
        import json

        skills_json_path.write_text(json.dumps(skills_payload) + "\n", encoding="utf-8")

        # Resolve the first skill if available
        if workflows:
            first_skill_id = workflows[0].id
            command_resolve_ms, _ = _mean_time_ms(
                lambda: resolve_command(first_skill_id, skills_path=skills_json_path),
                iterations=iterations,
            )
        else:
            command_resolve_ms = 0.0
    finally:
        if skills_json_path.exists():
            skills_json_path.unlink()

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return BenchmarkResult(
        workflows=workflow_count,
        parse_ms=parse_ms,
        generate_ms=generate_ms,
        context_detect_ms=context_detect_ms,
        command_resolve_ms=command_resolve_ms,
        peak_memory_mb=peak / (1024.0 * 1024.0),
    )


def main() -> int:
    print("Running comprehensive benchmarks with real local measurements\n")
    for count in [1, 10, 100]:
        result = run_benchmark(workflow_count=count)
        print(f"Workflows: {result.workflows}")
        print(f"Parsing time: {result.parse_ms:.3f} ms")
        print(f"Skill generation time: {result.generate_ms:.3f} ms")
        print(f"Context detection time: {result.context_detect_ms:.3f} ms")
        print(f"Command resolution time: {result.command_resolve_ms:.3f} ms")
        print(f"Memory usage: {result.peak_memory_mb:.3f} MB")
        print("-")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
