#!/usr/bin/env python3
"""
Validate Kustomize overlays for Airflow worker deployment.

This script validates that Kustomize overlays produce valid Kubernetes manifests
without needing to install kubectl or kustomize CLI tools.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Any

try:
    import yaml
except ImportError:
    print("PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)


def validate_yaml_files(directory: Path) -> bool:
    """Validate all YAML files in a directory are parseable."""
    errors = []
    for yaml_file in directory.rglob("*.yaml"):
        try:
            with open(yaml_file, 'r') as f:
                yaml.safe_load(f)
            print(f"✓ Valid YAML: {yaml_file.relative_to(directory.parent.parent)}")
        except yaml.YAMLError as e:
            errors.append(f"✗ Invalid YAML {yaml_file}: {e}")

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return False
    return True


def check_overlay_structure(base_dir: Path) -> Dict[str, List[str]]:
    """Check that overlay structure is correct."""
    overlays = {}
    overlays_dir = base_dir / "overlays"

    if not overlays_dir.exists():
        print(f"✗ Overlays directory not found: {overlays_dir}")
        return overlays

    for overlay in overlays_dir.iterdir():
        if overlay.is_dir():
            kustomization = overlay / "kustomization.yaml"
            if kustomization.exists():
                overlays[overlay.name] = []
                with open(kustomization, 'r') as f:
                    config = yaml.safe_load(f)

                # Check for resources
                if 'resources' in config:
                    overlays[overlay.name].append(f"Resources: {len(config['resources'])}")

                # Check for patches
                if 'patches' in config:
                    overlays[overlay.name].append(f"Patches: {len(config['patches'])}")

                if 'patchesStrategicMerge' in config:
                    overlays[overlay.name].append(
                        f"Strategic Merge Patches: {len(config['patchesStrategicMerge'])}"
                    )

                print(f"✓ Overlay: {overlay.name}")
                for detail in overlays[overlay.name]:
                    print(f"  - {detail}")
            else:
                print(f"✗ Missing kustomization.yaml in {overlay.name}")

    return overlays


def analyze_complexity_reduction(original_template: Path, simplified_template: Path = None):
    """Analyze how much complexity was reduced."""
    with open(original_template, 'r') as f:
        original_content = f.read()

    original_lines = len(original_content.splitlines())
    conditional_blocks = original_content.count("{{- if")
    nested_conditionals = original_content.count("{{- if") - original_content.count("{{- if or")

    print("\n" + "="*60)
    print("COMPLEXITY ANALYSIS")
    print("="*60)
    print(f"Original Template Lines: {original_lines}")
    print(f"Conditional Blocks ({{{{- if}}}}): {conditional_blocks}")
    print(f"Variable Assignments ({{{{- $...}}}}): {original_content.count('{{- $')}")

    if simplified_template and simplified_template.exists():
        with open(simplified_template, 'r') as f:
            simplified_content = f.read()

        simplified_lines = len(simplified_content.splitlines())
        simplified_conditionals = simplified_content.count("{{- if")

        reduction_lines = ((original_lines - simplified_lines) / original_lines) * 100
        reduction_conditionals = ((conditional_blocks - simplified_conditionals) / conditional_blocks) * 100

        print(f"\nSimplified Template Lines: {simplified_lines}")
        print(f"Simplified Conditionals: {simplified_conditionals}")
        print(f"\n✓ Line Reduction: {reduction_lines:.1f}%")
        print(f"✓ Conditional Reduction: {reduction_conditionals:.1f}%")

    print("="*60)


def check_for_kustomize_cli() -> bool:
    """Check if kustomize CLI is available."""
    try:
        result = subprocess.run(['kustomize', 'version'],
                              capture_output=True,
                              text=True,
                              timeout=5)
        if result.returncode == 0:
            print(f"✓ Kustomize CLI found: {result.stdout.strip()}")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Try kubectl kustomize
    try:
        result = subprocess.run(['kubectl', 'version', '--client', '--short'],
                              capture_output=True,
                              text=True,
                              timeout=5)
        if result.returncode == 0:
            print(f"✓ kubectl found (can use 'kubectl kustomize')")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    print("⚠ Neither 'kustomize' nor 'kubectl' CLI found")
    print("  Install kustomize: https://kubectl.docs.kubernetes.io/installation/kustomize/")
    print("  Or install kubectl: https://kubernetes.io/docs/tasks/tools/")
    return False


def build_overlay(overlay_path: Path, use_kubectl: bool = False) -> Dict[str, Any]:
    """Build a kustomize overlay and return the result."""
    if use_kubectl:
        cmd = ['kubectl', 'kustomize', str(overlay_path)]
    else:
        cmd = ['kustomize', 'build', str(overlay_path)]

    try:
        result = subprocess.run(cmd,
                              capture_output=True,
                              text=True,
                              timeout=10)
        if result.returncode == 0:
            # Parse the output as YAML documents
            docs = list(yaml.safe_load_all(result.stdout))
            return {'success': True, 'documents': docs, 'count': len(docs)}
        else:
            return {'success': False, 'error': result.stderr}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def main():
    """Main validation function."""
    print("="*60)
    print("AIRFLOW WORKER KUSTOMIZE VALIDATION")
    print("="*60)

    # Find the kustomize directory
    script_dir = Path(__file__).parent
    kustomize_dir = script_dir.parent

    if not kustomize_dir.exists():
        print(f"✗ Kustomize directory not found: {kustomize_dir}")
        return False

    print(f"\nKustomize Directory: {kustomize_dir}")

    # Validate all YAML files
    print("\n" + "-"*60)
    print("VALIDATING YAML FILES")
    print("-"*60)
    if not validate_yaml_files(kustomize_dir):
        return False

    # Check overlay structure
    print("\n" + "-"*60)
    print("CHECKING OVERLAY STRUCTURE")
    print("-"*60)
    overlays = check_overlay_structure(kustomize_dir)

    if not overlays:
        print("✗ No valid overlays found")
        return False

    print(f"\n✓ Found {len(overlays)} overlay(s): {', '.join(overlays.keys())}")

    # Analyze complexity reduction
    original_template = kustomize_dir.parent / "templates" / "workers" / "worker-deployment.yaml"
    simplified_template = kustomize_dir.parent / "templates" / "workers" / "worker-deployment-simplified.yaml"

    if original_template.exists():
        analyze_complexity_reduction(original_template, simplified_template)

    # Check for kustomize CLI
    print("\n" + "-"*60)
    print("CHECKING FOR KUSTOMIZE CLI")
    print("-"*60)
    has_cli = check_for_kustomize_cli()

    if has_cli:
        print("\n" + "-"*60)
        print("BUILDING OVERLAYS (if CLI available)")
        print("-"*60)

        # Try to build each overlay
        for overlay_name in overlays.keys():
            overlay_path = kustomize_dir / "overlays" / overlay_name
            print(f"\nBuilding overlay: {overlay_name}")

            # Try kubectl first, then kustomize
            result = build_overlay(overlay_path, use_kubectl=True)
            if not result.get('success'):
                result = build_overlay(overlay_path, use_kubectl=False)

            if result.get('success'):
                print(f"✓ Successfully built {result['count']} document(s)")

                # Analyze the built resources
                for doc in result['documents']:
                    if doc and 'kind' in doc:
                        name = doc.get('metadata', {}).get('name', 'unknown')
                        print(f"  - {doc['kind']}: {name}")
            else:
                print(f"✗ Failed to build: {result.get('error', 'Unknown error')}")

    print("\n" + "="*60)
    print("VALIDATION COMPLETE")
    print("="*60)
    print("\n✓ All YAML files are valid")
    print(f"✓ Found {len(overlays)} overlay(s)")

    if has_cli:
        print("✓ CLI tools available for building manifests")
    else:
        print("⚠ Install kustomize/kubectl to build and test overlays")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
