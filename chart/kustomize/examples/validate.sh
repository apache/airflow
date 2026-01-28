#!/bin/bash
# Validate Kustomize structure for Airflow worker deployment
# This script performs basic validation without requiring Python dependencies

set -e

KUSTOMIZE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CHART_DIR="$(dirname "$KUSTOMIZE_DIR")"

echo "============================================================"
echo "AIRFLOW WORKER KUSTOMIZE VALIDATION"
echo "============================================================"
echo ""
echo "Kustomize Directory: $KUSTOMIZE_DIR"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
errors=0
warnings=0

# Check if base exists
echo "------------------------------------------------------------"
echo "CHECKING BASE CONFIGURATION"
echo "------------------------------------------------------------"
if [ ! -d "$KUSTOMIZE_DIR/base" ]; then
    echo -e "${RED}✗ Base directory not found${NC}"
    ((errors++))
else
    echo -e "${GREEN}✓ Base directory found${NC}"

    if [ ! -f "$KUSTOMIZE_DIR/base/kustomization.yaml" ]; then
        echo -e "${RED}✗ base/kustomization.yaml not found${NC}"
        ((errors++))
    else
        echo -e "${GREEN}✓ base/kustomization.yaml found${NC}"
    fi

    if [ ! -f "$KUSTOMIZE_DIR/base/worker-deployment.yaml" ]; then
        echo -e "${RED}✗ base/worker-deployment.yaml not found${NC}"
        ((errors++))
    else
        echo -e "${GREEN}✓ base/worker-deployment.yaml found${NC}"
    fi
fi

# Check overlays
echo ""
echo "------------------------------------------------------------"
echo "CHECKING OVERLAY CONFIGURATIONS"
echo "------------------------------------------------------------"

overlay_count=0
if [ -d "$KUSTOMIZE_DIR/overlays" ]; then
    for overlay in "$KUSTOMIZE_DIR/overlays"/*; do
        if [ -d "$overlay" ]; then
            overlay_name=$(basename "$overlay")
            ((overlay_count++))

            if [ ! -f "$overlay/kustomization.yaml" ]; then
                echo -e "${RED}✗ Overlay '$overlay_name': missing kustomization.yaml${NC}"
                ((errors++))
            else
                echo -e "${GREEN}✓ Overlay '$overlay_name': valid${NC}"

                # Check if it references the base
                if grep -q "../../base" "$overlay/kustomization.yaml" 2>/dev/null; then
                    echo "  - References base correctly"
                else
                    echo -e "  ${YELLOW}- Warning: Does not reference ../../base${NC}"
                    ((warnings++))
                fi

                # Count patches
                patch_count=$(grep -c "^  - target:" "$overlay/kustomization.yaml" 2>/dev/null || echo "0")
                if [ "$patch_count" -gt 0 ]; then
                    echo "  - Contains $patch_count patch(es)"
                fi
            fi
        fi
    done
    echo ""
    echo -e "${GREEN}✓ Found $overlay_count overlay(s)${NC}"
else
    echo -e "${RED}✗ Overlays directory not found${NC}"
    ((errors++))
fi

# Analyze original template complexity
echo ""
echo "------------------------------------------------------------"
echo "COMPLEXITY ANALYSIS"
echo "------------------------------------------------------------"

original_template="$CHART_DIR/templates/workers/worker-deployment.yaml"
simplified_template="$CHART_DIR/templates/workers/worker-deployment-simplified.yaml"

if [ -f "$original_template" ]; then
    original_lines=$(wc -l < "$original_template")
    conditional_blocks=$(grep -c "{{- if" "$original_template" || echo "0")
    variable_assignments=$(grep -c "{{- \$" "$original_template" || echo "0")

    echo "Original Template:"
    echo "  - Lines: $original_lines"
    echo "  - Conditional blocks ({{- if}}): $conditional_blocks"
    echo "  - Variable assignments ({{- \$...}}): $variable_assignments"

    if [ -f "$simplified_template" ]; then
        simplified_lines=$(wc -l < "$simplified_template")
        simplified_conditionals=$(grep -c "{{- if" "$simplified_template" || echo "0")

        line_reduction=$(awk "BEGIN {printf \"%.1f\", (($original_lines - $simplified_lines) / $original_lines) * 100}")
        cond_reduction=$(awk "BEGIN {printf \"%.1f\", (($conditional_blocks - $simplified_conditionals) / $conditional_blocks) * 100}")

        echo ""
        echo "Simplified Template:"
        echo "  - Lines: $simplified_lines"
        echo "  - Conditional blocks: $simplified_conditionals"
        echo ""
        echo -e "${GREEN}✓ Line reduction: ${line_reduction}%${NC}"
        echo -e "${GREEN}✓ Conditional reduction: ${cond_reduction}%${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Original template not found for comparison${NC}"
    ((warnings++))
fi

# Check for CLI tools
echo ""
echo "------------------------------------------------------------"
echo "CHECKING CLI TOOLS"
echo "------------------------------------------------------------"

if command -v kustomize &> /dev/null; then
    version=$(kustomize version --short 2>/dev/null || kustomize version)
    echo -e "${GREEN}✓ kustomize CLI found: $version${NC}"
    HAS_KUSTOMIZE=1
elif command -v kubectl &> /dev/null; then
    version=$(kubectl version --client --short 2>/dev/null || kubectl version --client)
    echo -e "${GREEN}✓ kubectl found (can use 'kubectl kustomize')${NC}"
    HAS_KUBECTL=1
else
    echo -e "${YELLOW}⚠ Neither 'kustomize' nor 'kubectl' CLI found${NC}"
    echo "  Install kustomize: https://kubectl.docs.kubernetes.io/installation/kustomize/"
    echo "  Or install kubectl: https://kubernetes.io/docs/tasks/tools/"
    ((warnings++))
fi

# Try to build overlays if tools are available
if [ -n "$HAS_KUSTOMIZE" ] || [ -n "$HAS_KUBECTL" ]; then
    echo ""
    echo "------------------------------------------------------------"
    echo "BUILDING OVERLAYS"
    echo "------------------------------------------------------------"

    for overlay in "$KUSTOMIZE_DIR/overlays"/*; do
        if [ -d "$overlay" ] && [ -f "$overlay/kustomization.yaml" ]; then
            overlay_name=$(basename "$overlay")
            echo ""
            echo "Building overlay: $overlay_name"

            if [ -n "$HAS_KUSTOMIZE" ]; then
                if kustomize build "$overlay" > /dev/null 2>&1; then
                    doc_count=$(kustomize build "$overlay" 2>/dev/null | grep -c "^kind:" || echo "0")
                    echo -e "${GREEN}✓ Successfully built $doc_count document(s)${NC}"
                else
                    echo -e "${RED}✗ Failed to build overlay${NC}"
                    ((errors++))
                fi
            elif [ -n "$HAS_KUBECTL" ]; then
                if kubectl kustomize "$overlay" > /dev/null 2>&1; then
                    doc_count=$(kubectl kustomize "$overlay" 2>/dev/null | grep -c "^kind:" || echo "0")
                    echo -e "${GREEN}✓ Successfully built $doc_count document(s)${NC}"
                else
                    echo -e "${RED}✗ Failed to build overlay${NC}"
                    ((errors++))
                fi
            fi
        fi
    done
fi

# Summary
echo ""
echo "============================================================"
echo "VALIDATION SUMMARY"
echo "============================================================"

if [ $errors -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed${NC}"
    echo -e "${GREEN}✓ Found $overlay_count overlay(s)${NC}"

    if [ $warnings -gt 0 ]; then
        echo -e "${YELLOW}⚠ $warnings warning(s)${NC}"
    fi

    echo ""
    echo "To use these overlays:"
    echo "  kubectl apply -k chart/kustomize/overlays/production"
    echo "  kubectl apply -k chart/kustomize/overlays/staging"
    echo "  kubectl apply -k chart/kustomize/overlays/development"
    echo ""
    echo "To see generated manifests:"
    if [ -n "$HAS_KUSTOMIZE" ]; then
        echo "  kustomize build chart/kustomize/overlays/production"
    elif [ -n "$HAS_KUBECTL" ]; then
        echo "  kubectl kustomize chart/kustomize/overlays/production"
    fi

    exit 0
else
    echo -e "${RED}✗ Validation failed with $errors error(s)${NC}"
    if [ $warnings -gt 0 ]; then
        echo -e "${YELLOW}⚠ And $warnings warning(s)${NC}"
    fi
    exit 1
fi
