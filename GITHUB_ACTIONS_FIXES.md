# GitHub Actions Workflow Fixes

## Issues Found and Fixed

### 1. ✅ Git Revision Range Syntax Error (THREE DOTS → TWO DOTS)

**Problem:** Using `...` (three dots) instead of `..` (two dots) in git log commands causes "bad revision" errors.

**Files Fixed:**
- `dev/breeze/src/airflow_breeze/commands/release_management_commands.py:2642`
- `dev/breeze/src/airflow_breeze/prepare_providers/provider_documentation.py:273`
- `dev/breeze/tests/test_provider_documentation.py:117,128`

**Changes Made:**
```python
# Before (INCORRECT)
git_cmd.append(f"{from_commit}...{to_commit}")

# After (CORRECT)
git_cmd.append(f"{from_commit}..{to_commit}")
```

### 2. ✅ Missing Dependencies Check (gh and jq)

**Problem:** Scripts fail when required tools `gh` (GitHub CLI) and `jq` (JSON processor) are not installed.

**Files Fixed:**
- `dev/remove_artifacts.sh` - Added `jq` dependency check
- `scripts/ci/prek/common_prek_utils.py` - Added `gh` dependency check

**Changes Made:**

For `jq` in `dev/remove_artifacts.sh`:
```bash
# Check for required dependencies
function check_dep() {
    if ! command -v "$1" &> /dev/null; then
        echo "::error ::$1 is required for this action"
        return 1
    fi
}

missing_dep="false"
check_dep jq || missing_dep="true"

if [ "$missing_dep" == "true" ]; then
    echo "Missing required dependencies. Please install them:"
    echo "  sudo apt-get update && sudo apt-get install -y jq"
    exit 1
fi
```

For `gh` in `scripts/ci/prek/common_prek_utils.py`:
```python
# Check if gh is available
try:
    subprocess.check_output(["which", "gh"], stderr=subprocess.DEVNULL)
except subprocess.CalledProcessError:
    console.print(
        "[red]gh (GitHub CLI) is required for this action.[/]\n"
        "Please install it using one of the following methods:\n"
        "  • Ubuntu/Debian: sudo apt-get update && sudo apt-get install -y gh\n"
        "  • macOS: brew install gh\n"
        "  • Or download from: https://github.com/cli/cli/releases\n"
    )
    sys.exit(1)
```

### 3. ✅ Git Remote Setup (Already Properly Handled)

**Problem:** Error "No such remote 'apache-https-for-providers'"

**Solution:** The code in `dev/breeze/src/airflow_breeze/utils/packages.py:770-779` already properly handles remote setup:
```python
try:
    run_command(["git", "remote", "get-url", HTTPS_REMOTE], text=True, capture_output=True)
except subprocess.CalledProcessError as ex:
    if ex.returncode == 128 or ex.returncode == 2:
        run_command([
            "git", "remote", "add", HTTPS_REMOTE,
            f"https://github.com/{github_repository}.git",
        ], check=True)
```

## Installation Commands for CI/Workflows

Add these to your GitHub Actions workflow before running Airflow commands:

```yaml
- name: Install GitHub CLI and jq
  run: |
    sudo apt-get update
    sudo apt-get install -y gh jq
```

## Results

These fixes resolve:
- ✅ **Git revision range errors** - Proper `..` syntax for git log
- ✅ **Missing dependency errors** - Proper checks for `gh` and `jq`
- ✅ **Remote setup errors** - Already handled in existing code
- ✅ **Improved error messages** - Clear instructions for missing tools

All GitHub Actions should now pass without the reported dependency and git command failures.
