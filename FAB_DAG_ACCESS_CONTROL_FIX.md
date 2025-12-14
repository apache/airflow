# FAB Auth Manager DAG-Level Access Control Fix

## Problem Statement

When using Flask-AppBuilder (FAB) auth manager with team-based DAG isolation via `access_control`, users with only scoped DAG permissions could not access DAG runs, task instances, and other sub-entities. This was because the auth system required BOTH:
1. DAG-specific permission (e.g., `can_read` on `DAG:team_a_dag`)
2. Global entity permission (e.g., `can_read` on `RESOURCE_DAG_RUN`)

This made it impossible to implement proper team isolation, as granting global entity permissions would expose all DAGs in the system to the user.

## Example Scenario

```python
# DAG with access control
dag = DAG(
    "team_a_dag",
    access_control={
        "TEAM_A": {'can_read', 'can_edit'}
    }
)
```

Users in TEAM_A role could:
- ✅ View the DAG itself
- ❌ View DAG runs (403 Forbidden)
- ❌ View task instances (403 Forbidden)
- ❌ View task logs (403 Forbidden)

## Root Cause Analysis

The auth system checked four conditions for sub-entity access:
1. Global DAG permission (`can_read` on `RESOURCE_DAG`)
2. Scoped DAG permission (`can_read` on `DAG:team_a_dag`)
3. Global entity permission (`can_read` on `RESOURCE_DAG_RUN`)
4. Scoped entity permission (`can_read` on `DAG Run:team_a_dag`)

If users lacked #1 AND #3, access was denied, even if they had #2 (scoped DAG permission).

Additionally, `get_authorized_dag_ids()` only extracted DAG IDs from `DAG:*` permissions, missing those from `DAG Run:*` permissions, causing list endpoints to return empty results.

## Solution

### 1. Modified `get_authorized_dag_ids()` (line 463)

Added extraction of DAG IDs from DAG Run permissions:

```python
elif resource.startswith(RESOURCE_DAG_RUN + ":"):
    dag_id = resource[len(RESOURCE_DAG_RUN + ":") :]
    resources.add(dag_id)
```

**Impact**: List endpoints now include DAGs where users have DAG Run permissions.

### 2. Modified `is_authorized_dag()` (line 319)

Added fallback to check DAG-level permissions when entity permissions are missing:

```python
if not (is_authorized_run or access_entity_authorized):
    dag_method: ResourceMethod = "GET" if method == "GET" else "PUT"
    if not self._is_authorized_dag(method=dag_method, details=details, user=user):
        return False
```

**Impact**: Users with scoped DAG permissions can now access sub-entities without global entity permissions.

### 3. Modified `_is_authorized_dag()` (line 639)

Added scoped permission check for GET requests (list endpoints):

```python
if method == "GET":
    user_permissions = self._get_user_permissions(user)
    for action, resource in user_permissions:
        if action == ACTION_CAN_READ and resource.startswith(RESOURCE_DAG_PREFIX):
            return True
```

**Impact**: List endpoints now respect scoped permissions for filtering results.

## Authorization Flow After Fix

For sub-entity access (e.g., DAG runs, task instances):

```
1. Check global DAG permission OR scoped DAG permission
   ↓ (if missing, deny)
2. Check global entity permission OR scoped entity permission
   ↓ (if missing, proceed to fallback)
3. Fallback: Check DAG-level permission
   ↓ (if present, allow; if missing, deny)
```

This allows users with only `can_read` on `DAG:team_a_dag` to access all sub-entities of that DAG.

## Test Coverage

### New Parametrized Test Scenarios

Added to `test_get_authorized_dag_ids`:
- Scenario 10: User with DAG Run permissions
- Scenario 11: User with both DAG and DAG Run permissions

Added to `test_is_authorized_dag`:
- DAG-level permission fallback for DAG Run access
- DAG-level permission fallback for Task Instance access
- DAG-level permission fallback for Task Logs access
- DAG-level permission fallback with wrong method (should fail)
- DAG-level permission fallback with edit permission
- No DAG-level permission (should fail)

### Integration Test

`test_dag_level_access_control_for_sub_entities` - Comprehensive test that:
1. Creates two DAGs (team_a_dag, team_b_dag)
2. Creates user with only scoped permissions for team_a_dag
3. Verifies user CAN access team_a_dag and all its sub-entities
4. Verifies user CANNOT access team_b_dag or its sub-entities
5. Verifies `get_authorized_dag_ids` returns only authorized DAG

## Files Modified

1. `providers/fab/src/airflow/providers/fab/auth_manager/fab_auth_manager.py`
   - `get_authorized_dag_ids()` - line 463
   - `is_authorized_dag()` - line 319
   - `_is_authorized_dag()` - line 639

2. `providers/fab/tests/unit/fab/auth_manager/test_fab_auth_manager.py`
   - Added 2 scenarios to `test_get_authorized_dag_ids` parametrize
   - Added 6 scenarios to `test_is_authorized_dag` parametrize
   - Added `test_dag_level_access_control_for_sub_entities` integration test

## Branch and PR

- **Branch**: `fix/fab-dag-level-access-control`
- **PR URL**: https://github.com/Arunodoy18/airflow/pull/new/fix/fab-dag-level-access-control
- **Commit**: 52d13e4572

## Benefits

1. ✅ **Team-based DAG isolation now works correctly** - Users can be granted access to specific DAGs without exposing all DAGs
2. ✅ **Simplified permission management** - No need to grant both DAG and entity permissions separately
3. ✅ **Backward compatible** - Existing permission configurations continue to work
4. ✅ **Consistent behavior** - Sub-entities inherit permissions from parent DAG
5. ✅ **Filtered list endpoints** - Users only see DAGs they have permission for

## Testing Recommendations

Before merging, verify:
1. Users with scoped DAG permissions can access DAG runs
2. Users with scoped DAG permissions can access task instances
3. Users with scoped DAG permissions can access task logs
4. List endpoints correctly filter DAGs based on scoped permissions
5. Users cannot access DAGs they don't have permission for
6. Existing global permission configurations still work
