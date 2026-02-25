 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Execution API — Agent Instructions

## Versioning

This API uses [Cadwyn](https://github.com/zmievsa/cadwyn) with CalVer (`vYYYY_MM_DD.py`).

Workers and API servers deploy independently, so backward compatibility is critical — older clients must work with newer servers.

### When Making Changes

1. Check the latest version file in `versions/`. If its date is in the future (unreleased), add your `VersionChange` class to that file. Otherwise create a new `vYYYY_MM_DD.py`.
2. Update the version bundle in `versions/__init__.py` only when creating a new file.
3. Regenerate Task SDK models:

```bash
cd task-sdk && python dev/generate_task_sdk_models.py
```

4. Add tests for both the new and previous API versions.

### Common Patterns

- New schema field: `schema(Model).field("name").didnt_exist`
- New endpoint: `endpoint("/path", ["GET"]).didnt_exist`
- Response changes: implement `@convert_response_to_previous_version_for(Model)` and check field existence before popping.

### Pitfalls

- Don't use keyword arguments with `endpoint()` — use positional: `endpoint("/path", ["GET"])`.
- Don't add changes to already-released version files.
- Don't forget response converters for new fields in nested objects.

### Key Paths

- Models: `datamodels/`
- Routes: `routes/`
- Versions: `versions/`
- Task SDK generated models: `task-sdk/src/airflow/sdk/api/datamodels/_generated.py`
- Full versioning guide: [`contributing-docs/19_execution_api_versioning.rst`](../../../../contributing-docs/19_execution_api_versioning.rst)
