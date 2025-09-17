### Problem
Users can hit confusing failures when a single DAG tag exceeds 100 characters (DB-backed limit). The error surfaced late and wasn't clearly documented.

### Changes
- Add early tag-length validation in DAG creation paths (SDK DAG).
- Raise `AirflowException` naming the offending tag and its actual length; preview long tags for readability.
- Add a short "Tag limitations" note to the DAG docs (each tag ≤ 100 chars).

### Why safe
- Aligns with existing schema (`dag_tag.name VARCHAR(100)`).
- No migrations or behavior changes beyond clearer, earlier validation.

### Tests
- Tag of exactly 100 chars → allowed.
- Tag of 101 chars → raises `AirflowException` (message includes actual length and limit).
- Multiple tags with one too long → raises; message identifies the problematic tag via preview.

### Docs
- Added a concise "Tag limitations" subsection under core DAG concepts.
