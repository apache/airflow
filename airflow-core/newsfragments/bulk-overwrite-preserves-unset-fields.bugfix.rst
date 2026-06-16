Fix bulk create+overwrite silently resetting unset fields on pools and connections

The bulk APIs (``PATCH /api/v2/pools`` and ``PATCH /api/v2/connections``) applied a
``create`` action with ``action_on_existence=overwrite`` by dumping the whole request
body, so fields the request omitted were reset to their defaults on the existing
record. Most damagingly, omitting ``team_name`` nulled an existing pool's or
connection's multi-team ownership; ``description`` and ``include_deferred`` were
affected too. Overwrites now only write the fields the request actually provides, so
omitted fields keep their current value while explicitly-set fields are still applied.
