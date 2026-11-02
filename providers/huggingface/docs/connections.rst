Hugging Face connection
======================

The hook uses the Airflow connection type ``huggingface`` and defaults to
connection ID ``huggingface_default``. Put the Hub access token in the password
field, labelled **Access token** in the connection form. Host, schema, port,
and login are hidden. Extra fields are not used for authentication.

The hook creates a cached ``HfApi`` client and uses it for Hub metadata,
publication lookup, and ``whoami()`` connection testing. A successful
connection test confirms that the token authenticates; it does not check
access to a particular dataset or repository.

If the default connection is absent, no token is passed explicitly and
``huggingface_hub`` can use ``HF_TOKEN`` or a cached worker login. If an
explicit non-default connection ID is provided but cannot be resolved, the
hook raises Airflow's connection-not-found exception. An existing connection
with no password also defers to the SDK fallback.

Public dataset loads can use anonymous access. Private or gated reads need a
token with read permission. Publishing needs write permission to the target
repository. Prefer separate least-privilege tokens and store them in an Airflow
secrets backend or encrypted connection. Do not put tokens in DAG code or
operator parameters. The provider does not log token values; Airflow masks the
connection password in its normal logging paths.
