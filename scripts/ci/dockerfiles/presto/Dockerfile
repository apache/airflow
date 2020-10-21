ARG PRESTO_VERSION="330"
FROM prestosql/presto:${PRESTO_VERSION}

# Obtain root privileges
USER 0

# Setup entrypoint
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/lib/presto/bin/run-presto"]

# Expose HTTPS
EXPOSE 7778

LABEL org.apache.airflow.component="presto"
LABEL org.apache.airflow.presto.core.version="${PRESTO_VERSION}"
LABEL org.apache.airflow.airflow_bats.version="${AIRFLOW_PRESTO_VERSION}"
LABEL org.apache.airflow.commit_sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

# Restore user
USER presto:presto
