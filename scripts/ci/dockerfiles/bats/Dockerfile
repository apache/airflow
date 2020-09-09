FROM debian:buster-slim

ARG BATS_VERSION
ARG BATS_SUPPORT_VERSION
ARG BATS_ASSERT_VERSION
ARG BATS_FILE_VERSION
ARG AIRFLOW_BATS_VERSION
ARG COMMIT_SHA

# Install curl and gnupg2 - needed to download nodejs in the next step
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           curl \
           ca-certificates \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://github.com/bats-core/bats-core/archive/v${BATS_VERSION}.tar.gz -o /tmp/bats.tgz \
    && tar -zxf /tmp/bats.tgz -C /tmp \
    && /bin/bash /tmp/bats-core-${BATS_VERSION}/install.sh /opt/bats/  && rm -rf

RUN mkdir -p /opt/bats/lib/bats-support \
    && curl -sSL https://github.com/bats-core/bats-support/archive/v${BATS_SUPPORT_VERSION}.tar.gz -o /tmp/bats-support.tgz \
    && tar -zxf /tmp/bats-support.tgz -C /opt/bats/lib/bats-support --strip 1 && rm -rf /tmp/*

RUN mkdir -p /opt/bats/lib/bats-assert \
    && curl -sSL https://github.com/bats-core/bats-assert/archive/v${BATS_ASSERT_VERSION}.tar.gz -o /tmp/bats-assert.tgz \
    && tar -zxf /tmp/bats-assert.tgz -C /opt/bats/lib/bats-assert --strip 1 && rm -rf /tmp/*

RUN mkdir -p /opt/bats/lib/bats-file \
    && curl -sSL https://github.com/bats-core/bats-file/archive/v${BATS_FILE_VERSION}.tar.gz -o /tmp/bats-file.tgz \
    && tar -zxf /tmp/bats-file.tgz -C /opt/bats/lib/bats-file --strip 1 && rm -rf /tmp/*

COPY load.bash /opt/bats/lib/
RUN chmod a+x /opt/bats/lib/load.bash

LABEL org.apache.airflow.component="bats"
LABEL org.apache.airflow.bats.core.version="${BATS_VERSION}"
LABEL org.apache.airflow.bats.support.version="${BATS_SUPPORT_VERSION}"
LABEL org.apache.airflow.bats.assert.version="${BATS_ASSERT_VERSION}"
LABEL org.apache.airflow.bats.file.version="${BATS_FILE_VERSION}"
LABEL org.apache.airflow.airflow_bats.version="${AIRFLOW_BATS_VERSION}"
LABEL org.apache.airflow.commit_sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

ENTRYPOINT ["/opt/bats/bin/bats"]
CMD ["--help"]
