ARG ALPINE_VERSION="3.12"

FROM alpine:${ALPINE_VERSION}

ARG STRESS_VERSION
ARG AIRFLOW_STRESS_VERSION
ARG COMMIT_SHA

ARG STRESS_VERSION=1.0.4

# Those are build deps only but still we want the latest versions of those
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# It's ok to switch temporarily to the /tmp directory
# "Use WORKDIR to switch to a directory" https://github.com/hadolint/hadolint/wiki/DL3003
# hadolint ignore=DL3018,DL3003
RUN apk --no-cache add bash g++ make curl && \
    curl -o "/tmp/stress-${STRESS_VERSION}.tgz" \
        "https://fossies.org/linux/privat/stress-${STRESS_VERSION}.tar.gz" && \
    cd "/tmp" && \
    tar xvf "stress-${STRESS_VERSION}.tgz" && \
    rm "/tmp/stress-${STRESS_VERSION}.tgz" && \
    cd "/tmp/stress-${STRESS_VERSION}" &&  \
    ./configure && make "-j$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
    apk del g++ make curl &&  \
    rm -rf "/tmp/*" "/var/tmp/*" "/var/cache/distfiles/*"

LABEL org.apache.airflow.component="stress"
LABEL org.apache.airflow.stress.version="${STRESS_VERSION}"
LABEL org.apache.airflow.airflow_stress.version="${AIRFLOW_STRESS_VERSION}"
LABEL org.apache.airflow.commit_sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

CMD ["/usr/local/bin/stress"]
