FROM python:3.6-slim
LABEL maintainer="data-eng@coatue"

ARG COMMIT_SHA=""
ENV GIT_SHA=${COMMIT_SHA}

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW_HOME=/usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

COPY . /local-airflow/
COPY requirements.txt /requirements.txt

RUN apt-get update -yqq && apt-get install -yqq --no-install-recommends python3-virtualenv
ENV VIRTUAL_ENV=/opt/venv
RUN pip install virtualenv
RUN python3 -m virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN set -ex; \
   \
      buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        systemd \
        wget \
    && wget https://download.java.net/java/GA/jdk9/9/binaries/openjdk-9_linux-x64_bin.tar.gz -P /tmp \
    && mkdir -p /usr/local/java && tar xvf /tmp/openjdk-9_linux-x64_bin.tar.gz -C /usr/local/java \
    && export PATH=$PATH:/usr/local/java/jdk-9/bin \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install -r requirements.txt \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chown -R airflow: ${VIRTUAL_ENV}
EXPOSE 8080 5555 8793
ENV PATH="/usr/local/java/jdk-9/bin:${PATH}"
COPY docker-entrypoint.sh /entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
