Airflow's CI and production images are now based on `Docker Hardened Images <https://dhi.io>`_ for Python

Python is no longer compiled from source during the image build - it comes from the hardened base image
instead, which removes the source download, its signature verification and the compilation itself from the
build (the OS dependency layer of the production image went from roughly 210s to 105s in a local
cache-disabled build). Python still lives in ``/usr/python``, which is now a symlink to the ``/opt/python``
location the base image uses, so paths inside the image are unchanged.

The ``BASE_IMAGE`` build argument now defaults to ``ghcr.io/apache/airflow/base/python:<version>-debian12-dev``,
Airflow's public mirror of the upstream ``dhi.io/python`` image - building an image needs no registry
credentials. Anyone overriding ``BASE_IMAGE`` with a plain ``debian:bookworm-slim`` image has to switch to
an image that already provides Python.

The ``PYTHON_LTO`` build argument is gone. It existed to disable Link-Time Optimization when compiling
Python in FIPS mode, and there is no Python compilation left to configure. To build a FIPS-compliant image,
point ``BASE_IMAGE`` at a FIPS variant of the hardened image instead, for example
``dhi.io/python:3.13.15-debian12-fips-dev``.
