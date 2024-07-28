"""The setup script."""

from setuptools import setup

with open("src/README.md") as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as requirements_txt:
    app_requirements = requirements_txt.read().split("\n")

requirements = app_requirements

setup(
    author="Google",
    author_email="bjankiewicz@google.com",
    description="Python project dedicated to running performance tests of Apache Airflow",
    entry_points={"console_scripts": ["airflow_performance_test=run_performance_test:main"]},
    install_requires=requirements,
    license="",
    long_description=readme,
    name="airflow_gepard",
    packages=["performance_scripts"],
    python_requires=">=3.11",
    url="https://github.com/airflow/performance",
    version="0.1.0",
    zip_safe=False,
)
